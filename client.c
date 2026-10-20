#define _POSIX_C_SOURCE 200809L

#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <pthread.h>
#include <signal.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

#define CONNECTION_TIMEOUT_SECONDS 30
#define RESPONSE_HEADER_BUFFER_SIZE (16 * 1024)
#define MAX_RESPONSE_BODY_SIZE (16 * 1024 * 1024)

#define FETCH_THREAD_COUNT 4
#define STRESS_WRITER_COUNT 2
#define STRESS_READER_COUNT 2
#define WRITES_PER_WRITER 5
#define STRESS_PAYLOAD_LENGTH 10

static const char *demo_remote_path = "/demo.html";
static const char *stress_remote_path = "/stress.txt";
static const char *output_directory = "client-output";

static const unsigned char initial_document[] =
    "<!doctype html>\n"
    "<html><body><h1>Initial document</h1><p>Created by the client demo.</p></body></html>\n";

static const unsigned char pipeline_document[] =
    "<!doctype html>\n"
    "<html><body><h1>Updated document</h1><p>Written by the pipelined PUT.</p></body></html>\n";

static const unsigned char stress_payloads[STRESS_WRITER_COUNT][STRESS_PAYLOAD_LENGTH + 1] = {
    "1111111111",
    "2222222222",
};

static pthread_mutex_t output_mutex = PTHREAD_MUTEX_INITIALIZER;

/* Datatypes
 *
 *
 *
 *
 */

struct http_connection {
    int fd;
    unsigned char buffer[RESPONSE_HEADER_BUFFER_SIZE];
    size_t used;
};

struct http_response {
    int status_code;
    unsigned char *body;
    size_t body_length;
};

struct start_gate {
    pthread_mutex_t mutex;
    pthread_cond_t condition;
    bool open;
};

struct fetch_thread_arguments {
    const char *hostname;
    const char *port;
    struct start_gate *gate;
    size_t index;
    bool succeeded;
};

struct stress_context {
    const char *hostname;
    const char *port;
    struct start_gate gate;
    atomic_int active_writers;
    atomic_bool failed;
};

struct stress_thread_arguments {
    struct stress_context *context;
    size_t index;
    size_t successful_reads;
};

/* Error reporting
 *
 *
 *
 *
 */

static void report_error(const char *operation, int error) {
    pthread_mutex_lock(&output_mutex);
    fprintf(stderr, "client: %s: %s\n", operation, strerror(error));
    pthread_mutex_unlock(&output_mutex);
}

static void report_status_error(const char *operation, int status_code) {
    pthread_mutex_lock(&output_mutex);
    fprintf(stderr, "client: %s returned unexpected HTTP status %d\n", operation, status_code);
    pthread_mutex_unlock(&output_mutex);
}

/* Socket handling
 *
 *
 *
 *
 */

static bool configure_connection_timeouts(int sockfd) {
    struct timeval timeout = {
        .tv_sec = CONNECTION_TIMEOUT_SECONDS,
        .tv_usec = 0,
    };

    if (setsockopt(sockfd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof timeout) < 0) {
        perror("client: setsockopt SO_RCVTIMEO");
        return false;
    }
    if (setsockopt(sockfd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof timeout) < 0) {
        perror("client: setsockopt SO_SNDTIMEO");
        return false;
    }

    return true;
}

static int connect_to_server(const char *address, const char *port) {
    struct addrinfo hints;
    memset(&hints, 0, sizeof hints);
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;

    struct addrinfo *addresses;
    int status = getaddrinfo(address, port, &hints, &addresses);
    if (status != 0) {
        pthread_mutex_lock(&output_mutex);
        fprintf(stderr, "client: getaddrinfo %s:%s: %s\n", address, port, gai_strerror(status));
        pthread_mutex_unlock(&output_mutex);
        errno = EINVAL;
        return -1;
    }

    int sockfd = -1;
    int last_error = ECONNREFUSED;
    for (struct addrinfo *candidate = addresses; candidate != NULL;
         candidate = candidate->ai_next) {
        sockfd = socket(candidate->ai_family, candidate->ai_socktype, candidate->ai_protocol);
        if (sockfd < 0) {
            last_error = errno;
            continue;
        }

        if (connect(sockfd, candidate->ai_addr, candidate->ai_addrlen) < 0) {
            last_error = errno;
        } else if (!configure_connection_timeouts(sockfd)) {
            last_error = errno;
        } else {
            break;
        }

        close(sockfd);
        sockfd = -1;
    }

    freeaddrinfo(addresses);
    if (sockfd < 0) {
        errno = last_error;
        return -1;
    }
    return sockfd;
}

/* IO
 *
 *
 *
 *
 */

static bool send_all(int sockfd, const void *data, size_t length) {
    const unsigned char *bytes = data;
    size_t sent = 0;

    while (sent < length) {
        ssize_t result = send(sockfd, bytes + sent, length - sent, 0);
        if (result < 0) {
            if (errno == EINTR) {
                continue;
            }
            return false;
        }
        if (result == 0) {
            errno = EIO;
            return false;
        }
        sent += (size_t)result;
    }

    return true;
}

static bool write_file_all(int file_fd, const unsigned char *data, size_t length) {
    size_t written = 0;
    while (written < length) {
        ssize_t result = write(file_fd, data + written, length - written);
        if (result < 0) {
            if (errno == EINTR) {
                continue;
            }
            return false;
        }
        if (result == 0) {
            errno = EIO;
            return false;
        }
        written += (size_t)result;
    }
    return true;
}

/* Response parsing helpers
 *
 *
 *
 *
 */

static const unsigned char *find_header_end(const unsigned char *begin, const unsigned char *end) {
    size_t length = (size_t)(end - begin);

    for (size_t index = 0; index + 3 < length; ++index) {
        if (begin[index] == '\r' && begin[index + 1] == '\n' && begin[index + 2] == '\r' &&
            begin[index + 3] == '\n') {
            return begin + index + 4;
        }
    }

    return NULL;
}

static const unsigned char *find_crlf(const unsigned char *begin, const unsigned char *end) {
    for (const unsigned char *position = begin; position + 1 < end; ++position) {
        if (position[0] == '\r' && position[1] == '\n') {
            return position;
        }
    }
    return NULL;
}

static bool text_equals_ignore_case(const unsigned char *data, size_t length,
                                    const char *expected) {
    size_t expected_length = strlen(expected);
    return length == expected_length &&
           strncasecmp((const char *)data, expected, expected_length) == 0;
}

static bool parse_content_length(const unsigned char *begin, const unsigned char *end,
                                 size_t *result) {
    // in server the whitespaces have alreay been parsed
    // in the client we need to do it here
    while (begin < end && (*begin == ' ' || *begin == '\t')) {
        ++begin;
    }
    while (end > begin && (end[-1] == ' ' || end[-1] == '\t')) {
        --end;
    }

    if (begin == end) {
        return false;
    }

    size_t value = 0;
    for (const unsigned char *position = begin; position < end; ++position) {
        if (*position < '0' || *position > '9') {
            return false;
        }
        size_t digit = (size_t)(*position - '0');
        if (value > (SIZE_MAX - digit) / 10) {
            return false;
        }
        value = value * 10 + digit;
    }

    *result = value;
    return true;
}

static void destroy_http_response(struct http_response *response) {
    free(response->body);
    memset(response, 0, sizeof *response);
}

static bool receive_response_bytes(struct http_connection *connection) {
    while (connection->used < sizeof connection->buffer) {
        ssize_t received = recv(connection->fd, connection->buffer + connection->used,
                                sizeof connection->buffer - connection->used, 0);
        if (received > 0) {
            connection->used += (size_t)received;
            return true;
        }
        if (received == 0) {
            errno = ECONNRESET;
            return false;
        }
        if (errno != EINTR) {
            return false;
        }
    }

    errno = EMSGSIZE;
    return false;
}

static bool receive_http_response(struct http_connection *connection,
                                  struct http_response *response) {
    memset(response, 0, sizeof *response);

    // find response segemnt locations
    const unsigned char *header_end;
    while ((header_end = find_header_end(connection->buffer,
                                         connection->buffer + connection->used)) == NULL) {
        if (!receive_response_bytes(connection)) {
            return false;
        }
    }

    const unsigned char *header_begin = connection->buffer;
    size_t header_size = (size_t)(header_end - header_begin);
    const unsigned char *status_line_end = find_crlf(header_begin, header_end);
    if (status_line_end == NULL) {
        errno = EPROTO;
        return false;
    }

    // validate is HTTP/1.0 or HTTP/1.1
    const unsigned char *version_end =
        memchr(header_begin, ' ', (size_t)(status_line_end - header_begin));
    if (version_end == NULL || !((size_t)(version_end - header_begin) == sizeof "HTTP/1.1" - 1 &&
                                 (memcmp(header_begin, "HTTP/1.1", sizeof "HTTP/1.1" - 1) == 0 ||
                                  memcmp(header_begin, "HTTP/1.0", sizeof "HTTP/1.0" - 1) == 0))) {
        errno = EPROTO;
        return false;
    }

    // extract status code
    const unsigned char *status_begin = version_end + 1;
    if ((size_t)(status_line_end - status_begin) < 3 || status_begin[0] < '0' ||
        status_begin[0] > '9' || status_begin[1] < '0' || status_begin[1] > '9' ||
        status_begin[2] < '0' || status_begin[2] > '9' ||
        ((size_t)(status_line_end - status_begin) > 3 && status_begin[3] != ' ')) {
        errno = EPROTO;
        return false;
    }
    response->status_code =
        (status_begin[0] - '0') * 100 + (status_begin[1] - '0') * 10 + (status_begin[2] - '0');

    // find Content-Length header
    bool content_length_present = false;
    size_t content_length = 0;
    const unsigned char *line_begin = status_line_end + 2;
    const unsigned char *fields_end = header_end - 2;
    while (line_begin < fields_end) {
        const unsigned char *line_end = find_crlf(line_begin, fields_end);
        if (line_end == NULL) {
            errno = EPROTO;
            return false;
        }
        const unsigned char *colon = memchr(line_begin, ':', (size_t)(line_end - line_begin));
        if (colon == NULL || colon == line_begin) {
            errno = EPROTO;
            return false;
        }

        if (text_equals_ignore_case(line_begin, (size_t)(colon - line_begin), "Content-Length")) {
            if (content_length_present ||
                !parse_content_length(colon + 1, line_end, &content_length)) {
                errno = EPROTO;
                return false;
            }
            content_length_present = true;
        }
        line_begin = line_end + 2;
    }

    // there could be other status codes without a response body, however they
    // still have to set the header for the response to be valid. Unless status 204
    if (!content_length_present) {
        if (response->status_code != 204) {
            errno = EPROTO;
            return false;
        }
        content_length = 0;
    }
    if (content_length > MAX_RESPONSE_BODY_SIZE) {
        errno = EMSGSIZE;
        return false;
    }

    unsigned char *body = malloc(content_length + 1);
    if (body == NULL) {
        return false;
    }

    // status line and headers no longer needed. Move them out of buffer
    size_t buffered = connection->used - header_size;
    memmove(connection->buffer, connection->buffer + header_size, buffered);
    connection->used = buffered;

    // copy payload bytes without potential next request data
    size_t copied = content_length < connection->used ? content_length : connection->used;
    memcpy(body, connection->buffer, copied);
    memmove(connection->buffer, connection->buffer + copied, connection->used - copied);
    connection->used -= copied;

    while (copied < content_length) {
        ssize_t received = recv(connection->fd, body + copied, content_length - copied, 0);
        if (received > 0) {
            copied += (size_t)received;
            continue;
        }
        if (received == 0) {
            free(body);
            errno = ECONNRESET;
            return false;
        }
        if (errno != EINTR) {
            free(body);
            return false;
        }
    }

    body[content_length] = '\0';
    response->body = body;
    response->body_length = content_length;
    return true;
}

/* Request creation
 *
 *
 *
 *
 */

static bool format_authority(char *buffer, size_t buffer_size, const char *hostname,
                             const char *port) {
    int length;
    if (strchr(hostname, ':') != NULL) {
        // hostname is ipv6
        length = snprintf(buffer, buffer_size, "[%s]:%s", hostname, port);
    } else {
        // hostname is ipv4 or DNS resolvable name
        length = snprintf(buffer, buffer_size, "%s:%s", hostname, port);
    }
    if (length < 0 || (size_t)length >= buffer_size) {
        errno = ENAMETOOLONG;
        return false;
    }
    return true;
}

static bool send_get_request(struct http_connection *connection, const char *hostname,
                             const char *port, const char *path, bool close_connection) {
    char authority[512];
    if (!format_authority(authority, sizeof authority, hostname, port)) {
        return false;
    }

    char request[1024];
    int length = snprintf(request, sizeof request,
                          "GET %s HTTP/1.1\r\n"
                          "Host: %s\r\n"
                          "Connection: %s\r\n"
                          "\r\n",
                          path, authority, close_connection ? "close" : "keep-alive");
    if (length < 0 || (size_t)length >= sizeof request) {
        errno = ENAMETOOLONG;
        return false;
    }
    return send_all(connection->fd, request, (size_t)length);
}

static bool send_put_request(struct http_connection *connection, const char *hostname,
                             const char *port, const char *path, const unsigned char *body,
                             size_t body_length, bool close_connection) {
    char authority[512];
    if (!format_authority(authority, sizeof authority, hostname, port)) {
        return false;
    }

    char request[1024];
    int length = snprintf(request, sizeof request,
                          "PUT %s HTTP/1.1\r\n"
                          "Host: %s\r\n"
                          "Content-Type: text/html\r\n"
                          "Content-Length: %zu\r\n"
                          "Connection: %s\r\n"
                          "\r\n",
                          path, authority, body_length, close_connection ? "close" : "keep-alive");
    if (length < 0 || (size_t)length >= sizeof request) {
        errno = ENAMETOOLONG;
        return false;
    }
    return send_all(connection->fd, request, (size_t)length) &&
           send_all(connection->fd, body, body_length);
}

/* Single-request operations
 *
 *
 *
 *
 */

static bool perform_get(const char *hostname, const char *port, const char *path,
                        struct http_response *response) {
    int socket_fd = connect_to_server(hostname, port);
    if (socket_fd < 0) {
        return false;
    }

    struct http_connection connection = {.fd = socket_fd};
    bool succeeded = send_get_request(&connection, hostname, port, path, true) &&
                     receive_http_response(&connection, response);
    int saved_error = errno;
    close(socket_fd);
    errno = saved_error;
    return succeeded;
}

static bool perform_put(const char *hostname, const char *port, const char *path,
                        const unsigned char *body, size_t body_length,
                        struct http_response *response) {
    int socket_fd = connect_to_server(hostname, port);
    if (socket_fd < 0) {
        return false;
    }

    struct http_connection connection = {.fd = socket_fd};
    bool succeeded = send_put_request(&connection, hostname, port, path, body, body_length, true) &&
                     receive_http_response(&connection, response);
    int saved_error = errno;
    close(socket_fd);
    errno = saved_error;
    return succeeded;
}

/* Local output
 *
 *
 *
 *
 */

static bool save_body(const char *filename, const unsigned char *body, size_t body_length) {
    int file_fd = open(filename, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (file_fd < 0) {
        return false;
    }

    bool succeeded = write_file_all(file_fd, body, body_length);
    int saved_error = succeeded ? 0 : errno;
    if (close(file_fd) < 0 && succeeded) {
        succeeded = false;
        saved_error = errno;
    }
    if (!succeeded) {
        errno = saved_error;
    }
    return succeeded;
}

static void print_body(const char *label, const unsigned char *body, size_t body_length) {
    pthread_mutex_lock(&output_mutex);
    printf("\n%s (%zu bytes):\n", label, body_length);
    if (body_length > 0) {
        fwrite(body, 1, body_length, stdout);
        if (body[body_length - 1] != '\n') {
            fputc('\n', stdout);
        }
    }
    fflush(stdout);
    pthread_mutex_unlock(&output_mutex);
}

/* Thread start gate
 *
 *
 *
 *
 */

static bool initialize_start_gate(struct start_gate *gate) {
    gate->open = false;
    int error = pthread_mutex_init(&gate->mutex, NULL);
    if (error != 0) {
        errno = error;
        return false;
    }
    error = pthread_cond_init(&gate->condition, NULL);
    if (error != 0) {
        pthread_mutex_destroy(&gate->mutex);
        errno = error;
        return false;
    }
    return true;
}

static void destroy_start_gate(struct start_gate *gate) {
    pthread_cond_destroy(&gate->condition);
    pthread_mutex_destroy(&gate->mutex);
}

static bool wait_for_start(struct start_gate *gate) {
    int error = pthread_mutex_lock(&gate->mutex);
    if (error != 0) {
        errno = error;
        return false;
    }
    while (!gate->open) {
        error = pthread_cond_wait(&gate->condition, &gate->mutex);
        if (error != 0) {
            pthread_mutex_unlock(&gate->mutex);
            errno = error;
            return false;
        }
    }
    error = pthread_mutex_unlock(&gate->mutex);
    if (error != 0) {
        errno = error;
        return false;
    }
    return true;
}

static bool open_start_gate(struct start_gate *gate) {
    int error = pthread_mutex_lock(&gate->mutex);
    if (error != 0) {
        errno = error;
        return false;
    }
    gate->open = true;
    error = pthread_cond_broadcast(&gate->condition);
    int unlock_error = pthread_mutex_unlock(&gate->mutex);
    if (error == 0) {
        error = unlock_error;
    }
    if (error != 0) {
        errno = error;
        return false;
    }
    return true;
}

/* Parallel GET test
 *
 *
 *
 *
 */

static void *fetch_thread(void *argument) {
    struct fetch_thread_arguments *arguments = argument;
    if (!wait_for_start(arguments->gate)) {
        report_error("wait for fetch start", errno);
        return NULL;
    }

    struct http_response response;
    if (!perform_get(arguments->hostname, arguments->port, demo_remote_path, &response)) {
        report_error("GET", errno);
        return NULL;
    }
    if (response.status_code != 200) {
        report_status_error("GET", response.status_code);
        destroy_http_response(&response);
        return NULL;
    }

    char filename[256];
    int length = snprintf(filename, sizeof filename, "%s/fetch-thread-%zu.html", output_directory,
                          arguments->index);
    if (length < 0 || (size_t)length >= sizeof filename ||
        !save_body(filename, response.body, response.body_length)) {
        report_error("save fetched HTML", length < 0 ? EIO : errno);
        destroy_http_response(&response);
        return NULL;
    }

    char label[128];
    length = snprintf(label, sizeof label, "fetch thread %zu saved %s", arguments->index, filename);
    if (length > 0 && (size_t)length < sizeof label) {
        print_body(label, response.body, response.body_length);
    }
    arguments->succeeded = true;
    destroy_http_response(&response);
    return NULL;
}

static bool run_parallel_fetch_demo(const char *hostname, const char *port) {
    printf("\n=== Parallel GET demonstration (%d client threads) ===\n", FETCH_THREAD_COUNT);

    struct start_gate gate;
    if (!initialize_start_gate(&gate)) {
        report_error("initialize fetch start gate", errno);
        return false;
    }

    pthread_t threads[FETCH_THREAD_COUNT];
    struct fetch_thread_arguments arguments[FETCH_THREAD_COUNT];
    size_t created = 0;
    for (size_t index = 0; index < FETCH_THREAD_COUNT; ++index) {
        arguments[index] = (struct fetch_thread_arguments){
            .hostname = hostname,
            .port = port,
            .gate = &gate,
            .index = index,
            .succeeded = false,
        };
        int error = pthread_create(&threads[index], NULL, fetch_thread, &arguments[index]);
        if (error != 0) {
            report_error("pthread_create fetch thread", error);
            break;
        }
        ++created;
    }

    bool succeeded = created == FETCH_THREAD_COUNT;
    if (!open_start_gate(&gate)) {
        report_error("open fetch start gate", errno);
        succeeded = false;
    }
    for (size_t index = 0; index < created; ++index) {
        int error = pthread_join(threads[index], NULL);
        if (error != 0) {
            report_error("pthread_join fetch thread", error);
            succeeded = false;
        } else if (!arguments[index].succeeded) {
            succeeded = false;
        }
    }
    destroy_start_gate(&gate);
    return succeeded;
}

/* Pipelining test
 *
 *
 *
 *
 */

static bool response_matches(const struct http_response *response, const unsigned char *expected,
                             size_t expected_length) {
    return response->body_length == expected_length &&
           memcmp(response->body, expected, expected_length) == 0;
}

static bool run_pipeline_demo(const char *hostname, const char *port) {
    printf("\n=== HTTP pipelining demonstration ===\n");

    int socket_fd = connect_to_server(hostname, port);
    if (socket_fd < 0) {
        report_error("connect pipeline", errno);
        return false;
    }

    struct http_connection connection = {.fd = socket_fd};
    bool requests_sent = send_get_request(&connection, hostname, port, demo_remote_path, false) &&
                         send_put_request(&connection, hostname, port, demo_remote_path,
                                          pipeline_document, sizeof pipeline_document - 1, false) &&
                         send_get_request(&connection, hostname, port, demo_remote_path, true);
    if (!requests_sent) {
        int error = errno;
        close(socket_fd);
        report_error("send pipelined requests", error);
        return false;
    }

    printf("Sent GET, PUT, GET back-to-back before reading any response.\n");

    struct http_response before;
    struct http_response update;
    struct http_response after;
    bool succeeded = receive_http_response(&connection, &before);
    if (!succeeded) {
        report_error("receive first pipelined response", errno);
        close(socket_fd);
        return false;
    }
    succeeded = receive_http_response(&connection, &update);
    if (!succeeded) {
        report_error("receive second pipelined response", errno);
        destroy_http_response(&before);
        close(socket_fd);
        return false;
    }
    succeeded = receive_http_response(&connection, &after);
    if (!succeeded) {
        report_error("receive third pipelined response", errno);
        destroy_http_response(&before);
        destroy_http_response(&update);
        close(socket_fd);
        return false;
    }
    close(socket_fd);

    if (before.status_code != 200 || update.status_code != 204 || after.status_code != 200 ||
        !response_matches(&before, initial_document, sizeof initial_document - 1) ||
        !response_matches(&after, pipeline_document, sizeof pipeline_document - 1)) {
        pthread_mutex_lock(&output_mutex);
        fprintf(stderr,
                "client: pipeline verification failed (statuses: %d, %d, %d; lengths: %zu, "
                "%zu)\n",
                before.status_code, update.status_code, after.status_code, before.body_length,
                after.body_length);
        pthread_mutex_unlock(&output_mutex);
        succeeded = false;
    }

    char before_filename[256];
    char after_filename[256];
    int before_length = snprintf(before_filename, sizeof before_filename, "%s/pipeline-before.html",
                                 output_directory);
    int after_length =
        snprintf(after_filename, sizeof after_filename, "%s/pipeline-after.html", output_directory);
    if (before_length < 0 || (size_t)before_length >= sizeof before_filename || after_length < 0 ||
        (size_t)after_length >= sizeof after_filename ||
        !save_body(before_filename, before.body, before.body_length) ||
        !save_body(after_filename, after.body, after.body_length)) {
        report_error("save pipelined HTML", errno);
        succeeded = false;
    }

    print_body("pipeline GET before PUT", before.body, before.body_length);
    print_body("pipeline GET after PUT", after.body, after.body_length);
    if (succeeded) {
        printf("Pipeline responses arrived in order and both GET bodies matched the expected "
               "versions.\n");
    }

    destroy_http_response(&before);
    destroy_http_response(&update);
    destroy_http_response(&after);
    return succeeded;
}

/* Concurrent PUT/GET test
 *
 *
 *
 *
 */

static void sleep_milliseconds(long milliseconds) {
    struct timespec remaining = {
        .tv_sec = milliseconds / 1000,
        .tv_nsec = (milliseconds % 1000) * 1000000L,
    };
    while (nanosleep(&remaining, &remaining) < 0 && errno == EINTR) {
    }
}

static void *stress_writer_thread(void *argument) {
    struct stress_thread_arguments *arguments = argument;
    struct stress_context *context = arguments->context;
    if (!wait_for_start(&context->gate)) {
        report_error("wait for writer start", errno);
        atomic_store(&context->failed, true);
        atomic_fetch_sub(&context->active_writers, 1);
        return NULL;
    }

    const unsigned char *payload = stress_payloads[arguments->index];
    for (size_t iteration = 0; iteration < WRITES_PER_WRITER; ++iteration) {
        struct http_response response;
        if (!perform_put(context->hostname, context->port, stress_remote_path, payload,
                         STRESS_PAYLOAD_LENGTH, &response)) {
            report_error("concurrent PUT", errno);
            atomic_store(&context->failed, true);
            break;
        }
        if (response.status_code != 204) {
            report_status_error("concurrent PUT", response.status_code);
            atomic_store(&context->failed, true);
            destroy_http_response(&response);
            break;
        }

        destroy_http_response(&response);

        pthread_mutex_lock(&output_mutex);
        printf("writer %zu uploaded %.*s, iteration %zu\n", arguments->index, STRESS_PAYLOAD_LENGTH,
               (const char *)payload, iteration + 1);
        pthread_mutex_unlock(&output_mutex);
        sleep_milliseconds(20);
    }

    atomic_fetch_sub(&context->active_writers, 1);
    return NULL;
}

static void *stress_reader_thread(void *argument) {
    struct stress_thread_arguments *arguments = argument;
    struct stress_context *context = arguments->context;
    if (!wait_for_start(&context->gate)) {
        report_error("wait for reader start", errno);
        atomic_store(&context->failed, true);
        return NULL;
    }

    for (;;) {
        struct http_response response;
        if (!perform_get(context->hostname, context->port, stress_remote_path, &response)) {
            report_error("concurrent GET", errno);
            atomic_store(&context->failed, true);
            break;
        }
        if (response.status_code != 200) {
            report_status_error("concurrent GET", response.status_code);
            atomic_store(&context->failed, true);
            destroy_http_response(&response);
            break;
        }

        const unsigned char *matched_payload = NULL;
        for (size_t index = 0; index < STRESS_WRITER_COUNT; ++index) {
            if (response_matches(&response, stress_payloads[index], STRESS_PAYLOAD_LENGTH)) {
                matched_payload = stress_payloads[index];
                break;
            }
        }
        if (matched_payload == NULL) {
            pthread_mutex_lock(&output_mutex);
            fprintf(stderr, "reader %zu received an invalid stress payload of %zu bytes\n",
                    arguments->index, response.body_length);
            pthread_mutex_unlock(&output_mutex);
            atomic_store(&context->failed, true);
            destroy_http_response(&response);
            break;
        }

        ++arguments->successful_reads;
        pthread_mutex_lock(&output_mutex);
        printf("reader %zu received %.*s, read %zu\n", arguments->index, STRESS_PAYLOAD_LENGTH,
               (const char *)matched_payload, arguments->successful_reads);
        pthread_mutex_unlock(&output_mutex);
        destroy_http_response(&response);

        // stop fetching once the writers are done
        if (atomic_load(&context->active_writers) == 0) {
            break;
        }
        sleep_milliseconds(5);
    }
    return NULL;
}

static bool run_concurrent_update_demo(const char *hostname, const char *port) {
    printf("\n=== Concurrent atomic PUT/GET demonstration ===\n");
    printf("%d writers replace %s with ten 1s or ten 2s while %d readers verify every GET.\n",
           STRESS_WRITER_COUNT, stress_remote_path, STRESS_READER_COUNT);

    struct http_response setup_response;
    if (!perform_put(hostname, port, stress_remote_path, stress_payloads[0], STRESS_PAYLOAD_LENGTH,
                     &setup_response)) {
        report_error("initialize stress file", errno);
        return false;
    }
    if (setup_response.status_code != 201 && setup_response.status_code != 204) {
        report_status_error("initialize stress file", setup_response.status_code);
        destroy_http_response(&setup_response);
        return false;
    }
    destroy_http_response(&setup_response);

    struct stress_context context = {
        .hostname = hostname,
        .port = port,
    };
    atomic_init(&context.active_writers, 0);
    atomic_init(&context.failed, false);
    if (!initialize_start_gate(&context.gate)) {
        report_error("initialize stress start gate", errno);
        return false;
    }

    bool succeeded = true;
    pthread_t writer_threads[STRESS_WRITER_COUNT];
    pthread_t reader_threads[STRESS_READER_COUNT];
    struct stress_thread_arguments writer_arguments[STRESS_WRITER_COUNT];
    struct stress_thread_arguments reader_arguments[STRESS_READER_COUNT];
    size_t writers_created = 0;
    size_t readers_created = 0;

    for (size_t index = 0; index < STRESS_WRITER_COUNT; ++index) {
        writer_arguments[index] = (struct stress_thread_arguments){
            .context = &context,
            .index = index,
        };
        int error = pthread_create(&writer_threads[index], NULL, stress_writer_thread,
                                   &writer_arguments[index]);
        if (error != 0) {
            report_error("pthread_create writer", error);
            succeeded = false;
            break;
        }
        ++writers_created;
    }
    for (size_t index = 0; index < STRESS_READER_COUNT; ++index) {
        reader_arguments[index] = (struct stress_thread_arguments){
            .context = &context,
            .index = index,
        };
        int error = pthread_create(&reader_threads[index], NULL, stress_reader_thread,
                                   &reader_arguments[index]);
        if (error != 0) {
            report_error("pthread_create reader", error);
            succeeded = false;
            break;
        }
        ++readers_created;
    }

    atomic_store(&context.active_writers, (int)writers_created);
    if (writers_created == 0 || readers_created == 0) {
        succeeded = false;
    }
    if (!open_start_gate(&context.gate)) {
        report_error("open stress start gate", errno);
        succeeded = false;
    }

    for (size_t index = 0; index < writers_created; ++index) {
        int error = pthread_join(writer_threads[index], NULL);
        if (error != 0) {
            report_error("pthread_join writer", error);
            succeeded = false;
        }
    }
    size_t total_reads = 0;
    for (size_t index = 0; index < readers_created; ++index) {
        int error = pthread_join(reader_threads[index], NULL);
        if (error != 0) {
            report_error("pthread_join reader", error);
            succeeded = false;
        }
        total_reads += reader_arguments[index].successful_reads;
    }

    if (atomic_load(&context.failed)) {
        succeeded = false;
    }
    if (succeeded) {
        printf("Concurrent verification passed: %zu GET responses were exactly ten 1s or ten "
               "2s.\n",
               total_reads);
    }

    destroy_start_gate(&context.gate);
    return succeeded;
}

/* Program setup
 *
 *
 *
 *
 */

static bool ensure_output_directory(void) {
    if (mkdir(output_directory, 0755) == 0) {
        return true;
    }
    if (errno != EEXIST) {
        return false;
    }

    struct stat information;
    if (stat(output_directory, &information) < 0) {
        return false;
    }
    if (!S_ISDIR(information.st_mode)) {
        errno = ENOTDIR;
        return false;
    }
    return true;
}

static void print_usage(const char *program_name) {
    fprintf(stderr, "Usage: %s [-a hostname] [-P port]\n", program_name);
}

int main(int argc, char **argv) {
    const char *hostname = "localhost";
    int port = 3490;

    int option;
    while ((option = getopt(argc, argv, "a:P:")) != -1) {
        switch (option) {
        case 'a':
            if (*optarg == '\0') {
                fprintf(stderr, "Hostname must not be empty\n");
                return EXIT_FAILURE;
            }
            hostname = optarg;
            break;
        case 'P': {
            char *end;
            errno = 0;
            long parsed_port = strtol(optarg, &end, 10);
            if (errno != 0 || *optarg == '\0' || *end != '\0' || parsed_port < 1 ||
                parsed_port > 65535) {
                fprintf(stderr, "Invalid port number: %s\n", optarg);
                return EXIT_FAILURE;
            }
            port = (int)parsed_port;
            break;
        }
        case '?':
        default:
            print_usage(argv[0]);
            return EXIT_FAILURE;
        }
    }
    if (optind != argc) {
        print_usage(argv[0]);
        return EXIT_FAILURE;
    }

    struct sigaction signal_action;
    memset(&signal_action, 0, sizeof signal_action);
    signal_action.sa_handler = SIG_IGN;
    sigemptyset(&signal_action.sa_mask);
    if (sigaction(SIGPIPE, &signal_action, NULL) < 0) {
        perror("client: sigaction");
        return EXIT_FAILURE;
    }

    if (!ensure_output_directory()) {
        perror("client: output directory");
        return EXIT_FAILURE;
    }

    char port_string[6];
    int port_length = snprintf(port_string, sizeof port_string, "%d", port);
    if (port_length < 0 || (size_t)port_length >= sizeof port_string) {
        fprintf(stderr, "client: could not format port\n");
        return EXIT_FAILURE;
    }

    printf("Connecting to %s:%s and using remote resource %s\n", hostname, port_string,
           demo_remote_path);
    printf("Received files are stored in ./%s/\n", output_directory);

    struct http_response setup_response;
    if (!perform_put(hostname, port_string, demo_remote_path, initial_document,
                     sizeof initial_document - 1, &setup_response)) {
        perror("client: initialize remote HTML file");
        return EXIT_FAILURE;
    }
    if (setup_response.status_code != 201 && setup_response.status_code != 204) {
        report_status_error("initial PUT", setup_response.status_code);
        destroy_http_response(&setup_response);
        return EXIT_FAILURE;
    }
    destroy_http_response(&setup_response);

    bool fetch_succeeded = run_parallel_fetch_demo(hostname, port_string);
    bool pipeline_succeeded = fetch_succeeded && run_pipeline_demo(hostname, port_string);
    bool stress_succeeded = pipeline_succeeded && run_concurrent_update_demo(hostname, port_string);

    if (!fetch_succeeded || !pipeline_succeeded || !stress_succeeded) {
        fprintf(stderr, "\nClient demonstration failed.\n");
        return EXIT_FAILURE;
    }

    printf("\nAll client demonstrations completed successfully.\n");
    return EXIT_SUCCESS;
}
