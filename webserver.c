#define _POSIX_C_SOURCE 200809L
#define _DARWIN_C_SOURCE

#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <netdb.h>
#include <pthread.h>
#include <signal.h>
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

#define BACKLOG 10
// 8 KiB (also the maximum header size)
#define CONNECTION_BUFFER_SIZE (8 * 1024)
// Start with space for 8 headers and grow up to this limit.
#define MAX_HEADER_COUNT 127
// 1 MiB
#define MAX_REQUEST_SIZE (1024 * 1024)
#define CONNECTION_TIMEOUT_SECONDS 30
#define PUT_TEMP_FILE_PREFIX ".webserver-put-"
#define PUT_TEMP_FILE_ATTEMPTS 100

static const char *document_root = "./content";
static pthread_mutex_t put_temp_counter_mutex = PTHREAD_MUTEX_INITIALIZER;
static uintmax_t put_temp_counter = 0;

/* Datatypes
 *
 *
 *
 *
 */

struct http_text {
    const char *data;
    size_t length;
};

struct http_bytes {
    const unsigned char *data;
    size_t length;
};

struct http_header {
    struct http_text name;
    struct http_text value;
};

enum http_version { HTTP_VERSION_1_0, HTTP_VERSION_1_1, HTTP_UNSUPPORTED_VERSION };

enum http_method { HTTP_METHOD_GET, HTTP_METHOD_POST, HTTP_METHOD_PUT, HTTP_METHOD_UNSUPPORTED };

enum http_body_kind { HTTP_BODY_EMPTY, HTTP_BODY_MEMORY, HTTP_BODY_FILE };

enum request_parse_result {
    REQUEST_PARSE_OK,
    REQUEST_PARSE_INVALID,
    REQUEST_PARSE_OUT_OF_MEMORY,
    REQUEST_PARSE_TOO_MANY_HEADERS
};

struct http_request {
    char *storage;
    size_t storage_size;

    struct http_text method_text;
    struct http_text uri;
    struct http_text path;

    enum http_method method;
    enum http_version version;

    struct http_header *headers;
    size_t header_count;
    size_t header_capacity;

    struct http_bytes data;
};

struct http_response {
    enum http_version version;
    const char *status;
    struct http_header *headers;
    size_t header_count;
    size_t header_capacity;
    bool keep_alive;

    enum http_body_kind body_kind;
    union {
        struct {
            unsigned char *data;
            size_t length;
        } memory;
        struct {
            int fd;
            uintmax_t length;
        } file;
    } body;
};

static bool http_text_equals_ignore_case(struct http_text text, const char *expected) {
    size_t expected_length = strlen(expected);
    return text.length == expected_length && strncasecmp(text.data, expected, expected_length) == 0;
}

static struct http_header *append_http_header(struct http_header **headers, size_t *header_count,
                                              size_t *header_capacity) {
    if (*header_count >= MAX_HEADER_COUNT) {
        errno = EMSGSIZE;
        return NULL;
    }

    if (*header_count == *header_capacity) {
        size_t new_capacity = *header_capacity == 0 ? 8 : *header_capacity * 2;
        if (new_capacity > MAX_HEADER_COUNT) {
            new_capacity = MAX_HEADER_COUNT;
        }
        if (new_capacity > SIZE_MAX / sizeof **headers) {
            errno = ENOMEM;
            return NULL;
        }

        struct http_header *new_headers = realloc(*headers, new_capacity * sizeof **headers);
        if (new_headers == NULL) {
            errno = ENOMEM;
            return NULL;
        }
        *headers = new_headers;
        *header_capacity = new_capacity;
    }

    return &(*headers)[(*header_count)++];
}

static enum request_parse_result add_request_header(struct http_request *request,
                                                    const char *name_begin, const char *name_end,
                                                    const char *value_begin,
                                                    const char *value_end) {
    struct http_header *header =
        append_http_header(&request->headers, &request->header_count, &request->header_capacity);
    if (header == NULL) {
        return errno == EMSGSIZE ? REQUEST_PARSE_TOO_MANY_HEADERS : REQUEST_PARSE_OUT_OF_MEMORY;
    }
    header->name.data = name_begin;
    header->name.length = (size_t)(name_end - name_begin);
    header->value.data = value_begin;
    header->value.length = (size_t)(value_end - value_begin);
    return REQUEST_PARSE_OK;
}

static bool http_response_add_header(struct http_response *response, const char *name,
                                     const char *value) {
    if (name == NULL || value == NULL || *name == '\0') {
        errno = EINVAL;
        return false;
    }

    size_t name_length = strlen(name);
    size_t value_length = strlen(value);

    for (size_t index = 0; index < name_length; ++index) {
        unsigned char character = (unsigned char)name[index];
        if (!((character >= '0' && character <= '9') || (character >= 'A' && character <= 'Z') ||
              (character >= 'a' && character <= 'z') ||
              strchr("!#$%&'*+-.^_`|~", character) != NULL)) {
            errno = EINVAL;
            return false;
        }
    }

    for (size_t index = 0; index < value_length; ++index) {
        unsigned char character = (unsigned char)value[index];
        if ((character < 0x20 && character != '\t') || character == 0x7f) {
            errno = EINVAL;
            return false;
        }
    }

    char *name_copy = malloc(name_length + 1);
    if (name_copy == NULL) {
        return false;
    }
    char *value_copy = malloc(value_length + 1);
    if (value_copy == NULL) {
        free(name_copy);
        return false;
    }
    memcpy(name_copy, name, name_length + 1);
    memcpy(value_copy, value, value_length + 1);

    struct http_header *header =
        append_http_header(&response->headers, &response->header_count, &response->header_capacity);
    if (header == NULL) {
        free(name_copy);
        free(value_copy);
        return false;
    }
    header->name.data = name_copy;
    header->name.length = name_length;
    header->value.data = value_copy;
    header->value.length = value_length;
    return true;
}

static void relocate_http_request(struct http_request *request, const char *old_storage,
                                  char *new_storage, size_t storage_size, size_t header_size,
                                  size_t body_size) {
    // move the request from one buffer to another, mainitng the relative pointer values into the
    // buffer
    request->method_text.data = new_storage + (request->method_text.data - old_storage);
    request->uri.data = new_storage + (request->uri.data - old_storage);
    request->path.data = new_storage + (request->path.data - old_storage);

    for (size_t index = 0; index < request->header_count; ++index) {
        request->headers[index].name.data =
            new_storage + (request->headers[index].name.data - old_storage);
        request->headers[index].value.data =
            new_storage + (request->headers[index].value.data - old_storage);
    }

    request->storage = new_storage;
    request->storage_size = storage_size;
    request->data.data = (const unsigned char *)(new_storage + header_size);
    request->data.length = body_size;
}

static void destroy_http_request(struct http_request *request) {
    free(request->headers);
    free(request->storage);
    memset(request, 0, sizeof *request);
}

static void clear_http_response_body(struct http_response *response) {
    switch (response->body_kind) {
    case HTTP_BODY_MEMORY:
        free(response->body.memory.data);
        break;
    case HTTP_BODY_FILE:
        if (response->body.file.fd >= 0) {
            close(response->body.file.fd);
        }
        break;
    case HTTP_BODY_EMPTY:
        break;
    }

    memset(&response->body, 0, sizeof response->body);
    response->body_kind = HTTP_BODY_EMPTY;
}

static void destroy_http_response(struct http_response *response) {
    clear_http_response_body(response);
    for (size_t index = 0; index < response->header_count; ++index) {
        free((void *)response->headers[index].name.data);
        free((void *)response->headers[index].value.data);
    }
    free(response->headers);
    memset(response, 0, sizeof *response);
}

static void http_response_set_empty(struct http_response *response, const char *status) {
    clear_http_response_body(response);
    response->status = status;
}

static void http_response_set_file(struct http_response *response, const char *status, int file_fd,
                                   uintmax_t length) {
    clear_http_response_body(response);
    response->status = status;
    response->body_kind = HTTP_BODY_FILE;
    response->body.file.fd = file_fd;
    response->body.file.length = length;
}

/* utils
 *
 *
 *
 */
static void log_request(const struct http_request *request, bool content_length_present,
                        size_t content_length) {
    struct timespec timestamp;
    if (clock_gettime(CLOCK_REALTIME, &timestamp) < 0) {
        perror("server: clock_gettime");
        return;
    }

    intmax_t unix_milliseconds =
        (intmax_t)timestamp.tv_sec * 1000 + (intmax_t)(timestamp.tv_nsec / 1000000L);
    const char *version = request->version == HTTP_VERSION_1_0 ? "HTTP/1.0" : "HTTP/1.1";

    flockfile(stdout);
    fprintf(stdout, "%" PRIdMAX " - %.*s %.*s %s - Content-Length: ", unix_milliseconds,
            (int)request->method_text.length, request->method_text.data, (int)request->uri.length,
            request->uri.data, version);
    if (content_length_present) {
        fprintf(stdout, "%zu", content_length);
    } else {
        fputs("None", stdout);
    }
    fputc('\n', stdout);
    fflush(stdout);
    funlockfile(stdout);
}

static void print_listening_address(int sockfd) {
    struct sockaddr_storage address;
    socklen_t address_length = sizeof address;

    if (getsockname(sockfd, (struct sockaddr *)&address, &address_length) < 0) {
        perror("server: getsockname");
        return;
    }

    char host[128];
    char service[6];
    int status = getnameinfo((struct sockaddr *)&address, address_length, host, sizeof host,
                             service, sizeof service, NI_NUMERICHOST | NI_NUMERICSERV);
    if (status != 0) {
        fprintf(stderr, "server: getnameinfo: %s\n", gai_strerror(status));
        return;
    }

    if (address.ss_family == AF_INET6) {
        printf("server: listening on [%s]:%s\n", host, service);
    } else {
        printf("server: listening on %s:%s\n", host, service);
    }
    fflush(stdout);
}

/*
 * HTTP parsing;
 *
 *
 *
 *
 */
static const char *find_header_end(const char *begin, const char *end) {
    size_t length = (size_t)(end - begin);

    for (size_t index = 0; index + 3 < length; ++index) {
        if (begin[index] == '\r' && begin[index + 1] == '\n' && begin[index + 2] == '\r' &&
            begin[index + 3] == '\n') {
            return begin + index + 4;
        }
    }

    return NULL;
}
static const char *find_crlf(const char *begin, const char *end) {
    for (const char *position = begin; position + 1 < end; ++position) {
        if (position[0] == '\r' && position[1] == '\n') {
            return position;
        }
    }
    return NULL;
}

static enum request_parse_result parse_http_request_line(struct http_request *request,
                                                         const char *begin, const char *end) {
    const char *method_end = memchr(begin, ' ', (size_t)(end - begin));
    if (method_end == NULL || method_end == begin) {
        return REQUEST_PARSE_INVALID;
    }

    const char *uri_begin = method_end + 1;
    const char *uri_end = memchr(uri_begin, ' ', (size_t)(end - uri_begin));
    if (uri_end == NULL || uri_end == uri_begin) {
        return REQUEST_PARSE_INVALID;
    }

    // check for disallowed characters
    for (const char *position = begin; position < method_end; ++position) {
        unsigned char character = (unsigned char)*position;
        if (!((character >= '0' && character <= '9') || (character >= 'A' && character <= 'Z') ||
              (character >= 'a' && character <= 'z') ||
              strchr("!#$%&'*+-.^_`|~", character) != NULL)) {
            return REQUEST_PARSE_INVALID;
        }
    }

    // disallow ASCII control characters including space, as well as DEL
    // not perfect but better than nothing.
    for (const char *position = uri_begin; position < uri_end; ++position) {
        unsigned char character = (unsigned char)*position;
        if (character <= 0x20 || character == 0x7f) {
            return REQUEST_PARSE_INVALID;
        }
    }

    // extract path
    struct http_text path = {0};
    const char *components_end = uri_end;
    const char *query_begin = memchr(uri_begin, '?', (size_t)(uri_end - uri_begin));
    const char *fragment_begin = memchr(uri_begin, '#', (size_t)(uri_end - uri_begin));
    if (query_begin != NULL && query_begin < components_end) {
        components_end = query_begin;
    }
    if (fragment_begin != NULL && fragment_begin < components_end) {
        components_end = fragment_begin;
    }

    if (*uri_begin == '/') {
        path.data = uri_begin;
        path.length = (size_t)(components_end - uri_begin);
    } else if ((size_t)(uri_end - uri_begin) == 1 && *uri_begin == '*') {
        path.data = uri_begin;
        path.length = 1;
    } else {
        size_t scheme_length;
        if ((size_t)(uri_end - uri_begin) >= sizeof "http://" - 1 &&
            strncasecmp(uri_begin, "http://", sizeof "http://" - 1) == 0) {
            scheme_length = sizeof "http://" - 1;
        } else if ((size_t)(uri_end - uri_begin) >= sizeof "https://" - 1 &&
                   strncasecmp(uri_begin, "https://", sizeof "https://" - 1) == 0) {
            scheme_length = sizeof "https://" - 1;
        } else {
            return REQUEST_PARSE_INVALID;
        }

        const char *authority_begin = uri_begin + scheme_length;
        const char *path_begin =
            memchr(authority_begin, '/', (size_t)(components_end - authority_begin));
        const char *authority_end = path_begin == NULL ? components_end : path_begin;
        if (authority_begin == authority_end) {
            return REQUEST_PARSE_INVALID;
        }

        path.data = path_begin == NULL ? components_end : path_begin;
        path.length = path_begin == NULL ? 0 : (size_t)(components_end - path_begin);
    }

    // extract version
    const char *version_begin = uri_end + 1;
    if ((size_t)(end - version_begin) != sizeof "HTTP/1.1" - 1 ||
        memcmp(version_begin, "HTTP/", sizeof "HTTP/" - 1) != 0 || version_begin[5] < '0' ||
        version_begin[5] > '9' || version_begin[6] != '.' || version_begin[7] < '0' ||
        version_begin[7] > '9') {
        return REQUEST_PARSE_INVALID;
    }

    if (memcmp(version_begin, "HTTP/1.0", sizeof "HTTP/1.0" - 1) == 0) {
        request->version = HTTP_VERSION_1_0;
    } else if (memcmp(version_begin, "HTTP/1.1", sizeof "HTTP/1.1" - 1) == 0) {
        request->version = HTTP_VERSION_1_1;
    } else {
        request->version = HTTP_UNSUPPORTED_VERSION;
    }

    // extract method
    size_t method_length = (size_t)(method_end - begin);
    request->method_text.data = begin;
    request->method_text.length = method_length;
    if (method_length == sizeof "GET" - 1 && memcmp(begin, "GET", method_length) == 0) {
        request->method = HTTP_METHOD_GET;
    } else if (method_length == sizeof "POST" - 1 && memcmp(begin, "POST", method_length) == 0) {
        request->method = HTTP_METHOD_POST;
    } else if (method_length == sizeof "PUT" - 1 && memcmp(begin, "PUT", method_length) == 0) {
        request->method = HTTP_METHOD_PUT;
    } else {
        request->method = HTTP_METHOD_UNSUPPORTED;
    }
    request->uri.data = uri_begin;
    request->uri.length = (size_t)(uri_end - uri_begin);
    request->path = path;
    return REQUEST_PARSE_OK;
}

static enum request_parse_result parse_http_request_headers(struct http_request *request,
                                                            const char *begin, const char *end) {
    const char *line_begin = begin;
    while (line_begin < end) {
        const char *line_end = find_crlf(line_begin, end);
        if (line_end == NULL) {
            return REQUEST_PARSE_INVALID;
        }

        const char *colon = memchr(line_begin, ':', (size_t)(line_end - line_begin));
        if (colon == NULL || colon == line_begin) {
            return REQUEST_PARSE_INVALID;
        }
        for (const char *position = line_begin; position < colon; ++position) {
            unsigned char character = (unsigned char)*position;
            if (!((character >= '0' && character <= '9') ||
                  (character >= 'A' && character <= 'Z') ||
                  (character >= 'a' && character <= 'z') ||
                  strchr("!#$%&'*+-.^_`|~", character) != NULL)) {
                return REQUEST_PARSE_INVALID;
            }
        }

        const char *value_begin = colon + 1;
        while (value_begin < line_end && (*value_begin == ' ' || *value_begin == '\t')) {
            ++value_begin;
        }
        const char *value_end = line_end;
        while (value_end > value_begin && (value_end[-1] == ' ' || value_end[-1] == '\t')) {
            --value_end;
        }
        for (const char *position = value_begin; position < value_end; ++position) {
            unsigned char character = (unsigned char)*position;
            if ((character < 0x20 && character != '\t') || character == 0x7f) {
                return REQUEST_PARSE_INVALID;
            }
        }

        enum request_parse_result result =
            add_request_header(request, line_begin, colon, value_begin, value_end);
        if (result != REQUEST_PARSE_OK) {
            return result;
        }
        line_begin = line_end + 2;
    }

    return line_begin == end ? REQUEST_PARSE_OK : REQUEST_PARSE_INVALID;
}

static bool parse_content_length(const char *begin, const char *end, size_t *result) {
    if (begin == end) {
        return false;
    }

    size_t value = 0;
    for (const char *position = begin; position < end; ++position) {
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

/* File handling
 *
 *
 *
 *
 */

struct document_path {
    int parent_fd;
    char *storage;
    const char *filename;
};

static void close_document_path(struct document_path *path) {
    if (path->parent_fd >= 0) {
        close(path->parent_fd);
    }
    free(path->storage);
    path->parent_fd = -1;
    path->storage = NULL;
    path->filename = NULL;
}

static bool is_path_valid(struct http_text path) {
    /*
     *  Path must start with /.
     *  Directory names may contain only A-Z and a-z.
     *  The final filename may contain letters and periods.
     *  Empty components, including // and trailing /, are rejected.
     *  Filenames consisting only of periods, such as . and .., are rejected.
     */
    if (path.length == 0) {
        return true;
    }
    if (path.data == NULL || path.data[0] != '/') {
        return false;
    }
    if (path.length == 1) {
        return true;
    }

    size_t component_begin = 1;
    for (size_t index = 1; index <= path.length; ++index) {
        if (index < path.length && path.data[index] != '/') {
            continue;
        }

        size_t component_length = index - component_begin;
        if (component_length == 0) {
            return false;
        }

        bool is_filename = index == path.length;
        bool contains_letter = false;
        for (size_t position = component_begin; position < index; ++position) {
            unsigned char character = (unsigned char)path.data[position];
            bool is_letter =
                (character >= 'A' && character <= 'Z') || (character >= 'a' && character <= 'z');
            if (!is_letter && !(is_filename && character == '.')) {
                return false;
            }
            contains_letter = contains_letter || is_letter;
        }

        // filename must have at leat one letter and not just dots
        if (is_filename && !contains_letter) {
            return false;
        }

        component_begin = index + 1;
    }

    return true;
}

static int resolve_document_path(struct http_text path, struct document_path *result) {
    result->parent_fd = -1;
    result->storage = NULL;
    result->filename = NULL;

    // remove leading /, otherwise openat does not respect document_root
    size_t path_offset = path.length > 0 && path.data[0] == '/' ? 1 : 0;
    size_t storage_length = path.length - path_offset;
    if (storage_length == 0) {
        errno = EISDIR;
        return -1;
    }

    char *storage = malloc(storage_length + 1);
    if (storage == NULL) {
        return -1;
    }
    memcpy(storage, path.data + path_offset, storage_length);
    storage[storage_length] = '\0';

    int directory_fd = open(document_root, O_RDONLY | O_DIRECTORY | O_NOFOLLOW);
    if (directory_fd < 0) {
        free(storage);
        return -1;
    }

    char *component = storage;
    char *separator;
    while ((separator = strchr(component, '/')) != NULL) {
        *separator = '\0';
        int next_directory = openat(directory_fd, component, O_RDONLY | O_DIRECTORY | O_NOFOLLOW);
        if (next_directory < 0) {
            int saved_errno = errno;
            close(directory_fd);
            free(storage);
            errno = saved_errno;
            return -1;
        }
        close(directory_fd);
        directory_fd = next_directory;
        component = separator + 1;
    }

    result->parent_fd = directory_fd;
    result->storage = storage;
    result->filename = component;
    return 0;
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

static int create_put_temp_file(int parent_fd, char *name, size_t name_size) {
    for (int attempt = 0; attempt < PUT_TEMP_FILE_ATTEMPTS; ++attempt) {
        int error = pthread_mutex_lock(&put_temp_counter_mutex);
        if (error != 0) {
            errno = error;
            return -1;
        }
        uintmax_t number = ++put_temp_counter;
        error = pthread_mutex_unlock(&put_temp_counter_mutex);
        if (error != 0) {
            errno = error;
            return -1;
        }

        int length = snprintf(name, name_size, PUT_TEMP_FILE_PREFIX "%" PRIdMAX "-%" PRIuMAX ".tmp",
                              (intmax_t)getpid(), number);
        if (length < 0 || (size_t)length >= name_size) {
            errno = ENAMETOOLONG;
            return -1;
        }

        int file_fd = openat(parent_fd, name, O_WRONLY | O_CREAT | O_EXCL | O_NOFOLLOW, 0644);
        if (file_fd >= 0 || errno != EEXIST) {
            return file_fd;
        }
    }

    errno = EEXIST;
    return -1;
}

/* HTTP method handlers
 *
 *
 *
 *
 */

static bool path_has_extension(struct http_text path, const char *extension) {
    size_t extension_length = strlen(extension);
    return path.length >= extension_length &&
           strncasecmp(path.data + path.length - extension_length, extension, extension_length) ==
               0;
}

static const char *get_content_type(struct http_text path) {
    if (path_has_extension(path, ".html") || path_has_extension(path, ".htm")) {
        return "text/html; charset=utf-8";
    }
    if (path_has_extension(path, ".css")) {
        return "text/css; charset=utf-8";
    }
    if (path_has_extension(path, ".js")) {
        return "text/javascript; charset=utf-8";
    }
    if (path_has_extension(path, ".json")) {
        return "application/json";
    }
    if (path_has_extension(path, ".txt")) {
        return "text/plain; charset=utf-8";
    }
    if (path_has_extension(path, ".svg")) {
        return "image/svg+xml";
    }
    if (path_has_extension(path, ".png")) {
        return "image/png";
    }
    if (path_has_extension(path, ".jpg") || path_has_extension(path, ".jpeg")) {
        return "image/jpeg";
    }
    if (path_has_extension(path, ".gif")) {
        return "image/gif";
    }
    if (path_has_extension(path, ".pdf")) {
        return "application/pdf";
    }
    return "application/octet-stream";
}

static const char *get_http_status_for_file_error(int error) {
    const char *result;
    switch (error) {
    case ENOENT:
    case ENOTDIR:
    case EISDIR:
        result = "404 Not Found";
        break;
    case EACCES:
    case EPERM:
    case ELOOP:
        result = "403 Forbidden";
        break;

    default:
        result = "500 Internal Server Error";
        break;
    }
    return result;
}

static void set_file_error_response(struct http_response *response, int error) {
    const char *status = get_http_status_for_file_error(error);
    if (strcmp(status, "500 Internal Server Error") == 0) {
        errno = error;
        perror("server: document file");
    }
    http_response_set_empty(response, status);
}

static void handle_get(const struct http_request *request, struct http_response *response) {
    if (!is_path_valid(request->path)) {
        http_response_set_empty(response, "400 Bad Request");
        return;
    }

    struct document_path resolved;
    if (resolve_document_path(request->path, &resolved) < 0) {
        set_file_error_response(response, errno);
        return;
    }

    int file_fd = openat(resolved.parent_fd, resolved.filename, O_RDONLY | O_NONBLOCK | O_NOFOLLOW);
    int open_error = errno;
    close_document_path(&resolved);
    if (file_fd < 0) {
        set_file_error_response(response, open_error);
        return;
    }

    struct stat file_info;
    if (fstat(file_fd, &file_info) < 0) {
        int error = errno;
        close(file_fd);
        set_file_error_response(response, error);
        return;
    }

    if (!S_ISREG(file_info.st_mode) || file_info.st_size < 0) {
        // Only regular files have a stable byte representation suitable for this response.
        close(file_fd);
        http_response_set_empty(response, "404 Not Found");
        return;
    }

    http_response_set_file(response, "200 OK", file_fd, (uintmax_t)file_info.st_size);
    if (!http_response_add_header(response, "Content-Type", get_content_type(request->path))) {
        perror("server: response header allocation");
        http_response_set_empty(response, "500 Internal Server Error");
    }
}

static void handle_post(const struct http_request *request, struct http_response *response) {
    if (!is_path_valid(request->path)) {
        http_response_set_empty(response, "400 Bad Request");
        return;
    }

    struct document_path resolved;
    if (resolve_document_path(request->path, &resolved) < 0) {
        set_file_error_response(response, errno);
        return;
    }

    int file_fd = openat(resolved.parent_fd, resolved.filename,
                         O_WRONLY | O_CREAT | O_EXCL | O_NOFOLLOW, 0644);
    if (file_fd < 0) {
        int error = errno;
        close_document_path(&resolved);
        if (error == EEXIST) {
            http_response_set_empty(response, "409 Conflict");
        } else {
            set_file_error_response(response, error);
        }
        return;
    }

    bool write_succeeded = write_file_all(file_fd, request->data.data, request->data.length);
    int write_error = write_succeeded ? 0 : errno;
    if (close(file_fd) < 0 && write_succeeded) {
        write_succeeded = false;
        write_error = errno;
    }
    if (!write_succeeded) {
        (void)unlinkat(resolved.parent_fd, resolved.filename, 0);
        close_document_path(&resolved);
        errno = write_error;
        perror("server: write document file");
        http_response_set_empty(response, "500 Internal Server Error");
        return;
    }

    close_document_path(&resolved);
    http_response_set_empty(response, "201 Created");
}

static void handle_put(const struct http_request *request, struct http_response *response) {
    if (!is_path_valid(request->path)) {
        http_response_set_empty(response, "400 Bad Request");
        return;
    }

    struct document_path resolved;
    if (resolve_document_path(request->path, &resolved) < 0) {
        set_file_error_response(response, errno);
        return;
    }

    char temporary_name[128];
    int temporary_fd =
        create_put_temp_file(resolved.parent_fd, temporary_name, sizeof temporary_name);
    if (temporary_fd < 0) {
        int error = errno;
        close_document_path(&resolved);
        set_file_error_response(response, error);
        return;
    }

    bool write_succeeded = write_file_all(temporary_fd, request->data.data, request->data.length);
    int write_error = write_succeeded ? 0 : errno;
    if (close(temporary_fd) < 0 && write_succeeded) {
        write_succeeded = false;
        write_error = errno;
    }
    if (!write_succeeded) {
        (void)unlinkat(resolved.parent_fd, temporary_name, 0);
        close_document_path(&resolved);
        errno = write_error;
        perror("server: write document file");
        http_response_set_empty(response, "500 Internal Server Error");
        return;
    }

    struct stat destination_info;
    bool created;
    if (fstatat(resolved.parent_fd, resolved.filename, &destination_info, AT_SYMLINK_NOFOLLOW) ==
        0) {
        if (!S_ISREG(destination_info.st_mode)) {
            (void)unlinkat(resolved.parent_fd, temporary_name, 0);
            close_document_path(&resolved);
            http_response_set_empty(response, "409 Conflict");
            return;
        }
        created = false;
    } else if (errno == ENOENT) {
        created = true;
    } else {
        int error = errno;
        (void)unlinkat(resolved.parent_fd, temporary_name, 0);
        close_document_path(&resolved);
        set_file_error_response(response, error);
        return;
    }

    if (renameat(resolved.parent_fd, temporary_name, resolved.parent_fd, resolved.filename) < 0) {
        int error = errno;
        (void)unlinkat(resolved.parent_fd, temporary_name, 0);
        close_document_path(&resolved);
        set_file_error_response(response, error);
        return;
    }

    close_document_path(&resolved);
    http_response_set_empty(response, created ? "201 Created" : "204 No Content");
}

static void handle_request(const struct http_request *request, struct http_response *response) {
    // path currently not used, in the future we could
    // potentially have different handlers per path.
    switch (request->method) {
    case HTTP_METHOD_GET:
        handle_get(request, response);
        break;
    case HTTP_METHOD_POST:
        handle_post(request, response);
        break;
    case HTTP_METHOD_PUT:
        handle_put(request, response);
        break;
    case HTTP_METHOD_UNSUPPORTED:
    default:
        http_response_set_empty(response, "405 Method Not Allowed");
        if (!http_response_add_header(response, "Allow", "GET, POST, PUT")) {
            perror("server: response header allocation");
            http_response_set_empty(response, "500 Internal Server Error");
        }
        break;
    }
    return;
}

/* Network handlers
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

static bool send_http_response(int sockfd, const struct http_response *response) {
    uintmax_t content_length;
    switch (response->body_kind) {
    case HTTP_BODY_EMPTY:
        content_length = 0;
        break;
    case HTTP_BODY_MEMORY:
        content_length = (uintmax_t)response->body.memory.length;
        break;
    case HTTP_BODY_FILE:
        content_length = response->body.file.length;
        break;
    default:
        errno = EINVAL;
        return false;
    }

    if (response->status == NULL) {
        errno = EINVAL;
        return false;
    }

    const char *version = response->version == HTTP_VERSION_1_0 ? "HTTP/1.0" : "HTTP/1.1";
    bool no_content = strcmp(response->status, "204 No Content") == 0;
    if (no_content && content_length != 0) {
        errno = EINVAL;
        return false;
    }

    char headers[512];
    int length;
    if (no_content) {
        length = snprintf(headers, sizeof headers,
                          "%s %s\r\n"
                          "Connection: %s\r\n",
                          version, response->status, response->keep_alive ? "keep-alive" : "close");
    } else {
        length = snprintf(headers, sizeof headers,
                          "%s %s\r\n"
                          "Content-Length: %" PRIuMAX "\r\n"
                          "Connection: %s\r\n",
                          version, response->status, content_length,
                          response->keep_alive ? "keep-alive" : "close");
    }
    if (length < 0 || (size_t)length >= sizeof headers) {
        return false;
    }
    if (!send_all(sockfd, headers, (size_t)length)) {
        return false;
    }

    for (size_t index = 0; index < response->header_count; ++index) {
        struct http_header header = response->headers[index];

        // these headers are describe the response/ connection and are not set manually
        if (http_text_equals_ignore_case(header.name, "Content-Length") ||
            http_text_equals_ignore_case(header.name, "Connection")) {
            continue;
        }

        if (!send_all(sockfd, header.name.data, header.name.length) || !send_all(sockfd, ": ", 2) ||
            !send_all(sockfd, header.value.data, header.value.length) ||
            !send_all(sockfd, "\r\n", 2)) {
            return false;
        }
    }

    if (!send_all(sockfd, "\r\n", 2)) {
        return false;
    }

    switch (response->body_kind) {
    case HTTP_BODY_EMPTY:
        return true;
    case HTTP_BODY_MEMORY:
        if (response->body.memory.length > 0 && response->body.memory.data == NULL) {
            errno = EINVAL;
            return false;
        }
        return send_all(sockfd, response->body.memory.data, response->body.memory.length);
    case HTTP_BODY_FILE: {
        unsigned char buffer[16 * 1024];
        uintmax_t remaining = response->body.file.length;

        while (remaining > 0) {
            size_t amount = remaining < sizeof buffer ? (size_t)remaining : sizeof buffer;
            ssize_t received = read(response->body.file.fd, buffer, amount);
            if (received < 0) {
                if (errno == EINTR) {
                    continue;
                }
                return false;
            }
            if (received == 0) {
                errno = EIO;
                return false;
            }
            if (!send_all(sockfd, buffer, (size_t)received)) {
                return false;
            }
            remaining -= (uintmax_t)received;
        }

        return true;
    }
    default:
        errno = EINVAL;
        return false;
    }
}

static bool receive_request_bytes(int sockfd, char *buffer, size_t *used) {
    while (*used < CONNECTION_BUFFER_SIZE) {
        ssize_t received = recv(sockfd, buffer + *used, CONNECTION_BUFFER_SIZE - *used, 0);
        if (received > 0) {
            *used += (size_t)received;
            return true;
        }
        if (received == 0) {
            errno = 0;
            return false;
        }
        if (errno != EINTR) {
            int error = errno;
            perror("server: receive");
            errno = error;
            return false;
        }
    }

    errno = EMSGSIZE;
    return false;
}

static bool configure_connection_timeouts(int sockfd) {
    struct timeval timeout = {
        .tv_sec = CONNECTION_TIMEOUT_SECONDS,
        .tv_usec = 0,
    };

    if (setsockopt(sockfd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof timeout) < 0) {
        perror("server: setsockopt SO_RCVTIMEO");
        return false;
    }
    if (setsockopt(sockfd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof timeout) < 0) {
        perror("server: setsockopt SO_SNDTIMEO");
        return false;
    }

    return true;
}

int init_socket(const char *address, const char *port, int address_family) {
    struct addrinfo hints;
    memset(&hints, 0, sizeof hints);
    hints.ai_family = address_family;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_PASSIVE;

    struct addrinfo *addresses;
    int status = getaddrinfo(address, port, &hints, &addresses);
    if (status != 0) {
        fprintf(stderr, "server: getaddrinfo %s:%s: %s\n", address == NULL ? "*" : address, port,
                gai_strerror(status));
        errno = EINVAL;
        return -1;
    }

    int sockfd = -1;
    int last_error = EADDRNOTAVAIL;
    int yes = 1;
    for (struct addrinfo *candidate = addresses; candidate != NULL;
         candidate = candidate->ai_next) {
        sockfd = socket(candidate->ai_family, candidate->ai_socktype, candidate->ai_protocol);
        if (sockfd < 0) {
            last_error = errno;
            continue;
        }

        if (setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof yes) < 0) {
            last_error = errno;
        } else if (bind(sockfd, candidate->ai_addr, candidate->ai_addrlen) < 0) {
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

    if (listen(sockfd, BACKLOG) < 0) {
        int error = errno;
        close(sockfd);
        errno = error;
        return -1;
    }

    return sockfd;
}

/*
 *  Server implementation
 *
 *
 *
 */

void accept_requests(int sockfd) {
    char *connection_buffer = malloc(CONNECTION_BUFFER_SIZE);
    size_t used = 0;
    if (connection_buffer == NULL) {
        perror("server: connection buffer allocation");
        return;
    }

    for (;;) {
        struct http_request request = {0};
        struct http_response response = {
            .version = HTTP_VERSION_1_1,
            .keep_alive = false,
        };
        const char *error_status = NULL;

        const char *request_line_end;

        // Receive and parse the request line.
        while ((request_line_end = find_crlf(connection_buffer, connection_buffer + used)) ==
               NULL) {
            if (used >= CONNECTION_BUFFER_SIZE) {
                goto close_connection;
            }

            if (!receive_request_bytes(sockfd, connection_buffer, &used)) {
                goto close_connection;
            }
        }

        enum request_parse_result result =
            parse_http_request_line(&request, connection_buffer, request_line_end);
        if (result != REQUEST_PARSE_OK) {
            goto close_connection;
        }

        if (request.version == HTTP_UNSUPPORTED_VERSION) {
            error_status = "505 HTTP Version Not Supported";
            goto send_error_response;
        }

        response.version = request.version;

        // Receive the complete request header section.
        const char *headers_end;
        while ((headers_end = find_header_end(request_line_end, connection_buffer + used)) ==
               NULL) {
            if (used >= CONNECTION_BUFFER_SIZE) {
                error_status = "431 Request Header Fields Too Large";
                goto send_error_response;
            }

            if (!receive_request_bytes(sockfd, connection_buffer, &used)) {
                if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    error_status = "408 Request Timeout";
                    goto send_error_response;
                }
                if (errno == 0) {
                    error_status = "400 Bad Request";
                    goto send_error_response;
                }
                goto close_connection;
            }
        }
        const char *headers_begin = request_line_end + 2;
        const char *header_fields_end = headers_end - 2;

        // The buffer may also contain body bytes or part of the next request.
        size_t header_size = (size_t)(headers_end - connection_buffer);

        enum request_parse_result parse_result =
            parse_http_request_headers(&request, headers_begin, header_fields_end);
        if (parse_result == REQUEST_PARSE_OUT_OF_MEMORY) {
            perror("server: request header allocation");
            error_status = "500 Internal Server Error";
            goto send_error_response;
        }
        if (parse_result == REQUEST_PARSE_TOO_MANY_HEADERS) {
            error_status = "431 Request Header Fields Too Large";
            goto send_error_response;
        }
        if (parse_result != REQUEST_PARSE_OK) {
            fprintf(stderr, "server: malformed request headers\n");
            error_status = "400 Bad Request";
            goto send_error_response;
        }

        bool content_length_present = false;
        size_t content_length = 0;
        bool transfer_encoding_present = false;
        bool expectation_present = false;
        bool malformed_headers = false;
        bool connection_close = false;
        bool connection_keep_alive = false;

        for (size_t index = 0; index < request.header_count; ++index) {
            struct http_header header = request.headers[index];

            if (http_text_equals_ignore_case(header.name, "Transfer-Encoding")) {
                transfer_encoding_present = true;
            } else if (http_text_equals_ignore_case(header.name, "Expect")) {
                expectation_present = true;
            } else if (http_text_equals_ignore_case(header.name, "Content-Length")) {
                size_t parsed_length;
                if (content_length_present ||
                    !parse_content_length(header.value.data,
                                          header.value.data + header.value.length,
                                          &parsed_length)) {
                    malformed_headers = true;
                    break;
                }
                content_length_present = true;
                content_length = parsed_length;
            } else if (http_text_equals_ignore_case(header.name, "Connection")) {
                const char *position = header.value.data;
                const char *value_end = header.value.data + header.value.length;

                while (position < value_end) {
                    while (position < value_end && (*position == ' ' || *position == '\t')) {
                        ++position;
                    }

                    /* Recipients ignore empty members in comma-separated field values. */
                    if (position < value_end && *position == ',') {
                        ++position;
                        continue;
                    }
                    if (position == value_end) {
                        break;
                    }

                    const char *token_begin = position;
                    while (position < value_end) {
                        unsigned char character = (unsigned char)*position;
                        if (!((character >= '0' && character <= '9') ||
                              (character >= 'A' && character <= 'Z') ||
                              (character >= 'a' && character <= 'z') ||
                              strchr("!#$%&'*+-.^_`|~", character) != NULL)) {
                            break;
                        }
                        ++position;
                    }
                    const char *token_end = position;

                    while (position < value_end && (*position == ' ' || *position == '\t')) {
                        ++position;
                    }
                    if (token_begin == token_end || (position < value_end && *position != ',')) {
                        malformed_headers = true;
                        break;
                    }

                    size_t token_length = (size_t)(token_end - token_begin);
                    if (token_length == sizeof "close" - 1 &&
                        strncasecmp(token_begin, "close", token_length) == 0) {
                        connection_close = true;
                    } else if (token_length == sizeof "keep-alive" - 1 &&
                               strncasecmp(token_begin, "keep-alive", token_length) == 0) {
                        connection_keep_alive = true;
                    }

                    if (position < value_end) {
                        ++position;
                    }
                }

                if (malformed_headers) {
                    break;
                }
            }
        }

        // evaluate relevant request headers
        if (malformed_headers) {
            fprintf(stderr, "server: malformed request headers\n");
            error_status = "400 Bad Request";
            goto send_error_response;
        }

        // HTTP/1.0 closes connection by default, later versin leave it open by default
        response.keep_alive = request.version != HTTP_VERSION_1_0;
        if (request.version == HTTP_VERSION_1_0 && connection_keep_alive) {
            response.keep_alive = true;
        }
        if (connection_close) {
            response.keep_alive = false;
        }

        log_request(&request, content_length_present, content_length);

        if (transfer_encoding_present) {
            fprintf(stderr, "server: transfer encodings are not supported\n");
            error_status = "501 Not Implemented";
            goto send_error_response;
        }
        if (expectation_present) {
            fprintf(stderr, "server: expectations are not supported\n");
            error_status = "417 Expectation Failed";
            goto send_error_response;
        }

        size_t body_size = content_length_present ? content_length : 0;

        if (body_size > MAX_REQUEST_SIZE - header_size) {
            fprintf(stderr, "server: request is too large\n");
            error_status = "413 Content Too Large";
            goto send_error_response;
        }
        size_t request_size = header_size + body_size;

        char *request_buffer = malloc(request_size);
        if (request_buffer == NULL) {
            perror("server: request buffer allocation");
            error_status = "500 Internal Server Error";
            goto send_error_response;
        }
        request.storage = request_buffer;
        request.storage_size = request_size;

        if (request_size <= CONNECTION_BUFFER_SIZE) {
            /* The complete request fits in the connection buffer. Receiving
             * into all remaining space may also fetch part of a pipelined
             * request, which is preserved by the memmove below. */
            while (used < request_size) {
                ssize_t received =
                    recv(sockfd, connection_buffer + used, CONNECTION_BUFFER_SIZE - used, 0);
                if (received < 0) {
                    if (errno == EINTR) {
                        continue;
                    }
                    int error = errno;
                    perror("server: receive");
                    if (error == EAGAIN || error == EWOULDBLOCK) {
                        error_status = "408 Request Timeout";
                        goto send_error_response;
                    }
                    goto close_connection;
                }
                if (received == 0) {
                    error_status = "400 Bad Request";
                    goto send_error_response;
                }

                used += (size_t)received;
            }

            memcpy(request_buffer, connection_buffer, request_size);

            size_t remaining = used - request_size;
            memmove(connection_buffer, connection_buffer + request_size, remaining);
            used = remaining;
        } else {
            /* The request cannot fit in the connection buffer. Move the
             * bytes already received fromm connection to request buffer and receive the
             * rest of the payload directly into request buffer. */
            size_t request_used = used;
            memcpy(request_buffer, connection_buffer, request_used);

            used = 0;

            while (request_used < request_size) {
                ssize_t received =
                    recv(sockfd, request_buffer + request_used, request_size - request_used, 0);
                if (received < 0) {
                    if (errno == EINTR) {
                        continue;
                    }
                    int error = errno;
                    perror("server: receive");
                    if (error == EAGAIN || error == EWOULDBLOCK) {
                        error_status = "408 Request Timeout";
                        goto send_error_response;
                    }
                    goto close_connection;
                }
                if (received == 0) {
                    error_status = "400 Bad Request";
                    goto send_error_response;
                }

                request_used += (size_t)received;
            }
        }

        relocate_http_request(&request, connection_buffer, request_buffer, request_size,
                              header_size, body_size);

        handle_request(&request, &response);

        bool response_sent = send_http_response(sockfd, &response);
        bool keep_alive = response.keep_alive;

        destroy_http_request(&request);
        destroy_http_response(&response);

        if (!response_sent || !keep_alive) {
            break;
        }

        continue;

    send_error_response:
        http_response_set_empty(&response, error_status);
        response.keep_alive = false;
        (void)send_http_response(sockfd, &response);

    close_connection:
        destroy_http_request(&request);
        destroy_http_response(&response);
        break;
    }

    free(connection_buffer);
}

void *connection_thread(void *arg) {
    int connection_fd = *(int *)arg;
    free(arg);

    accept_requests(connection_fd);

    if (close(connection_fd) == -1) {
        perror("server: close");
    }

    return NULL;
}

void accept_connections(int sockfd) {
    pthread_attr_t thread_attributes;
    int error = pthread_attr_init(&thread_attributes);
    if (error != 0) {
        fprintf(stderr, "server: pthread_attr_init: %s\n", strerror(error));
        exit(EXIT_FAILURE);
    }

    error = pthread_attr_setdetachstate(&thread_attributes, PTHREAD_CREATE_DETACHED);
    if (error != 0) {
        fprintf(stderr, "server: pthread_attr_setdetachstate: %s\n", strerror(error));
        pthread_attr_destroy(&thread_attributes);
        exit(EXIT_FAILURE);
    }

    for (;;) {
        struct sockaddr_storage their_addr;
        socklen_t addr_size = sizeof their_addr;

        int new_fd = accept(sockfd, (struct sockaddr *)&their_addr, &addr_size);

        if (new_fd == -1) {
            perror("server: accept");
            continue;
        }

        if (!configure_connection_timeouts(new_fd)) {
            close(new_fd);
            continue;
        }

        int *thread_fd = malloc(sizeof *thread_fd);
        if (thread_fd == NULL) {
            perror("server: malloc");
            close(new_fd);
            continue;
        }
        *thread_fd = new_fd;

        pthread_t thread;
        error = pthread_create(&thread, &thread_attributes, connection_thread, thread_fd);

        if (error != 0) {
            fprintf(stderr, "server: pthread_create: %s\n", strerror(error));
            free(thread_fd);
            close(new_fd);
            continue;
        }
    }
}

int main(int argc, char **argv) {
    struct sigaction signal_action;
    memset(&signal_action, 0, sizeof signal_action);
    signal_action.sa_handler = SIG_IGN;
    sigemptyset(&signal_action.sa_mask);
    if (sigaction(SIGPIPE, &signal_action, NULL) < 0) {
        perror("server: sigaction");
        return EXIT_FAILURE;
    }

    const char *address = NULL;
    int port = 3490;
    int ch;

    while ((ch = getopt(argc, argv, "a:P:d:")) != -1) {
        switch (ch) {
        case 'a':
            address = optarg;
            break;
        case 'P': {
            char *end;
            long parsed_port;

            errno = 0;
            parsed_port = strtol(optarg, &end, 10);
            if (errno != 0 || *optarg == '\0' || *end != '\0' || parsed_port < 1 ||
                parsed_port > 65535) {
                fprintf(stderr, "Invalid port number: %s\n", optarg);
                return EXIT_FAILURE;
            }
            port = (int)parsed_port;
            break;
        }
        case 'd':
            if (*optarg == '\0') {
                fprintf(stderr, "Document root must not be empty\n");
                return EXIT_FAILURE;
            }
            document_root = optarg;
            break;
        case '?':
        default:
            fprintf(stderr, "Usage: %s [-a address] [-P port] [-d document_root]\n", argv[0]);
            return EXIT_FAILURE;
        }
    }

    if (optind != argc) {
        fprintf(stderr, "Usage: %s [-a address] [-P port] [-d document_root]\n", argv[0]);
        return EXIT_FAILURE;
    }

    int document_root_fd = open(document_root, O_RDONLY | O_DIRECTORY);
    if (document_root_fd < 0) {
        perror("server: document root");
        return EXIT_FAILURE;
    }
    if (close(document_root_fd) < 0) {
        perror("server: close document root");
        return EXIT_FAILURE;
    }

    char port_string[6];
    snprintf(port_string, sizeof port_string, "%d", port);

    int sock = init_socket(address, port_string, AF_UNSPEC);
    if (sock < 0) {
        perror("server: initialize listening socket");
        return EXIT_FAILURE;
    }
    print_listening_address(sock);

    accept_connections(sock);

    return EXIT_SUCCESS;
}
