# HTTP Server

This document section describes the webserver implemented in webserver.c
It is a small thread-per-connection HTTP file server.

## building and running

Build the server with:

```sh
make webserver
```

Run it with:

```text
./webserver2 [-a address] [-P port] [-d document_root]
```

| Option             | Meaning                                          | Default                                                    |
| ------------------ | ------------------------------------------------ | ---------------------------------------------------------- |
| `-a address`       | Hostname or IPv4/IPv6 address on which to listen | A wildcard address selected from the platform's candidates |
| `-P port`          | TCP port in the range 1–65535                    | `3490`                                                     |
| `-d document_root` | Existing directory containing the served files   | `./content` relative to the server's working directory     |

The address may be numeric, such as `127.0.0.1` or `::1`, or a hostname such as
`localhost`. On startup, the server prints the numeric address and port to standard
output:

```text
server: listening on [::]:3490
```

Example invocations:

```sh
./webserver2
./webserver2 -a 127.0.0.1 -P 8080
./webserver2 -a ::1 -P 8080 -d ./content
./webserver2 -a localhost -P 8080
```

## Implemented HTTP features

The server implements the following features:

- The server supports HTTP/1.0 and HTTP/1.1 requests
- Currently the `GET`, `POST`, `PUT` methods.
- Absolute as well as relative URIs are supported. Query parameters and fragements are not parsed
- Supports `Content-Length` header for requests with data.
- HTTP/1.0 close the connection by default unless `Connection: keep-alive` is present
- HTTP/1.1 leaves the connection open unless `Connection: close` is present
- Support for HTTP/1.1 pipelining (The Operating system network buffer
  could
  potentially run full when many large requests are pipelines, because the HTTP server only continues resuming bytes once a response has been send)
- Support for up to 127 Request Header. The request status lines, header as well as \r\n bytes cannot exceed CONNECTION_BUFFER_SIZE
- Empty, Memory-backed and file backed responses
- File Extension based `Content-Type` header for `GET` endpoint
- Possibility to set custom response headers in request handler

The following request headers currently affect behavior:

| Header              | Behavior                                                                                                                                                |
| ------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `Content-Length`    | Determines how many request-body bytes are read. The value must be a nonempty decimal integer that fits in `size_t`. Multiple occurrences are rejected. |
| `Connection`        | Controls persistence. Comma-separated tokens and repeated fields are parsed. `close` takes precedence over `keep-alive`.                                |
| `Transfer-Encoding` | Any occurrence is rejected because transfer codings, including chunked bodies, are not implemented.                                                     |
| `Expect`            | Any occurrence is rejected because `100-continue` and other expectations are not implemented.                                                           |

Other headers are syntactically parsed and retained in the request, but currently not used.
The server does neither enforce the presence of the Host header in HTTP/1.1 requests nor does it validate its value if it is present

## Message handling and limits

When a client sends a requests to the server the server consumes bytes until its connection_buffer is full
or the an Complete HTTP status line is consumed (indicated by CRLF). The server than parses the status line.
If the status lines is invalid the connection is closed immediately.

After the request line has been consumed we try to consume until we have collected all headers.
Indicated by CRLFCRLF.
Once the headers are consumed they are parsed.
We than Validate several Headers like `Content-Length`.
If everything is in order the we consume the payload and move everything in the Request buffer.

From there the request is handled off to the handler.

If a request is smaller than the connection buffer, the server may receive bytes
from a subsequent pipelined request at the same time. After copying the current
request into its own request buffer it uses `memmove()` to preserve those additional
bytes at the beginning of the connection buffer. For larger bodies, the bytes
already received are copied into the request allocation and the remainder is read
directly into the request buffer..

The limits are:

| Limit                                                                   |          Value |
| ----------------------------------------------------------------------- | -------------: |
| Connection/header buffer                                                |          8 KiB |
| Parsed headers                                                          |            127 |
| Complete request, including request line, headers, CRLF bytes, and body |          1 MiB |
| Receive and send timeout                                                |     30 seconds |
| Listen backlog                                                          | 10 connections |

If a request is smaller than the connection buffer, the server may receive bytes
from a subsequent pipelined request at the same time. After copying the current
request into its own allocation, it uses `memmove()` to preserve those additional
bytes at the beginning of the connection buffer. For larger bodies, the bytes
already received are copied into the request allocation and the remainder is read
directly into that allocation.

## Resources and methods

There is no separate routing table (allthough it will be easy to add). Every valid URL path represents a file below
the configured document root.

| Method                | Operation                                                                            | Successful response                                              |
| --------------------- | ------------------------------------------------------------------------------------ | ---------------------------------------------------------------- |
| `GET /path/file.txt`  | Opens and streams an existing regular file                                           | `200 OK` with the file bytes                                     |
| `POST /path/file.txt` | Creates a new file from the request body; refuses to replace an existing entry       | `201 Created`                                                    |
| `PUT /path/file.txt`  | Creates a file or atomically replaces an existing regular file with the request body | `201 Created` for a new file, `204 No Content` for a replacement |

All `POST` and `PUT` responses have empty bodies. The server does not inspect the
request's `Content-Type`; the request body is stored as raw bytes. Successful `GET`
responses infer their `Content-Type` from the case-insensitive filename extension:

| Extension                           | Response content type            |
| ----------------------------------- | -------------------------------- |
| `.html`, `.htm`                     | `text/html; charset=utf-8`       |
| `.css`                              | `text/css; charset=utf-8`        |
| `.js`                               | `text/javascript; charset=utf-8` |
| `.json`                             | `application/json`               |
| `.txt`                              | `text/plain; charset=utf-8`      |
| `.svg`                              | `image/svg+xml`                  |
| `.png`                              | `image/png`                      |
| `.jpg`, `.jpeg`                     | `image/jpeg`                     |
| `.gif`                              | `image/gif`                      |
| `.pdf`                              | `application/pdf`                |
| Any other extension or no extension | `application/octet-stream`       |

Example requests:

```sh
# Read an existing file
curl --http1.1 -i http://127.0.0.1:3490/rick.txt

# Create a file, failing if it already exists
curl --http1.1 -i -X POST --data-binary @local.txt \
  http://127.0.0.1:3490/new.txt

# Create or replace a file
curl --http1.1 -i -X PUT --data-binary @local.txt \
  http://127.0.0.1:3490/new.txt
```

All directories in the URIs paths must already exist. The server only creates files, not directories.

## PUT atomicity

`PUT` writes the complete body to a temporary file in the destination directory.
Temporary names start with `.webserver-put-` and include the process ID and a
mutex-protected counter. The file is created with `O_CREAT | O_EXCL`, so an existing
temporary file is never overwritten.

After a Successful write and close of the temporary file, we use `renameat()` to 'move' the file
to its correct location. While existing file pointers remain valid so that existing readers will
see the old content.

## Path handling and filesystem safety

- A nonempty path must start with `/`.
- Directory components may contain only ASCII letters `A-Z` and `a-z`.
- The final filename may contain ASCII letters and periods.
- The final filename must contain at least one letter.
- Empty components, consecutive slashes, and trailing slashes are rejected.
- `.` and `..` are rejected, as are digits, spaces, backslashes, hyphens, and
  percent-encoded alternatives.

| Path                          | Result                                         |
| ----------------------------- | ---------------------------------------------- |
| `/test.txt`                   | Valid                                          |
| `/Documents/Letters/test.txt` | Valid                                          |
| `/.hidden`                    | Valid                                          |
| `/Documents//test.txt`        | Invalid                                        |
| `/Documents.old/test.txt`     | Invalid because a directory contains a period  |
| `/test-one.txt`               | Invalid because the filename contains a hyphen |
| `/test1.txt`                  | Invalid because the filename contains a digit  |
| `/../test.txt`                | Invalid                                        |

we use openat() to manually follow the path to make sure that:

1. The path exists
2. We are not following any symlinks

## General Status code reference

The server can produce the following statuses:

| Status                                | Conditions                                                                                                                                                                                                                    |
| ------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `200 OK`                              | A GET target is an existing regular file and can be opened.                                                                                                                                                                   |
| `201 Created`                         | POST creates its target, or PUT creates a previously absent target.                                                                                                                                                           |
| `204 No Content`                      | PUT atomically replaces an existing regular file.                                                                                                                                                                             |
| `400 Bad Request`                     | After a valid request line is parsed, the header section, `Content-Length`, or `Connection` value is malformed; the peer closes before completing the headers or body; or a method handler receives an invalid document path. |
| `403 Forbidden`                       | A file operation fails with `EACCES`, `EPERM`, or `ELOOP`, including attempts to follow forbidden symbolic links.                                                                                                             |
| `404 Not Found`                       | A file or parent directory is missing, a path component is not a directory, the path identifies a directory, or GET identifies a nonregular file.                                                                             |
| `405 Method Not Allowed`              | The method is anything other than GET, POST, or PUT. The response includes `Allow`.                                                                                                                                           |
| `408 Request Timeout`                 | Receiving the header section or request body reaches the configured socket receive timeout.                                                                                                                                   |
| `409 Conflict`                        | POST's target already exists, or PUT's existing target is not a regular file.                                                                                                                                                 |
| `413 Content Too Large`               | The complete request would exceed 1 MiB.                                                                                                                                                                                      |
| `417 Expectation Failed`              | Any `Expect` header is present.                                                                                                                                                                                               |
| `431 Request Header Fields Too Large` | After a valid request line is found, the complete request line and header section do not fit in 8 KiB, or the request contains more than 127 headers.                                                                         |
| `500 Internal Server Error`           | Request allocation fails, response-header allocation fails, a file write or close fails, temporary-file creation is exhausted, or an unexpected filesystem error occurs.                                                      |
| `501 Not Implemented`                 | Any `Transfer-Encoding` header is present.                                                                                                                                                                                    |
| `505 HTTP Version Not Supported`      | The request line contains a syntactically valid version other than `HTTP/1.0` or `HTTP/1.1`. The response uses `HTTP/1.1`.                                                                                                    |

An invalid or oversized request line causes the connection to close without an HTTP
response. At that point the server cannot reliably determine which HTTP version to
place in a response. A receive error or memory-allocation failure before a request
line has been parsed is handled the same way.


# Client

The client demonstrates the server's capabilities. In regards to
- parallel GETs
- Request pipelining
- Atomic PUTs

## 1. Parallel GETs
Four client threads wait on a shared start gate and then request `/demo.html` at
approximately the same time. Each thread uses its own connection, requires a
`200 OK` response, prints the received HTML, and stores it as
`client-output/fetch-thread-N.html`. The scenario succeeds only if all four
threads receive and save the file successfully.

This demonstrates that the server can accept and process requests from several
clients concurrently.

## 2. HTTP pipelining
The client opens one connection and sends the following requests
back-to-back before reading any responses:

1. `GET /demo.html`
2. `PUT /demo.html` with an updated HTML document
3. `GET /demo.html`

It then reads the three responses in order. The expected status codes are
`200 OK`, `204 No Content`, and `200 OK`. The first GET body must contain the
initial document, while the second GET body must contain the updated document.
The two versions are printed and stored as `client-output/pipeline-before.html`
and `client-output/pipeline-after.html`.

This demonstrates that the server preserves request and response ordering when
several requests are pipelined over one connection.

## 3. Atomic Puts

The client initializes `/stress.txt` and starts two writer threads and two
reader threads together. 
One writer repeatedly replaces the file with `1111111111`, 
while the other repeatedly replaces it with `2222222222`. 
Each writer performs five PUT requests.
While the writers are active, both readers continuously request the file.

Every GET response must contain exactly ten `1` characters or exactly ten `2`
characters. A partial, mixed, empty, or otherwise unexpected body fails the
scenario. This checks that concurrent clients are handled correctly and that a
reader observes either the old complete file or the new complete file while PUT
uses atomic replacement.
(Allthough it is still possible that this succeeds by pure luck but oh well)
