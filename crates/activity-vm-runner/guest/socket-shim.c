#define _GNU_SOURCE

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/tcp.h>
#include <stdint.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/syscall.h>
#include <sys/un.h>
#include <unistd.h>

#define TRACKED_FDS 65536
static atomic_uint_least16_t listener_ports[TRACKED_FDS];
static atomic_uchar accepted_sockets[TRACKED_FDS];

static int kernel_connect(int fd, const struct sockaddr *address,
                          socklen_t length) {
  return (int)syscall(SYS_connect, fd, address, length);
}

static int kernel_bind(int fd, const struct sockaddr *address,
                       socklen_t length) {
  return (int)syscall(SYS_bind, fd, address, length);
}

static int kernel_getsockname(int fd, struct sockaddr *address,
                              socklen_t *length) {
  return (int)syscall(SYS_getsockname, fd, address, length);
}

static int kernel_getpeername(int fd, struct sockaddr *address,
                              socklen_t *length) {
  return (int)syscall(SYS_getpeername, fd, address, length);
}

static int visible_loopback(struct sockaddr *address, socklen_t *length,
                            uint16_t port) {
  if (address == NULL || length == NULL) {
    return 0;
  }
  struct sockaddr_in visible;
  memset(&visible, 0, sizeof(visible));
  visible.sin_family = AF_INET;
  visible.sin_port = htons(port);
  visible.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  socklen_t copied = *length < sizeof(visible) ? *length : sizeof(visible);
  memcpy(address, &visible, copied);
  *length = sizeof(visible);
  return 0;
}

static void listener_path(char *path, size_t capacity, uint16_t port) {
  const char prefix[] = "/tmp/obelisk-activity-vm-listen-";
  size_t cursor = sizeof(prefix) - 1;
  memcpy(path, prefix, cursor);
  char digits[5];
  size_t count = 0;
  do {
    digits[count++] = (char)('0' + port % 10);
    port /= 10;
  } while (port != 0);
  while (count != 0 && cursor + 1 < capacity) {
    path[cursor++] = digits[--count];
  }
  const char suffix[] = ".sock";
  memcpy(path + cursor, suffix, sizeof(suffix));
}

static int replace_with_unix_socket(int fd) {
  int status_flags = fcntl(fd, F_GETFL);
  int descriptor_flags = fcntl(fd, F_GETFD);
  int replacement = socket(AF_UNIX, SOCK_STREAM, 0);
  if (replacement < 0 || dup2(replacement, fd) < 0) {
    int saved_errno = errno;
    if (replacement >= 0) {
      close(replacement);
    }
    errno = saved_errno;
    return -1;
  }
  close(replacement);
  if (status_flags >= 0) {
    fcntl(fd, F_SETFL, status_flags);
  }
  if (descriptor_flags >= 0) {
    fcntl(fd, F_SETFD, descriptor_flags);
  }
  return 0;
}

static int unix_address(struct sockaddr_un *address, const char *path) {
  size_t length = strlen(path);
  if (length >= sizeof(address->sun_path)) {
    errno = ENAMETOOLONG;
    return -1;
  }
  memset(address, 0, sizeof(*address));
  address->sun_family = AF_UNIX;
  memcpy(address->sun_path, path, length + 1);
  return 0;
}

static int is_bridge_socket(int fd) {
  if (fd >= 0 && fd < TRACKED_FDS && atomic_load(&accepted_sockets[fd])) {
    return 1;
  }
  struct sockaddr_un address;
  socklen_t length = sizeof(address);
  memset(&address, 0, sizeof(address));
  if (kernel_getpeername(fd, (struct sockaddr *)&address, &length) != 0 ||
      address.sun_family != AF_UNIX) {
    return 0;
  }
  return strcmp(address.sun_path, "/tmp/obelisk-activity-vm-http.sock") == 0 ||
         strcmp(address.sun_path, "/tmp/obelisk-activity-vm-https.sock") == 0;
}

static uint16_t destination_port(const struct sockaddr *address,
                                 socklen_t length) {
  if (address->sa_family == AF_INET &&
      length >= sizeof(struct sockaddr_in)) {
    return ntohs(((const struct sockaddr_in *)address)->sin_port);
  }
  if (address->sa_family == AF_INET6 &&
      length >= sizeof(struct sockaddr_in6)) {
    return ntohs(((const struct sockaddr_in6 *)address)->sin6_port);
  }
  return 0;
}

static int numeric_port(const char *text, uint16_t *port) {
  unsigned int value = 0;
  if (text == NULL || *text == '\0') {
    return 0;
  }
  for (const char *cursor = text; *cursor != '\0'; cursor++) {
    if (*cursor < '0' || *cursor > '9') {
      return 0;
    }
    value = value * 10 + (unsigned int)(*cursor - '0');
    if (value > UINT16_MAX) {
      return 0;
    }
  }
  *port = (uint16_t)value;
  return 1;
}

int connect(int fd, const struct sockaddr *address, socklen_t length) {
  uint16_t port = destination_port(address, length);
  char listener[sizeof(((struct sockaddr_un *)0)->sun_path)];
  listener_path(listener, sizeof(listener), port);
  const char *path = port == 80   ? "/tmp/obelisk-activity-vm-http.sock"
                     : port == 443 ? "/tmp/obelisk-activity-vm-https.sock"
                     : access(listener, F_OK) == 0 ? listener
                                                   : NULL;
  if (path == NULL) {
    return kernel_connect(fd, address, length);
  }

  if (replace_with_unix_socket(fd) < 0) {
    return -1;
  }

  struct sockaddr_un local;
  if (unix_address(&local, path) < 0) {
    return -1;
  }
  return kernel_connect(fd, (const struct sockaddr *)&local, sizeof(local));
}

int bind(int fd, const struct sockaddr *address, socklen_t length) {
  uint16_t port = destination_port(address, length);
  if (port == 0 || (address->sa_family != AF_INET &&
                    address->sa_family != AF_INET6)) {
    return kernel_bind(fd, address, length);
  }
  char path[sizeof(((struct sockaddr_un *)0)->sun_path)];
  listener_path(path, sizeof(path), port);
  if (replace_with_unix_socket(fd) < 0) {
    return -1;
  }
  unlink(path);
  struct sockaddr_un local;
  if (unix_address(&local, path) < 0) {
    return -1;
  }
  int result = kernel_bind(fd, (const struct sockaddr *)&local, sizeof(local));
  if (result == 0 && fd >= 0 && fd < TRACKED_FDS) {
    atomic_store(&listener_ports[fd], port);
  }
  return result;
}

int getsockname(int fd, struct sockaddr *address, socklen_t *length) {
  uint16_t port = fd >= 0 && fd < TRACKED_FDS
                      ? (uint16_t)atomic_load(&listener_ports[fd])
                      : 0;
  if (port == 0) {
    return kernel_getsockname(fd, address, length);
  }
  return visible_loopback(address, length, port);
}

int getpeername(int fd, struct sockaddr *address, socklen_t *length) {
  if (fd >= 0 && fd < TRACKED_FDS && atomic_load(&accepted_sockets[fd])) {
    return visible_loopback(address, length, 1);
  }
  return kernel_getpeername(fd, address, length);
}

static int translated_accept(int fd, struct sockaddr *address,
                             socklen_t *length, int flags) {
  uint16_t port = fd >= 0 && fd < TRACKED_FDS
                      ? (uint16_t)atomic_load(&listener_ports[fd])
                      : 0;
  if (port == 0) {
    return (int)syscall(SYS_accept4, fd, address, length, flags);
  }
  struct sockaddr_un actual;
  socklen_t actual_length = sizeof(actual);
  int accepted = (int)syscall(SYS_accept4, fd, &actual, &actual_length, flags);
  if (accepted < 0) {
    return -1;
  }
  if (accepted < TRACKED_FDS) {
    atomic_store(&listener_ports[accepted], port);
    atomic_store(&accepted_sockets[accepted], 1);
  }
  visible_loopback(address, length, 1);
  return accepted;
}

int accept(int fd, struct sockaddr *address, socklen_t *length) {
  return translated_accept(fd, address, length, 0);
}

int accept4(int fd, struct sockaddr *address, socklen_t *length, int flags) {
  return translated_accept(fd, address, length, flags);
}

int close(int fd) {
  if (fd >= 0 && fd < TRACKED_FDS) {
    atomic_store(&listener_ports[fd], 0);
    atomic_store(&accepted_sockets[fd], 0);
  }
  return (int)syscall(SYS_close, fd);
}

int setsockopt(int fd, int level, int option, const void *value,
               socklen_t length) {
  if (level == IPPROTO_TCP && is_bridge_socket(fd)) {
    switch (option) {
    case TCP_NODELAY:
    case TCP_KEEPIDLE:
    case TCP_KEEPINTVL:
    case TCP_KEEPCNT:
      return 0;
    default:
      break;
    }
  }
  return (int)syscall(SYS_setsockopt, fd, level, option, value, length);
}

int getaddrinfo(const char *node, const char *service,
                const struct addrinfo *hints, struct addrinfo **result) {
  if (node == NULL) {
    return EAI_NONAME;
  }

  uint16_t port = 0;
  if (service != NULL) {
    if (numeric_port(service, &port)) {
      /* Already parsed. */
    } else if (strcmp(service, "http") == 0) {
      port = 80;
    } else if (strcmp(service, "https") == 0) {
      port = 443;
    } else {
      return EAI_SERVICE;
    }
  }

  struct addrinfo *answer = calloc(1, sizeof(*answer));
  struct sockaddr_in *address = calloc(1, sizeof(*address));
  if (answer == NULL || address == NULL) {
    free(answer);
    free(address);
    return EAI_MEMORY;
  }
  address->sin_family = AF_INET;
  address->sin_port = htons(port);
  address->sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  answer->ai_family = AF_INET;
  answer->ai_socktype = hints != NULL && hints->ai_socktype != 0
                            ? hints->ai_socktype
                            : SOCK_STREAM;
  answer->ai_protocol = hints != NULL ? hints->ai_protocol : 0;
  answer->ai_addrlen = sizeof(*address);
  answer->ai_addr = (struct sockaddr *)address;
  if (hints != NULL && (hints->ai_flags & AI_CANONNAME) != 0) {
    answer->ai_canonname = strdup(node);
    if (answer->ai_canonname == NULL) {
      free(address);
      free(answer);
      return EAI_MEMORY;
    }
  }
  *result = answer;
  return 0;
}

void freeaddrinfo(struct addrinfo *result) {
  while (result != NULL) {
    struct addrinfo *next = result->ai_next;
    free(result->ai_canonname);
    free(result->ai_addr);
    free(result);
    result = next;
  }
}

