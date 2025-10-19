#include <arpa/inet.h>
#include <fcntl.h>
#include <netdb.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

#if defined(__linux__)
#include <endian.h>
#elif defined(__APPLE__)
#include <libkern/OSByteOrder.h>
#define htobe64(x) OSSwapHostToBigInt64(x)
#define be64toh(x) OSSwapBigToHostInt64(x)
#endif

namespace {

constexpr std::size_t kBufferSize = 4 * 1024 * 1024;

struct Header {
    std::uint32_t path_length;
    std::uint64_t offset;
    std::uint64_t length;
};

std::uint64_t host_to_network64(std::uint64_t value) { return htobe64(value); }

std::uint64_t network_to_host64(std::uint64_t value) { return be64toh(value); }

class Error : public std::runtime_error {
   public:
    explicit Error(const std::string &msg) : std::runtime_error(msg) {}
};

int open_file_for_read(const std::filesystem::path &path) {
    int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        std::ostringstream oss;
        oss << "failed to open source file '" << path.string() << "': " << std::strerror(errno);
        throw Error(oss.str());
    }
    return fd;
}

int open_file_for_write(const std::filesystem::path &path) {
    std::filesystem::create_directories(path.parent_path());
    int fd = ::open(path.c_str(), O_CREAT | O_WRONLY, 0644);
    if (fd < 0) {
        std::ostringstream oss;
        oss << "failed to open destination file '" << path.string() << "': "
            << std::strerror(errno);
        throw Error(oss.str());
    }
    return fd;
}

void send_all(int sockfd, const void *buffer, std::size_t length) {
    const char *data = static_cast<const char *>(buffer);
    std::size_t sent = 0;
    while (sent < length) {
        ssize_t rc = ::send(sockfd, data + sent, length - sent, 0);
        if (rc < 0) {
            if (errno == EINTR) {
                continue;
            }
            std::ostringstream oss;
            oss << "socket send failed: " << std::strerror(errno);
            throw Error(oss.str());
        }
        sent += static_cast<std::size_t>(rc);
    }
}

void recv_all(int sockfd, void *buffer, std::size_t length) {
    char *data = static_cast<char *>(buffer);
    std::size_t received = 0;
    while (received < length) {
        ssize_t rc = ::recv(sockfd, data + received, length - received, 0);
        if (rc == 0) {
            throw Error("unexpected EOF on socket");
        }
        if (rc < 0) {
            if (errno == EINTR) {
                continue;
            }
            std::ostringstream oss;
            oss << "socket recv failed: " << std::strerror(errno);
            throw Error(oss.str());
        }
        received += static_cast<std::size_t>(rc);
    }
}

class AddrInfo {
   public:
    AddrInfo(const std::string &host, const std::string &port, int family, int socktype) {
        struct addrinfo hints;
        std::memset(&hints, 0, sizeof(hints));
        hints.ai_family = family;
        hints.ai_socktype = socktype;
        hints.ai_flags = (host.empty() ? AI_PASSIVE : 0);

        int rc = ::getaddrinfo(host.empty() ? nullptr : host.c_str(), port.c_str(), &hints, &info_);
        if (rc != 0) {
            std::ostringstream oss;
            oss << "getaddrinfo failed: " << ::gai_strerror(rc);
            throw Error(oss.str());
        }
    }

    ~AddrInfo() {
        if (info_ != nullptr) {
            ::freeaddrinfo(info_);
        }
    }

    struct addrinfo *get() const { return info_; }

   private:
    struct addrinfo *info_ = nullptr;
};

struct SendOptions {
    std::string host;
    int port = 0;
    std::filesystem::path file;
    std::filesystem::path relative_path;
    std::uint64_t offset = 0;
    std::uint64_t length = 0;
};

struct ReceiveOptions {
    std::string bind_address = "0.0.0.0";
    int port = 0;
    std::filesystem::path dest_root;
};

int connect_socket(const SendOptions &opts) {
    AddrInfo info(opts.host, std::to_string(opts.port), AF_UNSPEC, SOCK_STREAM);
    for (struct addrinfo *ai = info.get(); ai != nullptr; ai = ai->ai_next) {
        int sockfd = ::socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
        if (sockfd < 0) {
            continue;
        }
        if (::connect(sockfd, ai->ai_addr, ai->ai_addrlen) == 0) {
            return sockfd;
        }
        ::close(sockfd);
    }
    std::ostringstream oss;
    oss << "failed to connect to " << opts.host << ":" << opts.port;
    throw Error(oss.str());
}

int create_listening_socket(ReceiveOptions &opts) {
    AddrInfo info(opts.bind_address, std::to_string(opts.port), AF_UNSPEC, SOCK_STREAM);
    for (struct addrinfo *ai = info.get(); ai != nullptr; ai = ai->ai_next) {
        int sockfd = ::socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
        if (sockfd < 0) {
            continue;
        }
        int enable = 1;
        ::setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &enable, sizeof(enable));
        if (::bind(sockfd, ai->ai_addr, ai->ai_addrlen) == 0) {
            if (opts.port == 0) {
                struct sockaddr_storage addr;
                socklen_t len = sizeof(addr);
                if (::getsockname(sockfd, reinterpret_cast<struct sockaddr *>(&addr), &len) == 0) {
                    if (addr.ss_family == AF_INET) {
                        opts.port = ntohs(reinterpret_cast<struct sockaddr_in *>(&addr)->sin_port);
                    } else if (addr.ss_family == AF_INET6) {
                        opts.port = ntohs(reinterpret_cast<struct sockaddr_in6 *>(&addr)->sin6_port);
                    }
                }
            }
            if (::listen(sockfd, 16) != 0) {
                ::close(sockfd);
                continue;
            }
            return sockfd;
        }
        ::close(sockfd);
    }
    throw Error("failed to bind listening socket");
}

void run_send(const SendOptions &opts) {
    int sockfd = connect_socket(opts);
    const std::string path_bytes = opts.relative_path.generic_string();
    Header header{};
    header.path_length = htonl(static_cast<std::uint32_t>(path_bytes.size()));
    header.offset = host_to_network64(opts.offset);
    header.length = host_to_network64(opts.length);

    try {
        send_all(sockfd, &header, sizeof(header));
        if (!path_bytes.empty()) {
            send_all(sockfd, path_bytes.data(), path_bytes.size());
        }

        int fd = open_file_for_read(opts.file);
        if (::lseek(fd, static_cast<off_t>(opts.offset), SEEK_SET) < 0) {
            std::ostringstream oss;
            oss << "lseek failed: " << std::strerror(errno);
            ::close(fd);
            throw Error(oss.str());
        }

        std::vector<char> buffer(kBufferSize);
        std::uint64_t remaining = opts.length;
        while (remaining > 0) {
            std::size_t to_read = static_cast<std::size_t>(std::min<std::uint64_t>(remaining, buffer.size()));
            ssize_t rc = ::read(fd, buffer.data(), to_read);
            if (rc < 0) {
                if (errno == EINTR) {
                    continue;
                }
                std::ostringstream oss;
                oss << "read failed: " << std::strerror(errno);
                ::close(fd);
                throw Error(oss.str());
            }
            if (rc == 0) {
                ::close(fd);
                throw Error("unexpected EOF while reading source file");
            }
            send_all(sockfd, buffer.data(), static_cast<std::size_t>(rc));
            remaining -= static_cast<std::uint64_t>(rc);
        }
        ::close(fd);
    } catch (const Error &) {
        ::close(sockfd);
        throw;
    }
    ::close(sockfd);
}

void run_receive(ReceiveOptions opts) {
    int listen_fd = create_listening_socket(opts);
    std::cout << "PORT=" << opts.port << std::endl;
    std::cout.flush();

    int client_fd = -1;
    try {
        client_fd = ::accept(listen_fd, nullptr, nullptr);
        if (client_fd < 0) {
            throw Error("accept failed");
        }
        Header header{};
        recv_all(client_fd, &header, sizeof(header));
        std::uint32_t path_len = ntohl(header.path_length);
        std::vector<char> path_buf(path_len);
        if (path_len > 0) {
            recv_all(client_fd, path_buf.data(), path_buf.size());
        }
        std::string relative_path(path_buf.begin(), path_buf.end());
        std::uint64_t offset = network_to_host64(header.offset);
        std::uint64_t length = network_to_host64(header.length);

        std::filesystem::path dest_path = opts.dest_root / std::filesystem::path(relative_path);
        int fd = open_file_for_write(dest_path);
        if (::lseek(fd, static_cast<off_t>(offset), SEEK_SET) < 0) {
            std::ostringstream oss;
            oss << "lseek failed: " << std::strerror(errno);
            ::close(fd);
            throw Error(oss.str());
        }

        std::vector<char> buffer(kBufferSize);
        std::uint64_t remaining = length;
        while (remaining > 0) {
            std::size_t to_recv = static_cast<std::size_t>(std::min<std::uint64_t>(remaining, buffer.size()));
            recv_all(client_fd, buffer.data(), to_recv);
            std::size_t written = 0;
            while (written < to_recv) {
                ssize_t rc = ::write(fd, buffer.data() + written, to_recv - written);
                if (rc < 0) {
                    if (errno == EINTR) {
                        continue;
                    }
                    std::ostringstream oss;
                    oss << "write failed: " << std::strerror(errno);
                    ::close(fd);
                    throw Error(oss.str());
                }
                written += static_cast<std::size_t>(rc);
            }
            remaining -= to_recv;
        }
        ::close(fd);
    } catch (const Error &) {
        if (client_fd >= 0) {
            ::close(client_fd);
        }
        ::close(listen_fd);
        throw;
    }
    if (client_fd >= 0) {
        ::close(client_fd);
    }
    ::close(listen_fd);
}

void print_usage() {
    std::cerr << "Usage:\n"
                 "  dms_tcp_transfer send --host <host> --port <port> --file <path> "
                 "--relative-path <path> --offset <offset> --length <length>\n"
                 "  dms_tcp_transfer receive --bind <host> --port <port> --dest-root <path>\n";
}

SendOptions parse_send(int argc, char **argv) {
    SendOptions opts;
    for (int i = 0; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--host" && i + 1 < argc) {
            opts.host = argv[++i];
        } else if (arg == "--port" && i + 1 < argc) {
            opts.port = std::stoi(argv[++i]);
        } else if (arg == "--file" && i + 1 < argc) {
            opts.file = argv[++i];
        } else if (arg == "--relative-path" && i + 1 < argc) {
            opts.relative_path = argv[++i];
        } else if (arg == "--offset" && i + 1 < argc) {
            opts.offset = static_cast<std::uint64_t>(std::stoull(argv[++i]));
        } else if (arg == "--length" && i + 1 < argc) {
            opts.length = static_cast<std::uint64_t>(std::stoull(argv[++i]));
        } else {
            std::ostringstream oss;
            oss << "unknown or incomplete option: " << arg;
            throw Error(oss.str());
        }
    }
    if (opts.host.empty() || opts.port == 0 || opts.file.empty() || opts.relative_path.empty()) {
        throw Error("missing required send options");
    }
    return opts;
}

ReceiveOptions parse_receive(int argc, char **argv) {
    ReceiveOptions opts;
    for (int i = 0; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--bind" && i + 1 < argc) {
            opts.bind_address = argv[++i];
        } else if (arg == "--port" && i + 1 < argc) {
            opts.port = std::stoi(argv[++i]);
        } else if (arg == "--dest-root" && i + 1 < argc) {
            opts.dest_root = argv[++i];
        } else {
            std::ostringstream oss;
            oss << "unknown or incomplete option: " << arg;
            throw Error(oss.str());
        }
    }
    if (opts.dest_root.empty()) {
        throw Error("missing --dest-root option");
    }
    return opts;
}

}  // namespace

int main(int argc, char **argv) {
    if (argc < 2) {
        print_usage();
        return EXIT_FAILURE;
    }

    std::string mode = argv[1];
    try {
        if (mode == "send") {
            SendOptions opts = parse_send(argc - 2, argv + 2);
            run_send(opts);
        } else if (mode == "receive") {
            ReceiveOptions opts = parse_receive(argc - 2, argv + 2);
            run_receive(opts);
        } else if (mode == "--help" || mode == "-h") {
            print_usage();
            return EXIT_SUCCESS;
        } else {
            throw Error("unknown mode: " + mode);
        }
    } catch (const Error &err) {
        std::cerr << "error: " << err.what() << std::endl;
        return EXIT_FAILURE;
    }
    return EXIT_SUCCESS;
}
