# Use Ubuntu 24.04 as base
FROM ubuntu:24.04

# Avoid interactive prompts during apt installs
ENV DEBIAN_FRONTEND=noninteractive
ENV LANG=C.UTF-8 LC_ALL=C.UTF-8

# Ubuntu 24.04 needs to ignore system break packages for pip installs in global env
ENV PIP_BREAK_SYSTEM_PACKAGES=1

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

# Update and install common runtime/build tools for typical DMS workloads
RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends \
      ca-certificates \
      curl \
      wget \
      git \
      tzdata \
      locales \
      openssl \
      unzip \
      xz-utils \
      tar \
      bzip2 \
      gnupg \
      netbase \
      iproute2 \
      iputils-ping \
      dnsutils \
      python3 \
      python3-venv \
      python3-pip \
      python-is-python3 \
      build-essential \
      pkg-config \
      cmake \
      cmake-curses-gui \
      ninja-build \
      gdb \
      strace \
      ltrace \
      vim \
      nano \
      htop \
      procps \
      libc6 \
      libstdc++6 \
      libssl3 \
      redis-server \
    ; \
    locale-gen en_US.UTF-8 || true; \
    update-ca-certificates
    
WORKDIR /workspace

# Run redis background service
RUN redis-server --daemonize yes

CMD ["sleep", "infinity"]

# Build:
# docker build -t dms:latest -f /Users/mason/DMS/docker/Dockerfile /Users/mason/DMS/docker --no-cache
#
# Run:
# docker run --rm -it --name dms --privileged --network host --ipc host -v /Users/mason/DMS:/workspace/DMS dms:latest bash