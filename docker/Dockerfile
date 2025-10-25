# Use Ubuntu 24.04 as base
FROM ubuntu:24.04

# MPI version
ENV OPENMPI_VERSION=4.1.6

# Avoid interactive prompts during apt installs
ENV DEBIAN_FRONTEND=noninteractive
ENV LANG=C.UTF-8 LC_ALL=C.UTF-8

# Ubuntu 24.04 needs to ignore system break packages for pip installs in global env
ENV PIP_BREAK_SYSTEM_PACKAGES=1

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

WORKDIR /workspace

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
      openssh-client \
      rdma-core \
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

# Install UCX from APT
RUN set -eux; \
    apt-get update && \
    apt-get install -y \
      libevent-dev \
      libhwloc-dev \
      libucx0 \
      libucx-dev \
      ucx-utils; \  
    rm -rf /var/lib/apt/lists/*

# Build and install openmpi from source
RUN set -eux; \
    UCX_PREFIX=/usr; \
    UCX_MULTIARCH="$(dpkg-architecture -qDEB_HOST_MULTIARCH)"; \
    echo "Detected multiarch: ${UCX_MULTIARCH}"; \
    echo "UCX include: ${UCX_PREFIX}/include/ucx"; \
    echo "UCX libdir:  /usr/lib/${UCX_MULTIARCH}"; \
    cd /tmp && \
    wget https://download.open-mpi.org/release/open-mpi/v4.1/openmpi-${OPENMPI_VERSION}.tar.gz && \
    tar -xzf openmpi-${OPENMPI_VERSION}.tar.gz && \
    cd openmpi-${OPENMPI_VERSION} && \
    ./configure \
        --prefix=/usr/local/openmpi-${OPENMPI_VERSION} \
        --with-ucx=${UCX_PREFIX} \
        --with-ucx-libdir=/usr/lib/${UCX_MULTIARCH}; \
    make -j$(nproc) && \
    make install

ENV PATH="/usr/local/openmpi-${OPENMPI_VERSION}/bin:${PATH}"
ENV LD_LIBRARY_PATH="/usr/local/openmpi-${OPENMPI_VERSION}/lib:${LD_LIBRARY_PATH}"

# allow mpi to run as root
ENV OMPI_ALLOW_RUN_AS_ROOT=1
ENV OMPI_ALLOW_RUN_AS_ROOT_CONFIRM=1

# Smoke checks
RUN set -eux; \
    mpirun --version; \
    pkg-config --modversion ucx; \
    ompi_info --param pml all | grep -i ucx || true; \
    ompi_info --param osc all | grep -i ucx || true

# Run redis background service
RUN set -eux; \
    redis-server --daemonize yes;

# Default command to keep the container running
CMD ["sleep", "infinity"]

# Build:
# docker build -t dms:latest -f /Users/mason/DMS/docker/Dockerfile /Users/mason/DMS/docker --no-cache
#
# Run:
# docker run --rm -dit --name dms --privileged --network host --ipc host --shm-size=1g -v /Users/mason/DMS:/workspace/DMS dms:latest bash