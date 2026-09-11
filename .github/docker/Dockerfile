FROM archlinux:base-devel

RUN pacman -Syu --noconfirm \
    && pacman -S --noconfirm --needed \
       gcc clang lld libc++ libc++abi \
        cmake ninja cppcheck openssl \
        curl git \
     && pacman -Scc --noconfirm

RUN curl -fsSL https://just.systems/install.sh | bash -s -- --to /usr/local/bin

RUN gcc --version && g++ --version && clang++ --version \
    && cmake --version && ninja --version && just --version && git --version
