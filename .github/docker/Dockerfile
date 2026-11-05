FROM archlinux:base-devel

RUN pacman -Syu --noconfirm --needed \
    gcc clang lld llvm compiler-rt \
    cmake ninja cppcheck openssl \
    actionlint shellcheck \
    ccache git just \
    && pacman -Scc --noconfirm

COPY verify-toolchain.sh /usr/local/bin/verify-cinder-toolchain
RUN /usr/local/bin/verify-cinder-toolchain
