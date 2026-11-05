#!/usr/bin/env bash
set -euo pipefail

version_at_least() {
    local actual="$1" minimum="$2"
    [[ "$(printf '%s\n%s\n' "$actual" "$minimum" | sort -V | head -n1)" == "$minimum" ]]
}

gcc_version="$(gcc -dumpfullversion -dumpversion)"
clang_version="$(clang++ -dumpversion)"
cmake_version="$(cmake --version | sed -n '1s/^cmake version //p')"

version_at_least "$gcc_version" 16.0 || {
    echo "Cinder requires GCC >= 16 (found ${gcc_version:-missing})" >&2
    exit 1
}

version_at_least "$clang_version" 22.0 || {
    echo "Cinder requires Clang >= 22 (found ${clang_version:-missing})" >&2
    exit 1
}

version_at_least "$cmake_version" 4.4 || {
    echo "Cinder requires CMake >= 4.4 (found ${cmake_version:-missing})" >&2
    exit 1
}

gcc --version
clang++ --version
cmake --version
ninja --version
just --version
actionlint --version
shellcheck --version
git --version
llvm-ar --version
