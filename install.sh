#!/bin/sh
# Install script for GlideFS

set -e

REPO="paraglidehq/glidefs"
BINARY_NAME="glidefs"
INSTALL_DIR="${INSTALL_DIR:-/usr/local/bin}"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

info() { printf "${GREEN}[INFO]${NC} %s\n" "$1"; }
warn() { printf "${YELLOW}[WARN]${NC} %s\n" "$1"; }
error() { printf "${RED}[ERROR]${NC} %s\n" "$1" >&2; exit 1; }

detect_platform() {
    os="$(uname -s)"
    arch="$(uname -m)"

    case "$os" in
        Darwin*) os="darwin" ;;
        Linux*)  os="linux" ;;
        *)       error "Unsupported OS: $os" ;;
    esac

    case "$arch" in
        x86_64|amd64)
            [ "$os" = "darwin" ] && arch="x86_64" || arch="amd64"
            ;;
        aarch64|arm64)
            [ "$os" = "darwin" ] && arch="aarch64" || arch="arm64"
            ;;
        armv7l|armv7)  arch="armv7" ;;
        i386|i686)     arch="i686" ;;
        ppc64le)       arch="ppc64le" ;;
        riscv64)       arch="riscv64" ;;
        s390x)         arch="s390x" ;;
        *)             error "Unsupported architecture: $arch" ;;
    esac

    echo "${os}-${arch}"
}

main() {
    info "Installing ${BINARY_NAME}..."

    platform="$(detect_platform)"
    info "Detected platform: $platform"

    version="${VERSION:-latest}"
    if [ "$version" = "latest" ]; then
        url="https://github.com/${REPO}/releases/latest/download/${BINARY_NAME}-${platform}"
    else
        url="https://github.com/${REPO}/releases/download/${version}/${BINARY_NAME}-${platform}"
    fi

    info "Downloading from: $url"

    tmp="$(mktemp)"
    trap 'rm -f "$tmp"' EXIT

    if command -v curl >/dev/null 2>&1; then
        curl -fSL "$url" -o "$tmp" || error "Download failed"
    elif command -v wget >/dev/null 2>&1; then
        wget -q "$url" -O "$tmp" || error "Download failed"
    else
        error "curl or wget required"
    fi

    chmod +x "$tmp"

    dest="$INSTALL_DIR/$BINARY_NAME"
    if [ -w "$INSTALL_DIR" ]; then
        mv "$tmp" "$dest"
    else
        info "Using sudo to install to $INSTALL_DIR"
        sudo mv "$tmp" "$dest"
    fi

    info "Installed to $dest"
    "$dest" --version 2>/dev/null || info "Run '$BINARY_NAME' to get started"
}

main
