#!/usr/bin/env bash
set -euo pipefail

echo "::group::Setting up GlideFS"

OS=$(uname -s | tr '[:upper:]' '[:lower:]')
ARCH=$(uname -m)

case "$OS-$ARCH" in
    linux-x86_64|linux-amd64)
        BINARY_NAME="glidefs-linux-amd64-pgo"
        ;;
    linux-aarch64|linux-arm64)
        BINARY_NAME="glidefs-linux-arm64-pgo"
        ;;
    linux-armv7*)
        BINARY_NAME="glidefs-linux-armv7-pgo"
        ;;
    linux-i686)
        BINARY_NAME="glidefs-linux-i686-pgo"
        ;;
    darwin-x86_64|darwin-amd64)
        BINARY_NAME="glidefs-darwin-x86_64-pgo"
        ;;
    darwin-aarch64|darwin-arm64)
        BINARY_NAME="glidefs-darwin-aarch64-pgo"
        ;;
    *)
        echo "::error::Unsupported OS/architecture combination: $OS-$ARCH"
        exit 1
        ;;
esac

if [ "$GLIDEFS_VERSION" = "latest" ]; then
    DOWNLOAD_URL="https://github.com/Barre/glidefs/releases/latest/download/glidefs-pgo-multiplatform.tar.gz"
else
    DOWNLOAD_URL="https://github.com/Barre/glidefs/releases/download/${GLIDEFS_VERSION}/glidefs-pgo-multiplatform.tar.gz"
fi

echo "Downloading GlideFS archive from: $DOWNLOAD_URL"
echo "Will extract binary: $BINARY_NAME"

# Download and extract the specific binary
TEMP_DIR=$(mktemp -d)
echo "Using temp directory: $TEMP_DIR"

if ! curl -fsSL "$DOWNLOAD_URL" | tar xz -C "$TEMP_DIR"; then
    echo "::error::Failed to download or extract GlideFS archive"
    exit 1
fi

if [ ! -f "$TEMP_DIR/$BINARY_NAME" ]; then
    echo "::error::Binary $BINARY_NAME not found in archive"
    ls -la "$TEMP_DIR"
    exit 1
fi

sudo mv "$TEMP_DIR/$BINARY_NAME" /usr/local/bin/glidefs
rm -rf "$TEMP_DIR"

sudo chmod +x /usr/local/bin/glidefs

echo "GlideFS installed at: /usr/local/bin/glidefs"

if [ "$OS" = "linux" ]; then
    if ! command -v mount.nfs &> /dev/null; then
        echo "Installing NFS utilities..."
        sudo apt-get update && sudo apt-get install -y nfs-common || sudo yum install -y nfs-utils || true
    fi
fi

mkdir -p "$SLATEDB_CACHE_DIR"

if [ -z "${MOUNT_PATH:-}" ]; then
    case "$OS" in
        linux)
            MOUNT_PATH="/mnt/glidefs"
            ;;
        darwin)
            MOUNT_PATH="/tmp/glidefs"
            ;;
        *)
            MOUNT_PATH="/tmp/glidefs"
            ;;
    esac
fi

if [ "$OS" = "darwin" ] && [[ "$MOUNT_PATH" == /mnt/* ]]; then
    echo "::warning::macOS doesn't have /mnt by default, using /tmp instead"
    MOUNT_PATH="/tmp/$(basename "$MOUNT_PATH")"
fi

sudo mkdir -p "$MOUNT_PATH"

# Create GlideFS configuration file
cat > glidefs-action.toml << EOF
[cache]
dir = "$SLATEDB_CACHE_DIR"
disk_size_gb = $SLATEDB_CACHE_SIZE_GB
memory_size_gb = $GLIDEFS_MEMORY_CACHE_SIZE_GB

[storage]
url = "$OBJECT_STORE_URL"
encryption_password = "$GLIDEFS_ENCRYPTION_PASSWORD"

[servers.nfs]
addresses = ["$GLIDEFS_NFS_HOST:$GLIDEFS_NFS_HOST_PORT"]
EOF

# Add AWS config if credentials are provided
if [ -n "${AWS_ACCESS_KEY_ID:-}" ] && [ -n "${AWS_SECRET_ACCESS_KEY:-}" ]; then
    cat >> glidefs-action.toml << EOF

[aws]
access_key_id = "$AWS_ACCESS_KEY_ID"
secret_access_key = "$AWS_SECRET_ACCESS_KEY"
default_region = "$AWS_DEFAULT_REGION"
EOF
    
    # Add optional AWS settings
    if [ -n "${AWS_ENDPOINT:-}" ]; then
        echo "endpoint = \"$AWS_ENDPOINT\"" >> glidefs-action.toml
    fi
    if [ "$AWS_ALLOW_HTTP" = "true" ]; then
        echo "allow_http = \"true\"" >> glidefs-action.toml
    fi
fi

# Add Azure config if credentials are provided
if [ -n "${AZURE_STORAGE_ACCOUNT_NAME:-}" ] && [ -n "${AZURE_STORAGE_ACCOUNT_KEY:-}" ]; then
    cat >> glidefs-action.toml << EOF

[azure]
storage_account_name = "$AZURE_STORAGE_ACCOUNT_NAME"
storage_account_key = "$AZURE_STORAGE_ACCOUNT_KEY"
EOF
fi

nohup /usr/local/bin/glidefs run -c glidefs-action.toml > glidefs.log 2>&1 &
GLIDEFS_PID=$!
echo $GLIDEFS_PID > glidefs.pid

echo "Waiting for GlideFS to start..."
for i in {1..30}; do
    if nc -z "$GLIDEFS_NFS_HOST" "$GLIDEFS_NFS_HOST_PORT" 2>/dev/null; then
        echo "GlideFS is ready!"
        break
    fi
    if [ $i -eq 30 ]; then
        echo "::error::GlideFS failed to start within 30 seconds"
        cat glidefs.log
        exit 1
    fi
    sleep 1
done

echo "Mounting GlideFS..."
case "$OS" in
    linux)
        sudo mount -t nfs -o vers=3,async,nolock,tcp,port="$GLIDEFS_NFS_HOST_PORT",mountport="$GLIDEFS_NFS_HOST_PORT",hard "$GLIDEFS_NFS_HOST":/ "$MOUNT_PATH"
        ;;
    darwin)
        sudo mount -t nfs -o async,nolocks,vers=3,tcp,port="$GLIDEFS_NFS_HOST_PORT",mountport="$GLIDEFS_NFS_HOST_PORT",hard "$GLIDEFS_NFS_HOST":/ "$MOUNT_PATH"
        ;;
    *)
        echo "::error::Mounting is not supported on OS: $OS"
        exit 1
        ;;
esac

if mount | grep -q "$MOUNT_PATH"; then
    echo "GlideFS successfully mounted at: $MOUNT_PATH"
    df -h "$MOUNT_PATH"
else
    echo "::error::Failed to mount GlideFS"
    cat glidefs.log
    exit 1
fi

echo "mount-path=$MOUNT_PATH" >> $GITHUB_OUTPUT
echo "nfs-endpoint=$GLIDEFS_NFS_HOST:$GLIDEFS_NFS_HOST_PORT" >> $GITHUB_OUTPUT

cat > cleanup-glidefs.sh << 'EOF'
#!/usr/bin/env bash
set -euo pipefail

echo "::group::Cleaning up GlideFS"

if mount | grep -q "$MOUNT_PATH"; then
    echo "Unmounting $MOUNT_PATH..."
    sudo umount "$MOUNT_PATH" || sudo umount -f "$MOUNT_PATH" || true
fi

if [ -f glidefs.pid ]; then
    PID=$(cat glidefs.pid)
    if kill -0 $PID 2>/dev/null; then
        echo "Stopping GlideFS (PID: $PID)..."
        kill $PID || true
        sleep 2
        kill -9 $PID 2>/dev/null || true
    fi
    rm -f glidefs.pid
fi

if [ -f glidefs.log ] && grep -i error glidefs.log > /dev/null 2>&1; then
    echo "::warning::GlideFS had errors during execution:"
    grep -i error glidefs.log | head -20
fi

echo "::endgroup::"
EOF
chmod +x cleanup-glidefs.sh

echo "$PWD/cleanup-glidefs.sh" >> $GITHUB_PATH

echo "::endgroup::"
echo "::notice::GlideFS is ready! Volume mounted at: $MOUNT_PATH"
