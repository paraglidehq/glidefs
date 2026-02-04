#!/bin/bash
# Diagnostic script for mixed read/write performance analysis
# Run this after setting up an NBD device to identify the source of contention

set -e

DEVICE="${1:-/dev/nbd0}"
METRICS_URL="${2:-http://localhost:9090/metrics}"
TEST_SIZE="${3:-1G}"
RUNTIME="${4:-30}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "=============================================="
echo "  GlideFS Mixed I/O Performance Diagnostic"
echo "=============================================="
echo "Device: $DEVICE"
echo "Test size: $TEST_SIZE"
echo "Runtime per test: ${RUNTIME}s"
echo ""

# Function to fetch and display metrics
fetch_metrics() {
    local label="$1"
    echo ""
    echo "=== $label Metrics ==="

    if curl -sf "$METRICS_URL" > /tmp/metrics_$$.txt 2>/dev/null; then
        # Extract key metrics
        local read_ops=$(grep 'glidefs_guest_read_ops' /tmp/metrics_$$.txt | awk '{print $2}' | head -1)
        local write_ops=$(grep 'glidefs_guest_write_ops' /tmp/metrics_$$.txt | awk '{print $2}' | head -1)
        local cache_hits=$(grep 'glidefs_cache_hits' /tmp/metrics_$$.txt | awk '{print $2}' | head -1)
        local cache_misses=$(grep 'glidefs_cache_misses' /tmp/metrics_$$.txt | awk '{print $2}' | head -1)
        local hit_rate=$(grep 'glidefs_cache_hit_rate' /tmp/metrics_$$.txt | awk '{print $2}' | head -1)

        # Latency metrics
        local read_lat_avg=$(grep 'glidefs_read_latency_avg_us' /tmp/metrics_$$.txt | awk '{print $2}' | head -1)
        local read_lat_max=$(grep 'glidefs_read_latency_max_us' /tmp/metrics_$$.txt | awk '{print $2}' | head -1)
        local write_lat_avg=$(grep 'glidefs_write_latency_avg_us' /tmp/metrics_$$.txt | awk '{print $2}' | head -1)
        local write_lat_max=$(grep 'glidefs_write_latency_max_us' /tmp/metrics_$$.txt | awk '{print $2}' | head -1)
        local s3_lat_avg=$(grep 'glidefs_s3_fetch_latency_avg_us' /tmp/metrics_$$.txt | awk '{print $2}' | head -1)
        local s3_lat_max=$(grep 'glidefs_s3_fetch_latency_max_us' /tmp/metrics_$$.txt | awk '{print $2}' | head -1)
        local file_read_lat_avg=$(grep 'glidefs_file_read_latency_avg_us' /tmp/metrics_$$.txt | awk '{print $2}' | head -1)
        local file_read_lat_max=$(grep 'glidefs_file_read_latency_max_us' /tmp/metrics_$$.txt | awk '{print $2}' | head -1)

        echo "Operations:"
        echo "  Read ops:    ${read_ops:-0}"
        echo "  Write ops:   ${write_ops:-0}"
        echo "  Cache hits:  ${cache_hits:-0}"
        echo "  Cache misses: ${cache_misses:-0}"
        echo "  Hit rate:    ${hit_rate:-0}"
        echo ""
        echo "Latency Breakdown (microseconds):"
        printf "  %-15s %10s %10s\n" "Component" "Avg" "Max"
        printf "  %-15s %10s %10s\n" "---------" "---" "---"
        printf "  %-15s %10.0f %10.0f\n" "Read (e2e)" "${read_lat_avg:-0}" "${read_lat_max:-0}"
        printf "  %-15s %10.0f %10.0f\n" "Write (e2e)" "${write_lat_avg:-0}" "${write_lat_max:-0}"
        printf "  %-15s %10.0f %10.0f\n" "S3 Fetch" "${s3_lat_avg:-0}" "${s3_lat_max:-0}"
        printf "  %-15s %10.0f %10.0f\n" "File Read" "${file_read_lat_avg:-0}" "${file_read_lat_max:-0}"

        # Save for comparison
        cp /tmp/metrics_$$.txt "/tmp/metrics_${label// /_}.txt"
    else
        echo "  (Could not fetch metrics from $METRICS_URL)"
    fi
}

# Check if fio is installed
if ! command -v fio &> /dev/null; then
    echo -e "${RED}Error: fio is not installed${NC}"
    exit 1
fi

# Check if device exists
if [ ! -b "$DEVICE" ]; then
    echo -e "${RED}Error: $DEVICE does not exist${NC}"
    exit 1
fi

# Baseline metrics
fetch_metrics "Baseline"

echo ""
echo "----------------------------------------------"
echo "  Phase 1: Pure Random Read (4K, iodepth=64)"
echo "----------------------------------------------"
sudo fio --name=read_test \
    --filename=$DEVICE \
    --ioengine=libaio \
    --direct=1 \
    --rw=randread \
    --bs=4k \
    --iodepth=64 \
    --numjobs=1 \
    --runtime=$RUNTIME \
    --time_based \
    --group_reporting \
    --output-format=terse \
    2>/dev/null | awk -F';' '{
        printf "  IOPS: %d\n", $8
        printf "  Bandwidth: %.1f MB/s\n", $7/1024
        printf "  Avg Latency: %.2f ms\n", $40/1000
        printf "  P99 Latency: %.2f ms\n", $50/1000
    }'

fetch_metrics "After Pure Read"

echo ""
echo "----------------------------------------------"
echo "  Phase 2: Pure Random Write (4K, iodepth=64)"
echo "----------------------------------------------"
sudo fio --name=write_test \
    --filename=$DEVICE \
    --ioengine=libaio \
    --direct=1 \
    --rw=randwrite \
    --bs=4k \
    --iodepth=64 \
    --numjobs=1 \
    --runtime=$RUNTIME \
    --time_based \
    --group_reporting \
    --output-format=terse \
    2>/dev/null | awk -F';' '{
        printf "  IOPS: %d\n", $49
        printf "  Bandwidth: %.1f MB/s\n", $48/1024
        printf "  Avg Latency: %.2f ms\n", $81/1000
        printf "  P99 Latency: %.2f ms\n", $91/1000
    }'

fetch_metrics "After Pure Write"

echo ""
echo "----------------------------------------------"
echo "  Phase 3: Mixed 70/30 Read/Write (4K, iodepth=64)"
echo "----------------------------------------------"
sudo fio --name=mixed_test \
    --filename=$DEVICE \
    --ioengine=libaio \
    --direct=1 \
    --rw=randrw \
    --rwmixread=70 \
    --bs=4k \
    --iodepth=64 \
    --numjobs=1 \
    --runtime=$RUNTIME \
    --time_based \
    --group_reporting \
    --output-format=terse \
    2>/dev/null | awk -F';' '{
        # Mixed mode - reads and writes reported together
        read_iops = $8
        write_iops = $49
        total_iops = read_iops + write_iops
        read_bw = $7/1024
        write_bw = $48/1024
        read_lat = $40/1000
        write_lat = $81/1000
        read_p99 = $50/1000
        write_p99 = $91/1000

        printf "  Total IOPS: %d (read: %d, write: %d)\n", total_iops, read_iops, write_iops
        printf "  Total Bandwidth: %.1f MB/s\n", read_bw + write_bw
        printf "  Read Avg Latency: %.2f ms\n", read_lat
        printf "  Write Avg Latency: %.2f ms\n", write_lat
        printf "  Read P99 Latency: %.2f ms\n", read_p99
        printf "  Write P99 Latency: %.2f ms\n", write_p99
    }'

fetch_metrics "After Mixed"

echo ""
echo "=============================================="
echo "  Analysis Summary"
echo "=============================================="

# Calculate ratios if we have the data
if [ -f "/tmp/metrics_After_Pure_Read.txt" ] && [ -f "/tmp/metrics_After_Mixed.txt" ]; then
    read_lat_pure=$(grep 'glidefs_read_latency_avg_us' /tmp/metrics_After_Pure_Read.txt | awk '{print $2}' | head -1)
    read_lat_mixed=$(grep 'glidefs_read_latency_avg_us' /tmp/metrics_After_Mixed.txt | awk '{print $2}' | head -1)

    if [ -n "$read_lat_pure" ] && [ -n "$read_lat_mixed" ] && [ "$read_lat_pure" != "0" ]; then
        slowdown=$(echo "scale=2; $read_lat_mixed / $read_lat_pure" | bc 2>/dev/null || echo "N/A")
        echo "Read latency slowdown in mixed mode: ${slowdown}x"
    fi
fi

echo ""
echo "Key things to check:"
echo "  1. If S3 Fetch latency >> File Read latency: S3 is the bottleneck"
echo "  2. If File Read latency is high in mixed: File I/O contention"
echo "  3. If cache hit rate is low: Cold cache causing S3 fetches"
echo "  4. If write P99 >> avg even in pure mode: Sync worker interference"
echo ""

# Cleanup
rm -f /tmp/metrics_$$.txt

echo "Done!"
