# GlideFS

Block devices on S3. NBD server with write-behind caching.

```
Random Read:   15,856 IOPS    61.9 MB/s    4.04ms avg
Random Write:  28,094 IOPS   109.7 MB/s    2.28ms avg
Mixed 70/30:   17,345 IOPS    67.8 MB/s    3.75ms avg

Write latency:   20µs (local SSD)
Cache hit rate:  99.67%
```

---

## What It Does

S3 storage. Block device interface. Write to local SSD, sync to S3 in background.

- **20µs writes** — Local SSD, not network
- **<100ms ZFS snapshots** — FLUSH hits disk, not S3
- **Wake anywhere** — VM data lives in S3, start on any node
- **Scale to zero** — Drain to S3, pay nothing when idle

---

## Why

You run microVMs. You want them to:

- **Scale to zero** — Stop paying when idle
- **Wake anywhere** — Start on any node, not tied to hardware
- **Fork instantly** — Clone VMs in milliseconds for serverless workloads

The problem: VM storage is local. Move the VM, move the disk. Pay for storage even when the VM is off.

The obvious fix: Put VM data in S3. But S3 round-trips are 50-200ms. ZFS snapshots that should take 10ms now take 5-15 seconds. Your fork speed dies.

```
Naive S3:        FLUSH → S3 PUT → return     5-15 seconds
GlideFS:         FLUSH → local SSD → return     10ms
                              ↓
                     background → S3           async
```

GlideFS writes to local SSD, syncs to S3 in background. Snapshots stay instant. Data ends up in S3. You get both.

---

## Architecture

```
Guest VM
    │
    │ NBD
    ▼
┌─────────────────────────────────────────┐
│              GlideFS                     │
│                                          │
│   NBD Handler → Write Cache → S3 Sync   │
│                     │             │      │
│                Local SSD     Background  │
│              (dirty queue)    upload     │
└─────────────────────────────────────────┘
                      │
                      ▼
                     S3
```

Write: Guest → local SSD + mark dirty → return. 20µs.

Read: Check cache → hit (500µs) / miss (fetch from S3, 50-300ms).

Sync: Background worker drains dirty blocks to S3 in batches.

---

## Install

```bash
cargo install glidefs
```

---

## Quick Start

```toml
# glidefs.toml
[cache]
dir = "/var/cache/glidefs"
disk_size_gb = 100.0

[storage]
url = "s3://my-bucket/vms"
encryption_password = "${GLIDEFS_PASSWORD}"

[servers.nbd]
unix_socket = "/var/run/glidefs.sock"
auto_create_size_gb = 500.0
```

```bash
glidefs run -c glidefs.toml
```

Connect a VM:

```bash
sudo nbd-client -unix /var/run/glidefs.sock /dev/nbd0 -N my-vm
```

Done. Export created automatically. No API calls.

---

## Per-VM Usage

Each VM gets its own export. Just connect with the VM name:

```bash
sudo nbd-client -unix /var/run/glidefs.sock /dev/nbd0 -N vm-alice
sudo nbd-client -unix /var/run/glidefs.sock /dev/nbd1 -N vm-bob
sudo nbd-client -unix /var/run/glidefs.sock /dev/nbd2 -N vm-charlie
```

Each export is isolated. Data stored at `s3://my-bucket/vms/nbd/{name}/`.

Need a different size? Grow it:

```bash
curl -X PUT localhost:8080/api/exports/vm-alice -d '{"size_gb": 1000}'
# Reconnect NBD client to see new size
```

---

## With ZFS

```bash
sudo nbd-client -unix /var/run/glidefs.sock /dev/nbd0 -N my-vm
sudo zpool create vmpool /dev/nbd0
time sudo zfs snapshot vmpool@snap1  # <100ms
```

---

## Operations

### Scale to Zero

```bash
# Drain dirty blocks to S3
curl -X POST localhost:8080/api/exports/my-vm/drain

# Remove export (keeps S3 data)
curl -X DELETE localhost:8080/api/exports/my-vm
```

Data in S3. Zero local resources. Zero cost.

### Wake Anywhere

```bash
# On any node - just connect, data pulls from S3
sudo nbd-client -unix /var/run/glidefs.sock /dev/nbd0 -N my-vm
```

### Live Migration

```
Node A                         Node B
──────                         ──────
VM running
                               Connect (readonly auto-created)
Drain: POST /drain
Pause VM
Disconnect NBD
                               Promote: POST /promote
                               Resume VM

Downtime: 100-500ms
```

---

## API

For operations beyond connect/disconnect:

| Endpoint | Method | What it does |
|----------|--------|--------------|
| `/api/exports` | GET | List exports |
| `/api/exports/{name}` | PUT | Resize export (grow only) |
| `/api/exports/{name}` | DELETE | Remove export |
| `/api/exports/{name}/drain` | POST | Sync dirty blocks to S3 |
| `/api/exports/{name}/promote` | POST | Readonly → read-write |
| `/api/exports/{name}/metrics` | GET | I/O stats |
| `/metrics` | GET | Prometheus metrics |

---

## Tuning

### Batch Size

Blocks grouped into S3 objects. Fewer PUTs, lower cost.

```toml
[servers.nbd]
blocks_per_batch = 25  # 25 × 128KB = 3.2MB per S3 object
```

| Batch | Size | Storage overhead | Sync speed |
|-------|------|------------------|------------|
| 100 | 12.8MB | Higher | Slower |
| 25 | 3.2MB | Lowest | Fast |
| 10 | 1.28MB | Low | Fastest |

Default is 25. Best balance for scale-to-zero workloads.

### Sync Delay

```toml
[servers.nbd]
sync_delay_ms = 8000  # 8s cooldown for hot batches
```

Longer delay = more coalescing = fewer S3 PUTs. Trade-off: larger dirty window.

---

## Durability

| Event | Data location |
|-------|---------------|
| After FLUSH | Local SSD |
| After sync | S3 |
| After drain | S3 (guaranteed) |

Data between FLUSH and S3 sync is at risk if the node dies.

Mitigations:
- Background sync keeps dirty set small
- SIGTERM triggers drain before exit
- NVMe with power-loss protection recommended
- Call drain before migration

Same trade-off as ZFS `sync=disabled` or async database replication.

---

## Cost

For scale-to-zero (20% active, 80% idle):

| | Per VM/month |
|--|--------------|
| Storage overhead | ~1.3x data |
| API cost | $0.44 |
| Storage (20GB) | $0.60 |
| **Total** | **$1.04** |

---

## Requirements

- Linux
- Local SSD (NVMe recommended)
- S3-compatible storage
- VPC endpoint recommended (no egress cost)

---

## License

AGPL-3.0
