# GlideFS Architecture

NBD server with write-behind caching to S3. Makes ZFS snapshot/clone instant by returning FLUSH after local SSD fsync, not S3 round-trip.

## Data Flow

### Write Path

```
Guest write → NBD handler → WriteCache (local SSD)
                               ├── Write to sparse file
                               ├── Mark block present (atomic OR)
                               ├── Mark block dirty (CAS)
                               ├── Push to dirty_queue (lock-free)
                               ├── Notify sync worker
                               └── return (<1ms)
                                      │
                           (background) → S3BlockStore → S3
```

### Read Path (Read-Through)

```
Guest read → WriteCache.read_with_fetch()
               │
               ├── Block present locally?
               │     └── Yes → Return from SSD (<1ms)
               │
               └── No → S3BlockStore.read_block()
                          │
                          ├── In S3 → Fetch, cache, mark present (50-300ms)
                          │
                          └── Not in S3 → Return zeros (never written)
```

### Flush Path

```
Guest FLUSH → fsync(local_ssd) → return (<10ms)

(S3 sync continues in background)
```

This is the key insight: ZFS snapshot/clone are metadata operations. Should take milliseconds. Blocking on S3 during FLUSH makes them take 5-15 seconds.

## Concepts & Terminology

| Term | Definition | NOT |
|------|------------|-----|
| Export | NBD device backed by S3 prefix + local cache | Not a VM, not a volume |
| Batch | S3 object containing N consecutive blocks | Not a transaction, not atomic |
| Dirty | Block has local data newer than S3 | Not "needs write" - it was already written locally |
| Present | Block exists in local cache | Not "clean" - may be dirty |
| Lease | S3-based lock for single-writer enforcement | Not a lock service, not distributed consensus |
| Drain | Sync all dirty blocks to S3, blocking | Not shutdown, export stays active |

## State Machines

### Block State (Runtime)

```
Clean ◄──── sync complete ───── Syncing
  │                               ▲
  │ write                  claim dirty
  ▼                               │
Dirty ───── sync start ──────────┘
  ▲
  └── write during sync / sync failure
```

| From | Event | To | Notes |
|------|-------|-----|-------|
| Clean | write | Dirty | Block has local changes |
| Dirty | claim | Syncing | Sync worker took ownership |
| Syncing | upload complete | Clean | Matches S3 |
| Syncing | upload failed | Dirty | Retry later |
| Syncing | write | Dirty | New write during sync |

### Device Lifecycle (Typestate)

Compile-time enforcement. Can't serve I/O in wrong state.

```
WriteCache<Initializing>
         │ load cache
         ▼
WriteCache<Recovering>
         │ finish_recovery()
         ▼
WriteCache<Active>       ◄─ Only this state serves I/O
         │ shutdown()
         ▼
WriteCache<Draining>
         │ finish()
         ▼
       [Dropped]
```

| State | Allowed Operations |
|-------|-------------------|
| Initializing | Load local cache file |
| Recovering | Sync dirty/syncing blocks to S3 |
| Active | read(), write(), flush() |
| Draining | Sync remaining, reject new writes |

## Core Mechanism

### S3 Block Batching

Blocks are grouped into batches to reduce S3 PUTs:

```
Block 0-24   → s3://bucket/path/{export}/batches/000000000000
Block 25-49  → s3://bucket/path/{export}/batches/000000000001
...
```

**Why 25 blocks (3.2MB)?** Smaller batches waste less space for sparse/scattered data. Filesystems don't write sequentially. Larger batches = more storage overhead when only a few blocks per batch contain data.

| Batch Size | S3 Object Size | Storage Overhead | API Cost |
|------------|----------------|------------------|----------|
| 10 | 1.28MB | Lowest | Higher |
| 25 | 3.2MB | Low | Balanced |
| 100 | 12.8MB | Higher | Lower |

### Background Sync Worker

Event-driven. Zero CPU when idle.

```rust
loop {
    // Fencing: stop if lease lost
    if !lease_state.is_valid() { break; }

    // O(1) pop from lock-free queue
    dirty_blocks = cache.claim_dirty_blocks(batch_size);

    if dirty_blocks.is_empty() {
        // Sleep until write path wakes us
        select! {
            _ = cache.wait_for_dirty() => {}
            _ = sleep(1s) => {}  // Safety timeout
            _ = shutdown.changed() => break
        }
        continue;
    }

    // Group by S3 batch, defer hot batches
    for (batch_num, blocks) in group_by_batch(dirty_blocks) {
        if recently_synced(batch_num) {
            // Defer: mark dirty again, coalesce more writes
            requeue(blocks);
        } else {
            // GET-modify-PUT
            batch_data = s3.get_or_empty(batch_num);
            for block in blocks {
                batch_data[offset] = read_local(block);  // Page cache hit
            }
            s3.put(batch_num, batch_data);
            mark_hot(batch_num);
        }
    }
}
```

**Hot batch deferral**: Recently-synced batches are deferred. If VM writes blocks 0-99 (all batch 0) sequentially:
- Without deferral: 100 GET-modify-PUT operations
- With deferral: 1-2 GET-modify-PUT operations

### S3-Based Leases

Single-writer enforcement without external coordination (no etcd/Zookeeper).

```
s3://bucket/path/nbd/{export}/lease.json
```

```json
{
  "owner": "node-abc123",
  "generation": 42,
  "acquired_at": 1706900000,
  "ttl_seconds": 300
}
```

| Operation | Mechanism |
|-----------|-----------|
| Acquire | GET → check expiry → conditional PUT (If-Match) |
| Renew | Re-acquire with generation+1 (every 2.5 min) |
| Release | PUT with empty owner |
| Fencing | Generation counter checked before each batch write |

**Defense-in-depth**: Batch writes also use conditional PUT (If-Match with ETag). Even if leases fail, concurrent modification is detected.

## Design Decisions

### Why Write-Behind Over Write-Through?

```
Write-through:  FLUSH → S3 PUT → return     2-15 seconds
Write-behind:   FLUSH → local fsync → return     10ms
                         ↓
                background → S3              async
```

ZFS snapshot/clone are metadata operations. Blocking FLUSH on S3 turns 10ms operations into 10 second operations. We chose 420x faster snapshots.

**Trade-off**: Data between FLUSH and S3 sync is at risk if node dies. Same trade-off as `zfs sync=disabled` or MySQL async replication.

### Why Typestate for Device Lifecycle?

```rust
// Compile-time: impossible to serve I/O during recovery
WriteCache<Recovering>.read()  // Won't compile

// Runtime alternative (rejected):
cache.read()  // Panics if not ready - found in production
```

- Zero runtime overhead (no state checks in hot path)
- Bugs found at compile time
- Types encode valid transitions

### Why Direct Block-to-S3 (No LSM)?

LSM trees (RocksDB, SlateDB) add overhead:
- WAL + memtable + compaction
- Good for small key-value operations
- Bad for 128KB block I/O

Direct block-to-S3:
- One S3 batch per N blocks
- O(1) lookup: block 42 → batch 1, offset 17
- No compaction
- Simpler crash recovery

### Why Sparse Files for Cache?

100GB device with 10GB written = ~10GB on disk. OS handles allocation. No pre-allocation overhead.

### Why 128KB Default Block Size?

Matches ZFS default recordsize. Minimizes read amplification for ZFS workloads.

## Trust Model

### What We Rely On

| Trust | Provided By |
|-------|-------------|
| Encryption at rest | S3 Server-Side Encryption (SSE) |
| Access control | S3 bucket policies, IAM |
| Local disk encryption | OS-level (LUKS, FileVault) |
| Network security | VPC, firewall |

### What We Do NOT Provide

| Gap | Why Acceptable |
|-----|----------------|
| Client-side encryption | Incompatible with S3 range requests for batch reads |
| NBD protocol encryption | NBD intended for localhost/hypervisor network |
| Application-layer auth | API binds to localhost in production |

### Why S3 SSE Instead of Client-Side?

AEAD encryption (ChaCha20-Poly1305) requires full ciphertext to decrypt. We use S3 range requests to read individual blocks from batches. Client-side encryption breaks this.

## Configuration

```toml
[storage]
url = "s3://bucket/path"

[cache]
dir = "/var/cache/glidefs"
disk_size_gb = 100.0
memory_size_gb = 10.0    # Optional

[servers.nbd]
addresses = ["0.0.0.0:10809"]
unix_socket = "/run/glidefs/nbd.sock"
api_address = "127.0.0.1:8080"
block_size = 131072      # 128KB
blocks_per_batch = 25    # 3.2MB per S3 object
sync_delay_ms = 8000     # Hot batch cooldown

[[servers.nbd.exports]]
name = "vm-001"
size_gb = 50.0
```

| Variable | Default | Rationale |
|----------|---------|-----------|
| `block_size` | 128KB | Match ZFS recordsize |
| `blocks_per_batch` | 25 | Balance storage efficiency vs API cost |
| `sync_delay_ms` | 8000ms | Hot batch cooldown, reduces write amplification |
| `cache.dir` | required | Local SSD cache location |
| `cache.disk_size_gb` | required | Max cache size |

**Internal defaults** (not configurable):

| Variable | Default | Rationale |
|----------|---------|-----------|
| `dirty_queue_warn_threshold` | 1000 | 128MB @ 128KB blocks |
| `dirty_queue_critical_threshold` | 10000 | 1.28GB @ 128KB blocks |

## Failure Modes

| Failure | Impact | Recovery |
|---------|--------|----------|
| Crash before sync | Dirty blocks in local cache | Recovered on restart, synced to S3 |
| S3 unavailable | Writes continue locally | Sync resumes when S3 returns |
| Local SSD failure | Unsynced blocks lost | Restore from S3 (cold start) |
| Graceful shutdown | None | All dirty blocks synced before exit |
| Lease lost | Sync worker stops | Export becomes readonly |
| Network partition | Lease expires after TTL | Other node can acquire |

### Data Loss Window

Writes after FLUSH but before S3 sync are at risk if node dies.

**Mitigations**:
1. Continuous drain keeps dirty set small
2. Graceful shutdown syncs all dirty blocks
3. NVMe with power-loss protection recommended
4. On crash: dirty blocks in local cache recovered on restart

### When This Architecture Is Appropriate

| Good Fit | Bad Fit |
|----------|---------|
| ZFS snapshots must be instant | Need synchronous S3 replication |
| MicroVM fork speed critical | Cannot tolerate any data loss |
| Users accept `sync=disabled` trade-offs | Very few writes (write-through fine) |
| Production nodes have battery-backed NVMe | No UPS/power protection |

## Operational Scenarios

### Scale to Zero

```
1. VM stops
2. POST /api/exports/vm-001/drain    (blocking)
3. DELETE /api/exports/vm-001
4. All data in S3, zero local resources
```

### Wake Anywhere

```
1. POST nodeB:8080/api/exports { "name": "vm-001", "size_gb": 50 }
2. nbd-client nodeB /dev/nbd0 -N vm-001
3. VM starts, reads pull from S3 on demand

First read: 50-300ms (S3)
Subsequent: <1ms (local)
```

### Live Migration

```
Node A                              Node B
──────                              ──────
VM running
                                    POST /exports (readonly=true)
                                    Pre-warms cache from S3

POST /exports/vm-001/drain
(blocks until synced)

Pause VM

DELETE /exports/vm-001
                                    POST /exports/vm-001/promote

                                    Resume VM

Downtime: ~100-500ms
```

## HTTP API

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/exports` | GET | List exports |
| `/api/exports` | POST | Create export |
| `/api/exports/{name}` | GET | Export info |
| `/api/exports/{name}` | DELETE | Remove export |
| `/api/exports/{name}/drain` | POST | Sync to S3 (blocking) |
| `/api/exports/{name}/promote` | POST | Readonly → read-write |
| `/api/exports/{name}/metrics` | GET | I/O statistics |
| `/health` | GET | Health check |

## Metrics

Available via `GET /api/exports/{name}/metrics`:

| Metric | Description |
|--------|-------------|
| `guest_bytes_written` | Total bytes written by VM |
| `guest_bytes_read` | Total bytes read by VM |
| `s3_bytes_written` | Total bytes written to S3 |
| `s3_bytes_read` | Total bytes read from S3 |
| `batches_written` | S3 batch objects written |
| `cache_hits` / `cache_misses` | Read-through effectiveness |

**Derived**:

| Metric | Formula | Meaning |
|--------|---------|---------|
| `write_amplification` | s3_bytes / guest_bytes | >1 = batching overhead |
| `coalesce_ratio` | guest_ops / batches_written | >1 = batching effective |
| `cache_hit_rate` | hits / (hits + misses) | Higher = fewer S3 fetches |

## Package Structure

| File | Purpose |
|------|---------|
| `main.rs` | CLI entry point |
| `config.rs` | TOML parsing |
| `nbd/server.rs` | NBD protocol (TCP/Unix) |
| `nbd/router.rs` | Multi-tenant export routing |
| `nbd/handler.rs` | NBD command handling |
| `nbd/block_store.rs` | S3 batch storage |
| `nbd/write_cache.rs` | Write-behind cache with typestate |
| `nbd/lease.rs` | S3-based distributed leases |
| `nbd/metrics.rs` | Per-export I/O stats |
| `nbd/api.rs` | HTTP API |

## Performance

| Operation | Target | Notes |
|-----------|--------|-------|
| Write latency | <1ms | Local SSD |
| Read latency (cached) | <1ms | Local SSD |
| Read latency (cold) | 50-300ms | S3 fetch |
| ZFS snapshot | <100ms | Local fsync only |
| ZFS clone | <100ms | Metadata operation |

## Limits

| Limit | Value | Constraint |
|-------|-------|------------|
| Max device size | 16 EiB | 64-bit block indices |
| Block size | Configurable | Default 128KB |
| Local cache | Disk space | Sparse file grows as needed |
