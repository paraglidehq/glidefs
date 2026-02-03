# ZeroFS Architecture

A high-performance filesystem that makes Amazon S3 (or S3-compatible storage) the primary storage backend, providing file-level access via NFS/9P and block-level access via NBD.

## Data Flow

### Write Path

```
Client ──► NFS/9P/NBD Server ──► ZeroFS.write()
                                      │
                              ┌───────┴───────┐
                              │ Acquire lock  │
                              │ Check perms   │
                              │ Check quota   │
                              └───────┬───────┘
                                      │
                              ┌───────┴───────┐
                              │  Txn<Open>    │
                              │ ─────────────│
                              │ Split 32KB   │
                              │ Update inode │
                              └───────┬───────┘
                                      │
                              ┌───────┴───────┐
                              │ Txn<Sequenced>│
                              │ ─────────────│
                              │ Wait for seq │
                              └───────┬───────┘
                                      │
                              ┌───────┴───────┐
                              │   SlateDB     │
                              │ ─────────────│
                              │ Memtable     │
                              │ WAL → S3     │
                              │ SST → S3     │
                              └───────────────┘
```

### Read Path

```
Client ──► NFS/9P/NBD Server ──► ZeroFS.read()
                                      │
                              ┌───────┴───────┐
                              │ Get inode    │
                              │ Check perms  │
                              └───────┬───────┘
                                      │
                              ┌───────┴───────┐
                              │ ChunkStore   │
                              │ ─────────────│
                              │ Calc indices │
                              │ Scan for     │
                              │   chunks     │
                              └───────┬───────┘
                                      │
                      ┌───────────────┼───────────────┐
                      ▼               ▼               ▼
               Memory Cache     Disk Cache      S3 Backend
                (Foyer)        (configurable)   (decrypt +
                 ~1µs            ~10µs         decompress)
                                              50-300ms cold
```

### Lease Handoff (VM Migration)

```
Node A (Writer)                           Node B (New Writer)
      │                                         │
      ▼                                         │
PrepareHandoff(path)                            │
      │                                         │
      ├─ Block inode writes                     │
      ├─ Drain in-flight ops                    │
      ├─ Flush to durable                       │
      └─ State → Releasing                      │
      │                                         │
      ▼                                         │
CompleteHandoff(path)                           │
      │                                         │
      └─ State → Released ─────────────────────►│
                                                ▼
                                        AcquireLease(path)
                                                │
                                        ├─ Verify Released
                                        ├─ Atomic CAS on S3
                                        └─ State → Active
                                                │
                                                ▼
                                          Resume writes
                                         (<500ms total)
```

## Concepts & Terminology

| Term | Definition | NOT |
|------|------------|-----|
| Chunk | 32KB block of file data | Not a variable-size segment |
| Inode | Metadata for files/dirs/symlinks | Not the data itself |
| Lease | Per-path exclusive write lock | Not a global filesystem lock |
| SST | Sorted String Table (LSM output) | Not raw data in S3 |
| Tombstone | Marker for deleted file pending GC | Not immediate deletion |
| Sequence | Ordering number for write coordination | Not a timestamp |

## Core Mechanism

### Chunk-Based Storage

Files are split into fixed 32KB chunks for efficient S3 operations:

```rust
pub const CHUNK_SIZE: usize = 32 * 1024;  // fs/mod.rs:71

// Key layout for chunk storage
chunk_key(inode_id, chunk_index) → [0xFE][u64:inode][u64:index]

// Example: inode 42, chunk 0
[0xFE][0x0000002A][0x00000000] → <32KB data>
```

**Why 32KB?** Balances S3 API overhead (fixed per-request cost) against granularity for partial updates. Too small = excessive S3 requests. Too large = wasted bandwidth on small updates.

### Typestate Transactions

Compile-time enforcement of transaction lifecycle (`db.rs:83-99`):

```rust
// State 1: Open for mutations
pub struct Txn<Open> { batch: WriteBatch, ... }
    └─ put_bytes(), delete_bytes()
    └─ sequence() → Txn<Sequenced>

// State 2: Sequenced, ready to commit
pub struct Txn<Sequenced> { guard: SequenceGuard, ... }
    └─ wait_for_predecessors()
    └─ commit()
```

This prevents entire classes of bugs at compile time:
- Cannot mutate after sequencing
- Cannot commit without sequencing
- Cannot double-commit

### Key Prefix Ordering

LSM keys are prefixed for scan locality (`key_codec.rs:5-21`):

```
0x01-0x05: Hot metadata (co-located in SSTs)
  0x01 INODE      - Inode metadata
  0x02 DIR_ENTRY  - Directory entries
  0x03 DIR_SCAN   - Directory scan keys
  0x04 DIR_COOKIE - Readdir pagination
  0x05 STATS      - Statistics shards

0x06-0x07: Cold metadata
  0x06 SYSTEM     - Configuration (rarely accessed)
  0x07 TOMBSTONE  - GC-only scans

0xFE: Bulk data (isolated)
  0xFE CHUNK      - File data (dominates storage)
```

**Why this layout?** SlateDB stores each SST as a separate S3 object. Adjacent keys land in the same SST, so directory operations (`lookup`, `readdir`) touch fewer S3 objects.

## State Machines

### Transaction State

```
Txn<Open>
    │
    │ put_bytes() / delete_bytes()
    │
    └──► sequence()
            │
            ▼
      Txn<Sequenced>
            │
            │ wait_for_predecessors()
            │
            └──► commit()
                    │
                    ▼
               [Committed]

(drop without commit → [Abandoned])
```

### Lease State

```
[No Lease] ─── acquire() ───► Active
                                 │
                                 │ renew() every 10s
                                 │ (expires in 30s)
                                 │
                                 └──► prepare_handoff()
                                           │
                                           ▼
                                       Releasing
                                           │
                                           │ drain + flush
                                           │
                                           └──► complete_handoff()
                                                      │
                                                      ▼
                                                  Released
                                                      │
                                        [Available for new acquire()]
```

| From | Event | To | Guard |
|------|-------|-----|-------|
| None | acquire() | Active | No existing lease or expired |
| Active | timeout | Expired | elapsed > 30s without renewal |
| Active | prepare_handoff() | Releasing | holder matches |
| Releasing | complete_handoff() | Released | holder matches |
| Released | acquire() | Active | S3 conditional write succeeds |

## Design Decisions

### Why Per-Path Leases Over Global Lock?

We use per-path leases (`/.zerofs_leases/{path_hash}.json`) instead of a global writer lock:

1. **Independent VM migration** - Migrate one VM while others keep running
2. **Faster handoff** - Only block affected path (<500ms vs seconds)
3. **No coordination service** - S3 conditional writes provide atomicity
4. **Fault isolation** - One stuck handoff doesn't block the system

We considered Raft/Paxos but rejected it:
- Requires separate coordination service
- Overkill for single-writer-per-path semantics
- S3 already provides linearizable conditional writes

### Why S3 Conditional Writes for Leases?

Lease atomicity via S3 `If-Match` (ETag versioning) (`lease.rs:25-27`):

```rust
PutMode::Update(UpdateVersion { e_tag, ... })
```

1. **No consensus service needed** - S3 is the coordinator
2. **Works with any S3-compatible store** - MinIO, R2, GCS, Azure
3. **Partition tolerant** - S3 outage = lease unavailable (safe)
4. **Simple mental model** - CAS semantics engineers understand

### Why Typestate Over Runtime Checks?

```rust
// Compile-time: impossible to call commit() before sequence()
Txn<Open> → sequence() → Txn<Sequenced> → commit()

// Runtime alternative (rejected):
txn.commit()  // panics if not sequenced - bug found at runtime
```

Benefits:
- **Zero runtime overhead** - No checks in hot path
- **Bugs found at compile time** - Not in production
- **Self-documenting API** - Types encode valid transitions

### Why Chunk Keys at 0xFE (End of Keyspace)?

Chunks dominate storage (>95% by volume) but are rarely scanned with metadata:

```
lookup("/foo/bar")  →  scans 0x01-0x04 (metadata SSTs)
read_file(inode=42) →  scans 0xFE (chunk SSTs)
```

Separating chunks prevents metadata scans from downloading chunk-heavy SSTs. This can reduce S3 GETs by 10-100x for directory-heavy workloads.

### Why 9P Over FUSE?

We provide 9P (and NFS) instead of FUSE:

1. **No custom kernel modules** - Works on any OS immediately
2. **Battle-tested clients** - Linux kernel 9P client is mature
3. **Network-first design** - Handles latency, retries, disconnects
4. **Better fsync semantics** - 9P correctly signals durability

FUSE would require writing both filesystem logic AND handling S3's network characteristics in the kernel driver.

## Package Structure

| File | Purpose |
|------|---------|
| `fs/mod.rs` | Main ZeroFS struct, POSIX operations |
| `fs/store/chunk.rs` | File data chunking, 32KB blocks |
| `fs/store/inode.rs` | Inode allocation and persistence |
| `fs/store/directory.rs` | Directory entries, readdir |
| `fs/store/tombstone.rs` | Deleted file tracking for GC |
| `fs/lease.rs` | Per-path writer coordination |
| `fs/write_coordinator.rs` | Sequential write ordering |
| `fs/flush_coordinator.rs` | Coordinating flush operations |
| `fs/gc.rs` | Background garbage collection |
| `fs/key_codec.rs` | LSM key encoding/decoding |
| `fs/inode.rs` | Inode data structures |
| `db.rs` | SlateDB wrapper, typestate transactions |
| `nfs.rs` | NFS protocol adapter |
| `ninep/` | 9P protocol implementation |
| `nbd/` | NBD block device server |
| `rpc/server.rs` | gRPC admin API |
| `block_transformer.rs` | Compression + encryption pipeline |
| `cache.rs` | Foyer memory cache |

## Configuration

| Variable | Default | Rationale |
|----------|---------|-----------|
| `cache.disk_size_gb` | 10.0 | Balances local storage vs S3 latency |
| `cache.memory_size_gb` | 0.25 | Memory cache for hot metadata |
| `filesystem.max_size_gb` | 16 EiB | Effectively unlimited by default |
| `filesystem.compression` | `lz4` | Fast compression, good ratio |
| `lease.duration_secs` | 30 | Room for network jitter + renewal |
| `lease.renewal_interval_secs` | 10 | 3x attempts before expiry |

## Security Model

### What We Verify

- Password-derived key via Argon2id (128-bit salt, 256-bit key)
- XChaCha20-Poly1305 authenticated encryption on all data
- POSIX permission checks (uid/gid/mode) on every operation

### What We Do NOT Verify

- **Key structure is unencrypted** - Inode IDs, filenames visible in LSM
- **No access control on S3** - Anyone with bucket access sees encrypted blobs
- **Client identity** - NFS/9P client claims uid/gid (no authentication)

### Why This Is Acceptable

- Encrypting keys would break LSM sorting (severe performance impact)
- S3 bucket policies provide coarse access control
- For filename privacy, layer gocryptfs on top
- Production deployments should use network isolation

## Failure Modes

| Failure | Detection | Recovery |
|---------|-----------|----------|
| S3 unavailable | Request timeout | Retry with backoff, fail after threshold |
| Lease holder crash | Lease expires (30s) | New node acquires after expiry |
| Mid-write crash | WAL survives in S3 | SlateDB replays WAL on restart |
| Orphaned chunks | GC scans tombstones | Background deletion over time |
| Quota exceeded | Size check before write | Return ENOSPC, allow deletes |

## Performance Characteristics

| Operation | Latency | Notes |
|-----------|---------|-------|
| Memory cache hit | ~1 µs | Foyer in-memory cache |
| Disk cache hit | ~10 µs | Local SSD cache |
| S3 cold read | 50-300 ms | Download + decrypt + decompress |
| Sequential write | ~19 µs/op | SQLite fillseq benchmark |
| Random read | ~0.9 µs/op | SQLite readseq (cached) |
| PostgreSQL TPS | 53K write, 413K read | pgbench with L2ARC |

## Limits

| Limit | Value | Constraint |
|-------|-------|------------|
| Max file size | 16 EiB | 64-bit chunk indices × 32KB |
| Max files | 2^64 | 64-bit inode IDs |
| Max hardlinks | ~4 billion | 32-bit nlink counter |
| Max filename | 255 bytes | POSIX NAME_MAX |
| Chunk size | 32 KB | Fixed, not configurable |
