# ZeroFS Architecture

ZeroFS is a high-performance NBD (Network Block Device) server backed by S3-compatible object storage. It enables microVM storage with scale-to-zero and live migration capabilities.

## Design Philosophy

- **Performance is a feature**: Minimize allocations, do less work, measure before optimizing
- **Idempotent operations**: Safe to retry after failures
- **Atomic state transitions**: Either fully succeed or leave system in recoverable state
- **S3 as source of truth**: Local cache is ephemeral, S3 is durable

## Deployment Model

```
Compute Node (ONE zerofs process):
  zerofs daemon
    ├── Export "vm-001" → /dev/nbd0 → MicroVM 1
    │     ├── S3: s3://bucket/path/vm-001/
    │     └── Cache: /var/cache/zerofs/<bucket-id>/vm-001/
    │
    ├── Export "vm-002" → /dev/nbd1 → MicroVM 2
    │     ├── S3: s3://bucket/path/vm-002/
    │     └── Cache: /var/cache/zerofs/<bucket-id>/vm-002/
    │
    └── Export "vm-003" → /dev/nbd2 → MicroVM 3
          ├── S3: s3://bucket/path/vm-003/
          └── Cache: /var/cache/zerofs/<bucket-id>/vm-003/
```

**Key invariant**: One zerofs daemon per compute node, multiple exports per daemon (one per microVM).

## Package Structure

```
src/
├── cli/
│   ├── mod.rs          # CLI entry point
│   ├── server.rs       # Server startup and signal handling
│   └── password.rs     # Password validation
├── nbd/
│   ├── mod.rs          # Module exports
│   ├── server.rs       # NBD protocol server (TCP/Unix socket)
│   ├── router.rs       # Multi-tenant export router
│   ├── api.rs          # HTTP API for dynamic export management
│   ├── handler.rs      # NBD command handler (read/write/flush/trim)
│   ├── block_store.rs  # S3 block storage backend
│   ├── write_cache.rs  # Write-behind cache with typestate
│   ├── state.rs        # Device state machines
│   ├── protocol.rs     # NBD protocol types
│   └── error.rs        # Error types
├── block_transformer.rs # Encryption + compression pipeline
├── config.rs           # Configuration parsing
├── key_management.rs   # Encryption key derivation
└── ...
```

## Core Components

### ExportRouter (`nbd/router.rs`)

Central coordinator for multi-tenant operation. Manages the lifecycle of all exports.

```rust
pub struct ExportRouter {
    exports: RwLock<HashMap<String, ExportState>>,
    object_store: Arc<dyn ObjectStore>,
    db_path: String,
    cache_dir: PathBuf,
    block_size: usize,
    transformer: Option<Arc<ZeroFsBlockTransformer>>,
}
```

**Responsibilities**:
- Create/remove exports dynamically
- Route NBD connections to correct handler
- Coordinate drain operations for migration
- Handle graceful shutdown

### NBDServer (`nbd/server.rs`)

Handles NBD protocol negotiation and session management. Supports both TCP and Unix socket transports.

```rust
pub struct NBDServer {
    router: Arc<ExportRouter>,
    transport: Transport,  // TCP or Unix
}
```

**Protocol flow**:
1. Client connects, negotiates NBD protocol
2. Client requests export by name (`NBD_OPT_GO`)
3. Server looks up handler via router
4. Transmission phase: read/write/flush/trim commands

### WriteCache (`nbd/write_cache.rs`)

Write-behind cache with typestate pattern for lifecycle safety and **read-through** support.

```rust
WriteCache<Initializing> → WriteCache<Recovering> → WriteCache<Active> → WriteCache<Draining>
```

**States**:
- `Initializing`: Loading metadata, not accepting I/O
- `Recovering`: Replaying uncommitted writes from cache
- `Active`: Normal operation, accepting reads/writes
- `Draining`: Flushing all dirty blocks to S3

**Key guarantees**:
- `FLUSH` = fsync to local SSD (not S3 round-trip)
- Background sync pushes dirty blocks to S3
- Drain blocks until all dirty blocks synced
- **Read-through**: Blocks not present locally are fetched from S3 on demand

**Block presence tracking**:
- `present_blocks` bitmap tracks which blocks exist locally
- Fresh cache on new node: all blocks marked NOT present
- On read: if block not present, fetch from S3 and cache locally
- On write: mark block as present and dirty

### BlockState (`nbd/state.rs`)

Per-block state machine for cache coherency.

```
Clean → Dirty → Syncing → Clean
          ↑        │
          └────────┘  (retry on S3 failure)
```

### S3BlockStore (`nbd/block_store.rs`)

Direct block-to-S3 mapping. Each block is one S3 object.

```
s3://bucket/path/{export_name}/blocks/{block_id}
```

**No LSM tree** - direct addressing for predictable latency.

## HTTP API (`nbd/api.rs`)

REST API for orchestrator integration.

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/exports` | GET | List all exports |
| `/api/exports` | POST | Create new export |
| `/api/exports/{name}` | GET | Get export info |
| `/api/exports/{name}` | DELETE | Remove export |
| `/api/exports/{name}/drain` | POST | Drain to S3 |
| `/api/exports/{name}/promote` | POST | Readonly → read-write |
| `/health` | GET | Health check |

## Operational Scenarios

### Scale to Zero

```
1. VM stops (graceful or timeout)
2. Orchestrator: POST /api/exports/vm-001/drain
3. zerofs drains dirty blocks to S3 (blocking)
4. Orchestrator: DELETE /api/exports/vm-001
5. zerofs releases export (cache preserved for fast re-attach)

Result: vm-001 has zero local resources, all data in S3
        Other VMs unaffected
```

### Wake Anywhere

```
1. Orchestrator: POST nodeB:8080/api/exports
   { "name": "vm-001", "size_gb": 50 }
2. zerofs creates export, loads existing cache if present
3. nbd-client nodeB /dev/nbd0 -N vm-001
4. VM starts, reads pull from S3 on demand

First read: 50-300ms (S3 fetch)
Subsequent: <1ms (local cache)
```

### Live Migration

```
Node A                              Node B
──────                              ──────
VM running

1. POST B/api/exports
   { "name": "vm-001", "readonly": true }
                                    2. Creates read-only export
                                       Pre-warms cache from S3

3. POST A/api/exports/vm-001/drain
   Drains dirty blocks (blocking)

4. Pause VM

5. DELETE A/api/exports/vm-001

                                    6. POST B/api/exports/vm-001/promote
                                       readonly → read-write

                                    7. Resume VM

Downtime: Steps 4-7 (~100-500ms)
```

### Graceful Shutdown

```
SIGTERM/SIGINT:
1. Cancel all servers
2. Wait for connections to close
3. Drain all exports to S3
4. Exit

SIGUSR1 (node maintenance):
1. Drain all exports to S3
2. Continue running (for planned maintenance)
```

## Data Flow

### Write Path

```
Guest write → NBD handler → WriteCache (local SSD)
                               ├── Mark block present
                               ├── Mark block dirty
                               └── (background) → S3BlockStore → S3
```

### Read Path (Read-Through Caching)

```
Guest read → NBD handler → WriteCache.read_with_fetch()
                             │
                             ├── Block present locally?
                             │     └── Yes → Read from local SSD → Return
                             │
                             └── No → S3BlockStore.read_block()
                                        │
                                        ├── Block in S3 → Fetch, cache locally, mark present
                                        │
                                        └── Not in S3 → Return zeros (never-written block)
```

This enables **wake anywhere**: VMs can start on any node and read their data on-demand from S3.

### Flush Path

```
Guest FLUSH → NBD handler → WriteCache.flush()
                               ↓
                           fsync(local_ssd)
                               ↓
                           Return success

(S3 sync happens asynchronously in background)
```

### Background Sync Worker

The sync worker continuously drains dirty blocks to S3, keeping the dirty set small:

```
loop {
    dirty_blocks = cache.claim_dirty_blocks(batch_size)

    if dirty_blocks.is_empty() {
        sleep(100ms)
        continue
    }

    // Parallel uploads with bounded concurrency
    upload_parallel(dirty_blocks, max_concurrent=16)
}
```

**Key properties**:
- **Continuous drain**: No fixed interval, syncs as fast as S3 allows
- **Parallel uploads**: Up to 16 concurrent S3 PUTs for throughput
- **Small dirty set**: Reduces data-at-risk and speeds up drain operations
- **Configurable**: `SyncWorkerConfig` controls batch size, concurrency, idle sleep

**Configuration defaults**:
```rust
SyncWorkerConfig {
    batch_size: 100,              // Blocks per batch
    max_concurrent_uploads: 16,    // Parallel S3 PUTs
    idle_sleep: 100ms,            // Sleep when no dirty blocks
    metadata_save_interval: 10,    // Save metadata every N batches
}
```

## Configuration

```toml
[storage]
url = "s3://bucket/path"
encryption_password = "..."

[cache]
dir = "/var/cache/zerofs"

[servers.nbd]
addresses = ["0.0.0.0:10809"]
unix_socket = "/run/zerofs/nbd.sock"
api_address = "127.0.0.1:8080"
block_size = 131072  # 128KB

[[servers.nbd.exports]]
name = "vm-001"
size_gb = 50.0

[[servers.nbd.exports]]
name = "vm-002"
size_gb = 100.0
s3_prefix = "custom-prefix"  # Optional, defaults to name
```

## Security

- **Encryption**: ChaCha20-Poly1305 with per-block nonces
- **Key derivation**: Argon2 from password, stored encrypted in S3
- **Compression**: LZ4 (fast) or Zstd (ratio) applied before encryption
- **API**: Bind to localhost only in production, use firewall rules

## Coordination

**What zerofs handles**:
- Per-export drain/sync
- Readonly mode for safe pre-warming
- Graceful shutdown

**What the orchestrator must handle**:
- Ensuring single-writer invariant (only one node writes to an export)
- Coordinating migration sequence
- Tracking which node serves which export

If two nodes write to the same export simultaneously → data corruption. This is an orchestrator bug, not a zerofs bug.
