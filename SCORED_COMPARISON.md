# Glide v2 vs. ZeroFS — Scored Comparison

## Rating Scale

**1-3**: Poor / fundamentally unsuited
**4-5**: Adequate / works but not ideal
**6-7**: Good / solid design for this dimension
**8-9**: Excellent / clearly strong
**10**: Best-in-class / hard to improve

---

## Summary Table

| Dimension | Glide v2 | ZeroFS | Notes |
|-----------|:--------:|:------:|-------|
| Scalability to 1M VMs | **9** | **4** | Glide's O(1) hot path and event-driven GC scale horizontally. ZeroFS's per-VM LSM compaction generates fleet-wide S3 pressure. |
| Glide's use case (ephemeral microVM platform) | **10** | **4** | Glide is purpose-built for this. ZeroFS is a general-purpose filesystem bolted onto VMs. |
| Multi-node coordination | **8** | **6** | Glide uses S3 leases (no external service). ZeroFS relies on S3 as shared backend, but has no coordination primitive for concurrent writers. |
| Cross-host fork / portability | **10** | **3** | Glide's killer feature: copy manifest, boot anywhere. ZeroFS has no fork primitive; you'd need to copy or restructure data. |
| Storage efficiency / dedup | **10** | **2** | Content-addressing deduplicates globally across all tenants. ZeroFS stores one full copy per VM. |
| Durability / data safety | **6** | **9** | Glide accepts host death = data loss (bounded by flush mode). ZeroFS writes everything to S3; host death loses only the memtable. |
| Boot / wake latency | **9** | **6** | Same-host wake: milliseconds (local cache warm). ZeroFS always reads through S3-backed LSM. |
| Write performance (throughput) | **9** | **7** | Glide acks after local WAL (~20us). ZeroFS acks after memtable (fast), but compaction contends with S3. |
| Read performance (cache hot) | **9** | **7** | Glide: ~100ns memory / ~100us SSD. ZeroFS: memtable + block cache, but LSM multi-level lookup adds overhead. |
| Read performance (cache cold) | **7** | **6** | Both hit S3. Glide prefetches 25 blocks per pack miss. ZeroFS fetches SST files (may contain irrelevant keys). |
| S3 cost efficiency | **10** | **4** | Demand-driven mode: zero S3 ops during active operation. ZeroFS: every write generates eventual S3 PUTs via memtable flush + compaction. |
| Operational simplicity | **7** | **7** | Glide: custom GC, WAL, flush scheduling to manage. ZeroFS: SlateDB handles compaction/recovery, but standalone compactor + 3 protocol frontends add surface area. |
| Encryption / security posture | **5** | **9** | Glide: SSE-KMS (trust AWS). ZeroFS: client-side XChaCha20-Poly1305 (zero-knowledge, S3 provider sees nothing). |
| Protocol versatility | **4** | **9** | Glide: NBD only (block device). ZeroFS: NFS + 9P + NBD + full POSIX (8,662 pjdfstest passing). |
| Implementation risk | **6** | **7** | Glide: building custom storage engine is high-risk/high-reward. ZeroFS: standing on SlateDB, but POSIX compliance is its own risk surface. |
| Garbage collection at scale | **9** | **7** | Glide: event-driven refcounts O(1) per flush, monthly reconciliation as safety net. ZeroFS: LSM compaction is automatic but generates ongoing S3 I/O proportional to write volume. |
| Crash recovery | **7** | **8** | Glide: WAL replay (local) or S3 manifest fallback (cross-host, data loss possible). ZeroFS: read S3 manifest, always recoverable, no data loss. |

---

## Detailed Analysis

### 1. Scalability to 1M VMs — Glide: 9 / ZeroFS: 4

**Glide** is designed from the ground up for fleet-scale operation:
- Hot path is O(1) per read/write with no fleet-size dependency
- Flush path is O(D) where D is dirty count, not block map size or fleet size
- Event-driven GC refcounts: O(1) per flush event, O(P) per fork/delete (per-VM, not per-fleet)
- `manifest_packs` table at 1M VMs x 3,200 packs = 3.2B rows — PostgreSQL handles this with partitioning
- Monthly reconciliation: ~60 minutes at 1M VMs, incremental at 10M+
- Each host is independent — adding hosts scales linearly

**ZeroFS** faces compounding pressure at scale:
- Each VM runs its own LSM tree, generating compaction I/O proportional to write volume
- 1M VMs each doing periodic compaction = massive aggregate S3 PUT/GET load
- No dedup means storage costs scale linearly with VM count (not unique data)
- LSM compaction is workload-dependent and hard to bound across a fleet
- No fleet-level GC coordination — each instance manages its own SST lifecycle

The fundamental issue: ZeroFS's S3 I/O is proportional to VM uptime and write volume. At 1M VMs, the aggregate S3 operation count becomes a cost and throughput concern. Glide's S3 I/O is proportional to cross-host transitions, which is a dramatically smaller number.

### 2. Glide's Use Case (Ephemeral MicroVM Platform) — Glide: 10 / ZeroFS: 4

This is the dimension that matters most for Paraglide. The workload is:
- Fork from production, install deps, build, run tests, delete
- 90%+ of VMs are ephemeral (previews, dev environments, promotion builds)
- VMs share enormous amounts of data (same OS, runtime, packages)
- Fast boot and sleep/wake are user-facing latency

**Glide** was designed for exactly this:
- Fork = copy manifest. Zero data transfer. Works cross-host.
- Demand-driven flush means preview VMs that live and die on one host generate *zero* S3 operations
- Content-addressing deduplicates everything — 1,000 Next.js tenants share one copy of Ubuntu + Node
- Same-host sleep/wake is milliseconds (cache warm, no S3)
- "S3 traffic proportional to cross-host transitions, not VM uptime" — the core insight

**ZeroFS** would work but fights the workload:
- No fork primitive — creating a preview requires copying or restructuring data
- No dedup — 1,000 tenants = 1,000 copies of base OS + runtime
- Every VM generates ongoing S3 I/O even for ephemeral workloads
- POSIX compliance is unnecessary overhead when the guest provides its own filesystem
- The system pays for generality it doesn't use

### 3. Multi-Node Coordination — Glide: 8 / ZeroFS: 6

**Glide** uses S3-based leases with conditional PUTs:
- No external coordination service (no etcd, Zookeeper, Consul)
- Generation-counter fencing prevents stale writes
- Single-writer per export enforced by lease
- Migration protocol: drain → transfer lease → promote (100-500ms downtime)
- Defense-in-depth: batch writes also use conditional PUT with ETag

**ZeroFS** relies on S3 as shared backend:
- Any node can read from S3 (implicit cross-host access)
- No explicit coordination primitive for concurrent writers
- SlateDB's WAL and manifest provide crash-consistent state
- But multi-writer scenarios (migration, failover) require external coordination not provided by the system

Glide's lease protocol is well-specified and sufficient for the single-writer-per-VM model. ZeroFS's implicit S3 sharing works for read access but leaves multi-writer coordination as an exercise for the operator.

### 4. Cross-Host Fork / Portability — Glide: 10 / ZeroFS: 3

**Glide's crown jewel.** Forking a 100GB VM:
1. Source host flushes dirty blocks (tiny for steady-state production)
2. Control plane copies manifest in S3 (~1MB)
3. Destination host loads manifest, boots VM
4. Zero data copied. Blocks shared by reference (same hashes)
5. Storage cost: proportional to unique writes, not total disk size

Two fork modes:
- **Consistent fork** (default): source flushes first, exact point-in-time copy, source never paused
- **Lazy fork**: copy S3 manifest as-is, zero source coordination, slightly stale

**ZeroFS** has no fork concept:
- You could start a new instance reading the same S3 prefix, but that gives shared reads, not independent writes
- Independent fork would require copying all SST files or implementing layered reads — neither is built in
- SlateDB checkpoints are for rollback, not cross-host cloning
- A 100GB fork would require significant data copying or architectural additions

This alone is a disqualifying gap for the Paraglide use case, where fork is the most common operation.

### 5. Storage Efficiency / Dedup — Glide: 10 / ZeroFS: 2

**Glide** deduplicates globally and automatically:
- BLAKE3-128 content-addressing: same content = same hash = stored once
- Works across all tenants, all VMs, all base images
- 1,000 tenants running Next.js: ~2.7GB shared vs. ~3TB of per-tenant copies (15x reduction)
- Host-level pack index dedup: 80-90% skip rate on uploads for similar VMs on same host
- LZ4 compression: additional 1.5-2x reduction on S3 storage

**ZeroFS** has no deduplication:
- Each VM's LSM tree stores its own copy of everything
- 1,000 tenants = 1,000 copies of the base OS, runtime, and common packages
- No content-addressing, no hash-based dedup, no shared block references

At fleet scale, this is a massive cost difference. Glide's marginal storage cost per additional similar tenant converges to their unique source code (~50-200MB). ZeroFS's marginal cost is the full disk image (~10-100GB).

### 6. Durability / Data Safety — Glide: 6 / ZeroFS: 9

**ZeroFS** prioritizes durability:
- S3 is the primary store — data reaches S3 through memtable flush
- Host death loses only the in-memory memtable (small, recent writes)
- Automatic crash recovery: read manifest from S3, resume
- Client-side encryption means data is safe even if S3 is compromised

**Glide** explicitly trades durability for performance:
- **Demand-driven mode**: host death = data loss back to last S3 checkpoint (could be hours)
- **Continuous mode**: host death loses at most ~5 seconds of writes
- Acceptable for ephemeral VMs (fork from known state, recreate)
- Production VMs use continuous flush to bound the loss window
- Synchronous S3 writes available at cost of latency for VMs that need zero loss

Glide's durability model is *correct for the workload* — ephemeral VMs don't need strong durability, and production VMs bound the window. But ZeroFS's "never lose data" model is strictly stronger.

### 7. Boot / Wake Latency — Glide: 9 / ZeroFS: 6

**Glide** optimizes aggressively for boot:
- Same-host wake: load block map from local file. Milliseconds. Cache warm. Zero S3.
- Cross-host wake: GET manifest from S3 (one request), lazy block pull, boot hot set prefetch
- Boot hot set: `glidefs bless` records which blocks are accessed during first 10 seconds, prefetches them
- Sequential read-ahead hides S3 latency for consecutive access patterns
- Pack-level prefetch: cache miss warms 25 blocks at once

**ZeroFS** boots through the LSM read path:
- Every read resolves through the LSM tree (memtable → L0 → L1 → ...)
- Block cache helps for repeated reads, but initial boot is cold
- No boot-specific prefetch optimization
- S3 latency on every cache miss, with SST-level granularity (may fetch unnecessary data)

For a platform where "time to first instruction" is user-facing latency, Glide's purpose-built boot optimizations make a material difference.

### 8. Write Performance (Throughput) — Glide: 9 / ZeroFS: 7

**Glide** write path:
1. BLAKE3 hash (~5us)
2. WAL append (~20us, sequential SSD)
3. Memory + SSD cache insert
4. Block map update (O(1))
5. Ack to VM (~20us total)
6. S3 upload happens later (asynchronous, off critical path)

**ZeroFS** write path:
1. Write to memtable (fast, in-memory)
2. Ack to client
3. Memtable flushes to SST in S3 (async)
4. Compaction runs across levels (async, but contends for S3 bandwidth)

Both ack fast (local memory). The difference is what happens after:
- Glide: nothing touches S3 until a cross-host transition in demand-driven mode
- ZeroFS: memtable flush generates periodic S3 PUTs, compaction generates ongoing S3 I/O
- Under heavy writes (build VMs doing npm install), ZeroFS's compaction backlog can create S3 contention that affects read latency

### 9. Read Performance (Cache Hot) — Glide: 9 / ZeroFS: 7

**Glide**: ~100ns (memory cache) or ~100us (SSD cache). Direct hash lookup. Content-addressed blocks are immutable — no coherence protocol needed.

**ZeroFS**: LSM read path checks memtable, then L0, L1, ... through block cache. Bloom filters reduce unnecessary reads, but multi-level lookup adds baseline overhead vs. a single hash lookup.

### 10. Read Performance (Cache Cold) — Glide: 7 / ZeroFS: 6

Both hit S3 on cache miss. Differences:

**Glide**: Fetches the entire pack (~3.2MB, 25 blocks). Temporal locality — blocks written in the same flush cycle are often read together. One cache miss warms 25 entries. Sequential read-ahead prefetches next pack proactively.

**ZeroFS**: Fetches SST file regions. SSTs are sorted by key, so sequential block reads map well, but the SST may contain keys from other block ranges (less efficient prefetch). Compaction can fragment related blocks across SST levels.

### 11. S3 Cost Efficiency — Glide: 10 / ZeroFS: 4

**Glide** (typical tenant, typical day: 3 prod VMs, 20 previews, 5 promotions):
- Monthly S3 operations: ~3,750 PUTs + ~15,600 GETs = **$0.03/month**
- At 10x scale: **$0.30/month**
- Preview VMs that never cross hosts: **zero S3 operations**
- Storage: content-addressing deduplicates aggressively

**ZeroFS** (same workload):
- Every VM generates ongoing S3 I/O from memtable flushes + compaction
- 23 VMs x continuous writes x memtable flush every few seconds = thousands of PUTs/day
- Compaction amplifies: each level rewrites data, multiplying S3 operations
- No dedup: storage cost scales linearly with VM count
- Rough estimate: 10-100x higher S3 operation costs for the same workload

The "S3 traffic proportional to cross-host transitions, not VM uptime" insight is Glide's economic moat.

### 12. Operational Simplicity — Glide: 7 / ZeroFS: 7

**Glide** complexity:
- Custom GC system (event-driven refcounts + monthly reconciliation)
- WAL management and crash recovery
- Flush scheduling (demand-driven vs. continuous, adaptive intervals)
- Pack index management (host-level shared DashMap, rebuild on lifecycle events)
- Block map persistence and overlay flattening
- But: no ZFS to tune, no distributed filesystem, no QoS code (cgroup v2 handles it)

**ZeroFS** complexity:
- SlateDB handles compaction, crash recovery, manifest management (less custom code)
- But: standalone compactor process to operate at scale
- Three protocol frontends (NFS, 9P, NBD) — large surface area
- Full POSIX compliance to maintain (8,662 tests)
- LSM tuning: memtable size, compaction strategy, level multipliers, cache sizing
- Client-side encryption key management

Both systems have meaningful operational surface area. Glide's is concentrated in custom storage mechanics; ZeroFS's is spread across protocol frontends and POSIX semantics.

### 13. Encryption / Security Posture — Glide: 5 / ZeroFS: 9

**ZeroFS** implements zero-knowledge encryption:
- Client-side XChaCha20-Poly1305 before data reaches S3
- HKDF-derived per-object keys
- S3 provider (including AWS) never sees plaintext
- Protects against: bucket access compromise, cloud provider insider threat, regulatory data exposure

**Glide** delegates to AWS:
- SSE-KMS with shared platform key
- AWS sees plaintext during encrypt/decrypt
- Simpler: zero crypto code in the daemon
- Per-tenant KMS keys possible as future opt-in (breaks cross-tenant dedup)
- Sufficient for SOC2; insufficient for zero-trust or customer-managed key requirements

ZeroFS's security posture is strictly stronger. For a developer platform, Glide's approach is pragmatically sufficient, but ZeroFS demonstrates a higher ceiling.

### 14. Protocol Versatility — Glide: 4 / ZeroFS: 9

**ZeroFS**: NFS + 9P + NBD + full POSIX compliance (8,662 pjdfstest passing). Can serve as a general-purpose cloud filesystem for any workload — databases, dev environments, file sharing, container storage.

**Glide**: NBD only. Block device for VM root disks. That's it.

For Paraglide's use case, this dimension is irrelevant — the guest OS provides the filesystem. But ZeroFS's versatility is genuinely impressive for a broader set of use cases.

### 15. Implementation Risk — Glide: 6 / ZeroFS: 7

**Glide** is building a custom storage engine:
- Content-addressed block store with custom WAL, GC, flush scheduling
- Snapshot mechanism with sequence numbers and concurrent write safety
- Pack file management with host-level dedup and index rebuilds
- Each of these is a system that must be correct under concurrent operations
- Mitigated by: Rust's type system, typestate pattern, loom testing, smaller scope (block-level only)

**ZeroFS** stands on SlateDB:
- Gets compaction, crash recovery, write buffering, manifest management for free
- But: implementing a correct concurrent POSIX filesystem is notoriously hard
- Three protocol frontends multiply the correctness surface area
- Client-side encryption adds complexity to every I/O path
- Mitigated by: well-tested LSM foundation, comprehensive test suite (8,662 POSIX tests)

Glide's risk is concentrated (custom storage engine). ZeroFS's risk is distributed (POSIX correctness across three protocols). Both are manageable; ZeroFS has a slight edge from standing on more proven foundations.

### 16. Garbage Collection at Scale — Glide: 9 / ZeroFS: 7

**Glide**:
- Event-driven refcounts: O(1) per flush, O(P) per fork/delete
- Transactional with manifest updates — no drift
- Pack deletion: background worker processes refcount=0 after 24h grace
- Monthly reconciliation: safety net, not routine operation
- At 1M VMs: ~10K refcount updates/s (PostgreSQL handles easily)
- Nothing in steady-state GC scales with fleet size

**ZeroFS**:
- LSM compaction is automatic (SlateDB handles it)
- But compaction generates S3 I/O proportional to write volume x amplification factor
- At fleet scale, aggregate compaction I/O becomes significant
- No pack-level sharing means each VM's compaction is independent (no dedup savings)
- Compaction backlog under heavy writes can affect read latency

Glide's GC is more complex to build but scales better. ZeroFS's compaction is simpler but generates unbounded S3 I/O at scale.

### 17. Crash Recovery — Glide: 7 / ZeroFS: 8

**ZeroFS**: Read manifest from S3, resume. Always recoverable. No data loss beyond the memtable. Simple and reliable.

**Glide**:
- Local recovery (daemon restart): WAL replay + local block map. Milliseconds. No data loss.
- Cross-host recovery (host death): fall back to last S3 manifest. Data loss bounded by flush mode.
- Continuous mode: ~5s loss window. Demand-driven: loss back to last checkpoint.

Both are well-designed. ZeroFS's "S3 is always the truth" model is simpler and strictly safer. Glide's local recovery is faster, but cross-host recovery involves data loss by design.

---

## Weighted Score (for Paraglide's Use Case)

Not all dimensions matter equally. Weights reflect Paraglide's priorities: ephemeral microVM platform at scale.

| Dimension | Weight | Glide v2 | ZeroFS | Glide Weighted | ZeroFS Weighted |
|-----------|:------:|:--------:|:------:|:--------------:|:---------------:|
| Scalability to 1M VMs | 3x | 9 | 4 | 27 | 12 |
| Glide's use case (ephemeral microVM) | 3x | 10 | 4 | 30 | 12 |
| Cross-host fork / portability | 3x | 10 | 3 | 30 | 9 |
| Storage efficiency / dedup | 2x | 10 | 2 | 20 | 4 |
| S3 cost efficiency | 2x | 10 | 4 | 20 | 8 |
| Boot / wake latency | 2x | 9 | 6 | 18 | 12 |
| Write performance | 2x | 9 | 7 | 18 | 14 |
| Multi-node coordination | 1x | 8 | 6 | 8 | 6 |
| Read performance (hot) | 1x | 9 | 7 | 9 | 7 |
| Read performance (cold) | 1x | 7 | 6 | 7 | 6 |
| Operational simplicity | 1x | 7 | 7 | 7 | 7 |
| Durability / data safety | 1x | 6 | 9 | 6 | 9 |
| Crash recovery | 1x | 7 | 8 | 7 | 8 |
| GC at scale | 1x | 9 | 7 | 9 | 7 |
| Implementation risk | 1x | 6 | 7 | 6 | 7 |
| Encryption | 0.5x | 5 | 9 | 2.5 | 4.5 |
| Protocol versatility | 0x | 4 | 9 | 0 | 0 |
| **Total** | | | | **224.5** | **125.5** |

**Glide v2: 224.5 / ZeroFS: 125.5** (weighted for Paraglide's use case)

---

## Bottom Line

**Glide v2 is purpose-built for Paraglide's workload and it shows.** The architecture makes the right tradeoffs at every layer — accepting bounded data loss for ephemeral VMs in exchange for zero S3 traffic, building content-addressing into the foundation for free dedup and cheap fork, and designing every algorithm to be O(per-VM) rather than O(per-fleet).

**ZeroFS is a strong general-purpose system** with best-in-class encryption, full POSIX compliance, and conservative durability. If you needed a cloud filesystem for diverse workloads, it would be compelling. But for a platform where the dominant operation is "fork a VM, do work, delete it," ZeroFS pays for generality it doesn't use and lacks the primitives (fork, dedup) that define the use case.

**The gap is structural, not incidental.** ZeroFS can't close the fork/dedup gap without fundamental architectural changes (adding content-addressing, manifest-based cloning). These aren't features you bolt on — they're the foundation Glide is built on. Conversely, Glide could adopt client-side encryption or add protocol frontends as incremental additions, without rearchitecting the core.

**Where ZeroFS is genuinely better:** encryption posture (zero-knowledge vs. trust-AWS), durability guarantees (no data loss vs. bounded loss), and protocol versatility (NFS/9P/NBD vs. NBD-only). These matter in different use cases — but for Paraglide's ephemeral microVM platform, they're secondary to the dimensions where Glide dominates.
