# Glide v2 vs. ZeroFS — Design Comparison

## Fundamental Architectural Difference

**Glide v2** is a **custom content-addressed block store** — a bespoke NBD daemon that manages block maps, pack files, WAL, and a two-tier cache, with S3 as a "portability layer" that's only touched when data crosses host boundaries.

**ZeroFS** is an **LSM tree on S3** — it wraps SlateDB (an embedded LSM database designed for object storage) and exposes three protocol frontends (NFS, 9P, NBD). S3 is the **primary persistent store**; local storage is purely cache.

This is the root of every downstream difference.

---

## 1. Where Data Lives (The Big One)

| | Glide v2 | ZeroFS |
|---|---|---|
| **Primary store** | Local SSD | S3 |
| **S3 role** | Portability layer (fork, migrate, sleep) | Durable persistent backend |
| **Local SSD role** | Source of truth during active operation | Cache only |
| **Write ack** | After local WAL + SSD | After memtable (then async flush to S3) |

**Glide's tradeoff**: Faster writes, zero S3 traffic during normal operation, but **host death = data loss** (bounded by flush mode). Accepts this explicitly for ephemeral VMs. Production uses continuous flush (~5s loss window).

**ZeroFS's tradeoff**: Every write eventually reaches S3 as the source of truth. No data loss on host death (beyond what's in the memtable). But **every write generates S3 traffic** — the memtable flushes to SST files in S3, compaction generates more S3 I/O.

**Pros/Cons**:
- Glide's model is ideal for Paraglide's use case: mostly ephemeral VMs that fork from production, do work, and die. The insight that "S3 traffic should be proportional to cross-host transitions, not VM uptime" is powerful.
- ZeroFS's model is more conservative and general-purpose. You never lose data. But you pay for it in S3 operations — every write eventually becomes S3 PUTs.
- Glide's demand-driven flush means a preview VM that lives and dies on one host generates **zero** S3 operations. That's hard to beat economically.

---

## 2. Storage Engine

| | Glide v2 | ZeroFS |
|---|---|---|
| **Engine** | Custom: block map + pack files + WAL | SlateDB (LSM tree) |
| **Key abstraction** | Content-addressed 128KB blocks | Key-value pairs in sorted string tables |
| **Deduplication** | First-class (same content → same hash → one copy) | None |
| **S3 object model** | Pack files (25 blocks, ~3.2MB each) | SST files (sorted string tables) |

**Glide's tradeoff**: Building a custom storage engine is more work and risk, but it's **purpose-built** for the exact use case — block-level storage for microVMs with cross-host fork as the killer feature. Content-addressing gives free dedup, free integrity verification, and makes fork a metadata-only operation.

**ZeroFS's tradeoff**: Building on SlateDB means standing on a well-tested foundation. The LSM tree handles compaction, write buffering, crash recovery, and manifest management — that's a lot of complexity you don't write yourself. But it's a **general-purpose** engine that doesn't understand blocks, dedup, or VM lifecycle.

**Pros/Cons**:
- Glide: 10 VMs running the same Node app share one copy of the base OS. ZeroFS stores 10 copies. For a platform running hundreds of similar VMs, this is a huge storage cost difference.
- ZeroFS: You get atomic transactions, total write ordering, and immediate crash recovery from SlateDB for free. Glide has to build all of that (WAL, sequence numbers, block map persistence).
- Glide: Content-addressing makes integrity checking trivial — rehash and compare. ZeroFS relies on S3 and checksums at the SST level.

---

## 3. Fork / Clone

| | Glide v2 | ZeroFS |
|---|---|---|
| **Mechanism** | Copy manifest in S3 (metadata only) | SlateDB checkpoint |
| **Cross-host** | First-class — copy manifest, boot anywhere | Implicit — S3 is primary, any host can read it |
| **Cost** | O(manifest size) — a few MB | O(checkpoint creation) — point-in-time LSM snapshot |
| **Shared data** | Blocks shared by reference (same hashes) | No sharing — each instance has its own data |

**This is Glide's crown jewel.** Forking a 100GB VM is copying a ~1MB manifest. The fork shares 100% of its blocks with the parent until it writes. Storage cost is proportional to *unique writes*, not total disk size.

ZeroFS checkpoints are powerful but different — they snapshot the LSM state (which is useful for rollback), but they don't give you the "fork a 100GB VM to another host for zero data transfer" story. You'd need to start a new ZeroFS instance reading from the same S3 bucket, which gets you read access but not independent writes without more coordination.

---

## 4. Filesystem Semantics vs. Block Device

| | Glide v2 | ZeroFS |
|---|---|---|
| **Interface** | NBD only (block device) | NFS + 9P + NBD |
| **POSIX compliance** | N/A (raw blocks, guest OS provides filesystem) | Full (8,662 pjdfstest passing) |
| **Use case** | Backing store for VM root disks | General-purpose cloud filesystem + block device |

**Glide's tradeoff**: By operating at the block level, Glide is simpler and doesn't need to understand files, directories, permissions, locks, etc. The guest OS handles all that. But it *only* works as a VM backing store.

**ZeroFS's tradeoff**: Full POSIX semantics make it versatile (mount anywhere, use for anything), but it's a **much** larger surface area to implement and maintain. Implementing a correct, concurrent POSIX filesystem is notoriously hard.

For Paraglide's specific use case — backing microVMs — Glide's block-level approach is the right call. You don't need POSIX in the storage layer when the guest provides it.

---

## 5. Encryption

| | Glide v2 | ZeroFS |
|---|---|---|
| **Approach** | Server-side (SSE-KMS) | Client-side (XChaCha20-Poly1305) |
| **Key management** | AWS KMS, per-tenant keys | Application-managed, HKDF-derived |
| **Trust model** | Trust AWS | Trust nobody (zero-knowledge) |

**ZeroFS wins on security posture** — the S3 provider never sees plaintext. Glide delegates to AWS, which is simpler operationally but means AWS (or anyone with bucket access + KMS key access) can read your data.

**Glide wins on simplicity** — zero crypto code in the daemon.

---

## 6. Write Amplification & I/O Patterns

**Glide**: A 4KB write to a 128KB chunk produces a 128KB new block locally (32x amplification on local SSD). But this only hits S3 when flushed, and typical workloads do large sequential writes where amplification is ~1x.

**ZeroFS**: An LSM tree has its own write amplification from compaction — writes flow through L0 → L1 → L2 → ... with data rewritten at each level. For a write-heavy workload, the total write amplification to S3 could be 10-30x across compaction levels. But each individual write is small (only the changed bytes + overhead), not a full 128KB chunk.

**Glide's approach favors the VM workload**: large sequential writes (package installs, builds, app deployment) produce ~1x amplification. ZeroFS's LSM compaction amplification is workload-independent.

---

## 7. Operational Complexity

| | Glide v2 | ZeroFS |
|---|---|---|
| **GC** | Custom mark-and-sweep with grace period | LSM compaction (handled by SlateDB) |
| **Crash recovery** | WAL replay + S3 manifest fallback | Automatic (read manifest from S3) |
| **Components to operate** | NBD daemon + control plane coordination | ZeroFS process + optional standalone compactor |
| **Tuning knobs** | Flush intervals, cache sizes, chunk size, pack size | SlateDB tuning (memtable size, compaction strategy, cache sizes) |

ZeroFS has the advantage of leveraging SlateDB's well-tested crash recovery and compaction. Glide's custom GC, WAL, and flush scheduling are all systems that need to be correct under concurrent operations — a significant engineering investment.

---

## Summary

**Glide v2 is the right design for Paraglide.** The architecture is purpose-built for the exact workload:
- Ephemeral VMs that fork from production → content-addressed manifest copy
- Mostly local I/O with rare cross-host transitions → S3 as portability layer
- Storage efficiency across hundreds of similar VMs → content-addressed dedup
- Fast boot and same-host sleep/wake → local SSD is primary

**ZeroFS is a better general-purpose solution.** If you needed a cloud filesystem for diverse workloads (databases, dev environments, file sharing), ZeroFS's POSIX compliance and strong durability model are compelling.

**The key risk in Glide v2** is building and maintaining all of the custom infrastructure (content-addressed store, block map, WAL, pack files, GC, flush scheduling, snapshot mechanism). ZeroFS gets most of this "for free" from SlateDB. The question is whether the performance and cost advantages of Glide's purpose-built design justify that engineering investment — and for a platform where VM storage is a core differentiator, they clearly do.

**The key weakness of ZeroFS for this use case** is that it has no story for dedup or cheap fork. Every VM would store its own full copy of data, and "fork" would require copying or restructuring data rather than just copying a manifest. For a platform running hundreds of VMs from the same base images, that's a dealbreaker on cost alone.
