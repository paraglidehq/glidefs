//! Integration tests for ZeroFS NBD cache.
//!
//! These tests verify the key architectural claims:
//! 1. Wake from any node - read-through cache fetches from S3
//! 2. Live migration - readonly staging → drain → promote with lease handoff
//! 3. Instant snapshot - flush() is local SSD only
//! 4. Failure recovery - graceful handling of S3/network failures

mod failure_injection;
mod live_migration;
mod wake_any_node;
