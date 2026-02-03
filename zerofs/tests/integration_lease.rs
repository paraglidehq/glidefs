//! Integration tests for per-path writer lease coordination
//!
//! These tests verify the complete per-path lease handoff flow including:
//! - Per-path graceful handoff (one VM at a time)
//! - Multiple independent paths
//! - Timing requirements (<500ms)
//! - Per-inode write blocking

use std::sync::Arc;
use std::time::{Duration, Instant};

use slatedb::object_store::memory::InMemory;
use slatedb::object_store::ObjectStore;

// Re-export from zerofs crate
use zerofs::fs::lease::{LeaseConfig, LeaseCoordinator, LeaseState};
use zerofs::fs::flush_coordinator::FlushCoordinator;
use zerofs::fs::write_coordinator::WriteCoordinator;

fn create_test_store() -> Arc<dyn ObjectStore> {
    Arc::new(InMemory::new())
}

fn create_config(name: &str) -> LeaseConfig {
    LeaseConfig {
        lease_duration_secs: 30,
        renewal_interval_secs: 10,
        holder_name: name.to_string(),
    }
}

/// Create a mock FlushCoordinator for testing that immediately succeeds
fn create_mock_flush_coordinator() -> FlushCoordinator {
    use tokio::sync::mpsc;
    use tokio::sync::oneshot;
    use zerofs::fs::errors::FsError;

    let (sender, mut receiver) =
        mpsc::unbounded_channel::<oneshot::Sender<Result<(), FsError>>>();

    tokio::spawn(async move {
        while let Some(reply) = receiver.recv().await {
            let _ = reply.send(Ok(()));
        }
    });

    #[derive(Clone)]
    struct MockFlushCoordinator {
        sender: mpsc::UnboundedSender<oneshot::Sender<Result<(), FsError>>>,
    }

    let mock = MockFlushCoordinator { sender };
    // SAFETY: FlushCoordinator has the same memory layout
    unsafe { std::mem::transmute(mock) }
}

fn create_mock_write_coordinator() -> Arc<WriteCoordinator> {
    Arc::new(WriteCoordinator::default())
}

/// Test: Per-inode blocking works correctly
///
/// Verifies that blocking one inode doesn't affect others
#[tokio::test]
async fn test_per_inode_blocking() {
    let store = create_test_store();

    let coordinator = Arc::new(LeaseCoordinator::new(
        store,
        "blocking-db",
        create_config("test-node"),
    ));

    // Initially nothing is blocked
    assert!(!coordinator.is_inode_blocked(42));
    assert!(!coordinator.is_inode_blocked(43));

    // Block inode 42
    coordinator.blocked_inodes.insert(42);
    assert!(coordinator.is_inode_blocked(42));
    assert!(!coordinator.is_inode_blocked(43)); // Other inodes not affected

    // Block inode 43
    coordinator.blocked_inodes.insert(43);
    assert!(coordinator.is_inode_blocked(42));
    assert!(coordinator.is_inode_blocked(43));

    // Unblock inode 42
    coordinator.blocked_inodes.remove(&42);
    assert!(!coordinator.is_inode_blocked(42));
    assert!(coordinator.is_inode_blocked(43)); // 43 still blocked

    // Unblock inode 43
    coordinator.blocked_inodes.remove(&43);
    assert!(!coordinator.is_inode_blocked(42));
    assert!(!coordinator.is_inode_blocked(43));
}

/// Test: Complete per-path graceful handoff
///
/// Simulates the full handoff flow for a single path:
/// 1. Node A acquires lease for path
/// 2. Node A prepares handoff (blocks inode, flushes)
/// 3. Node A completes handoff (releases lease)
/// 4. Node B acquires the released lease
#[tokio::test]
async fn test_per_path_graceful_handoff() {
    let store = create_test_store();
    let path = "/.nbd/vm-1.raw";
    let inode_id = 42;

    // Node A acquires lease
    let node_a = Arc::new(LeaseCoordinator::new(
        Arc::clone(&store),
        "shared-db",
        create_config("node-a"),
    ));
    node_a.acquire_path(path, inode_id).await.expect("Node A should acquire lease");

    assert!(node_a.holds_lease_for(path));
    assert!(!node_a.is_inode_blocked(inode_id));

    let status_a = node_a.get_path_status(path).expect("Should have status");
    assert_eq!(status_a.state, LeaseState::Active);
    assert_eq!(status_a.holder_name, "node-a");
    assert_eq!(status_a.path, path);

    // Node A prepares for handoff - returns token for compile-time enforcement
    let write_coordinator = create_mock_write_coordinator();
    let flush_coordinator = create_mock_flush_coordinator();

    let prepared = node_a
        .prepare_handoff(path, &write_coordinator, &flush_coordinator)
        .await
        .expect("Prepare handoff should succeed");

    assert!(node_a.is_inode_blocked(inode_id), "Inode should be blocked after prepare");
    let status_a = node_a.get_path_status(path).expect("Should have status");
    assert_eq!(status_a.state, LeaseState::Releasing);

    // Node A completes handoff - REQUIRES the token from prepare (compile-time enforced)
    node_a
        .complete_handoff(prepared)
        .await
        .expect("Complete handoff should succeed");

    let status_a = node_a.get_path_status(path).expect("Should have status");
    assert_eq!(status_a.state, LeaseState::Released);

    // Now Node B can acquire the same path
    let node_b = Arc::new(LeaseCoordinator::new(
        Arc::clone(&store),
        "shared-db",
        create_config("node-b"),
    ));
    node_b.acquire_path(path, inode_id).await.expect("Node B should acquire after handoff");

    assert!(node_b.holds_lease_for(path));
    assert!(!node_b.is_inode_blocked(inode_id));

    let status_b = node_b.get_path_status(path).expect("Should have status");
    assert_eq!(status_b.state, LeaseState::Active);
    assert_eq!(status_b.holder_name, "node-b");

    // Version should have incremented through the handoff
    assert!(
        status_b.version > status_a.version,
        "Version should increment: {} > {}",
        status_b.version,
        status_a.version
    );
}

/// Test: Multiple paths can be managed independently
///
/// One path can be in handoff while another remains active
#[tokio::test]
async fn test_multiple_independent_paths() {
    let store = create_test_store();
    let path1 = "/.nbd/vm-1.raw";
    let path2 = "/.nbd/vm-2.raw";
    let inode1 = 42;
    let inode2 = 43;

    let coordinator = Arc::new(LeaseCoordinator::new(
        Arc::clone(&store),
        "multi-db",
        create_config("test-node"),
    ));

    // Acquire both paths
    coordinator.acquire_path(path1, inode1).await.expect("Should acquire path1");
    coordinator.acquire_path(path2, inode2).await.expect("Should acquire path2");

    assert!(coordinator.holds_lease_for(path1));
    assert!(coordinator.holds_lease_for(path2));
    assert!(!coordinator.is_inode_blocked(inode1));
    assert!(!coordinator.is_inode_blocked(inode2));

    // Prepare handoff for path1 only
    let write_coordinator = create_mock_write_coordinator();
    let flush_coordinator = create_mock_flush_coordinator();

    let prepared1 = coordinator
        .prepare_handoff(path1, &write_coordinator, &flush_coordinator)
        .await
        .expect("Prepare handoff for path1 should succeed");

    // Path1's inode is blocked, path2's is not
    assert!(coordinator.is_inode_blocked(inode1), "Inode1 should be blocked");
    assert!(!coordinator.is_inode_blocked(inode2), "Inode2 should NOT be blocked");

    // Path2 is still active
    let status2 = coordinator.get_path_status(path2).expect("Should have status for path2");
    assert_eq!(status2.state, LeaseState::Active);

    // Complete handoff for path1 - requires the token
    coordinator.complete_handoff(prepared1).await.expect("Complete should succeed");

    let status1 = coordinator.get_path_status(path1).expect("Should have status for path1");
    assert_eq!(status1.state, LeaseState::Released);

    // Path2 is still active and unaffected
    let status2 = coordinator.get_path_status(path2).expect("Should have status for path2");
    assert_eq!(status2.state, LeaseState::Active);
    assert!(!coordinator.is_inode_blocked(inode2));
}

/// Test: Handoff timing requirement (<500ms)
///
/// Measures the time taken for a complete per-path handoff.
/// This is critical for fast microVM migration.
#[tokio::test]
async fn test_handoff_timing_under_500ms() {
    let store = create_test_store();
    let path = "/.nbd/vm-1.raw";
    let inode_id = 42;

    // Node A acquires lease
    let node_a = Arc::new(LeaseCoordinator::new(
        Arc::clone(&store),
        "timing-db",
        create_config("node-a"),
    ));
    node_a.acquire_path(path, inode_id).await.unwrap();

    let write_coordinator = create_mock_write_coordinator();
    let flush_coordinator = create_mock_flush_coordinator();

    // Measure handoff time
    let start = Instant::now();

    // Phase 1: Prepare handoff - returns token
    let prepared = node_a
        .prepare_handoff(path, &write_coordinator, &flush_coordinator)
        .await
        .unwrap();

    // Phase 2: Complete handoff - requires token
    node_a.complete_handoff(prepared).await.unwrap();

    // Phase 3: New node acquires
    let node_b = Arc::new(LeaseCoordinator::new(
        Arc::clone(&store),
        "timing-db",
        create_config("node-b"),
    ));
    node_b.acquire_path(path, inode_id).await.unwrap();

    let elapsed = start.elapsed();

    println!("Handoff completed in {:?}", elapsed);

    assert!(
        elapsed < Duration::from_millis(500),
        "Handoff took {:?}, expected < 500ms",
        elapsed
    );

    // Verify the handoff actually worked
    assert!(node_b.holds_lease_for(path));
    assert_eq!(
        node_b.get_path_status(path).unwrap().state,
        LeaseState::Active
    );
}

/// Test: List held leases
#[tokio::test]
async fn test_list_held_leases() {
    let store = create_test_store();
    let path1 = "/.nbd/vm-1.raw";
    let path2 = "/.nbd/vm-2.raw";
    let inode1 = 42;
    let inode2 = 43;

    let coordinator = Arc::new(LeaseCoordinator::new(
        store,
        "list-db",
        create_config("test-node"),
    ));

    // Initially no leases
    let leases = coordinator.list_held_leases();
    assert!(leases.is_empty(), "Should have no leases initially");

    // Acquire first path
    coordinator.acquire_path(path1, inode1).await.unwrap();
    let leases = coordinator.list_held_leases();
    assert_eq!(leases.len(), 1);
    assert_eq!(leases[0].0, path1);

    // Acquire second path
    coordinator.acquire_path(path2, inode2).await.unwrap();
    let leases = coordinator.list_held_leases();
    assert_eq!(leases.len(), 2);

    // Release first path
    coordinator.release_path(path1).await.unwrap();
    let leases = coordinator.list_held_leases();
    assert_eq!(leases.len(), 1);
    assert_eq!(leases[0].0, path2);

    // Release all
    coordinator.release_all().await.unwrap();
    let leases = coordinator.list_held_leases();
    assert!(leases.is_empty(), "Should have no leases after release_all");
}

/// Test: Version monotonically increases across all operations
#[tokio::test]
async fn test_version_monotonic_increase() {
    let store = create_test_store();
    let path = "/.nbd/vm-1.raw";
    let inode_id = 42;

    let node_a = Arc::new(LeaseCoordinator::new(
        Arc::clone(&store),
        "version-db",
        create_config("node-a"),
    ));

    node_a.acquire_path(path, inode_id).await.unwrap();
    let v1 = node_a.get_path_status(path).unwrap().version;

    // Prepare handoff increments version
    let write_coordinator = create_mock_write_coordinator();
    let flush_coordinator = create_mock_flush_coordinator();
    let prepared = node_a
        .prepare_handoff(path, &write_coordinator, &flush_coordinator)
        .await
        .unwrap();
    let v2 = node_a.get_path_status(path).unwrap().version;
    assert!(v2 > v1, "Prepare handoff should increment version");

    // Complete handoff increments version
    node_a.complete_handoff(prepared).await.unwrap();
    let v3 = node_a.get_path_status(path).unwrap().version;
    assert!(v3 > v2, "Complete handoff should increment version");

    // New node acquire increments version
    let node_b = Arc::new(LeaseCoordinator::new(
        Arc::clone(&store),
        "version-db",
        create_config("node-b"),
    ));
    node_b.acquire_path(path, inode_id).await.unwrap();
    let v4 = node_b.get_path_status(path).unwrap().version;
    assert!(v4 > v3, "New node acquire should increment version");

    println!("Version progression: {} -> {} -> {} -> {}", v1, v2, v3, v4);
}

/// Test: Multiple handoff cycles for same path
///
/// Verifies the system can handle multiple consecutive handoffs for the same path
#[tokio::test]
async fn test_multiple_handoff_cycles() {
    let store = create_test_store();
    let write_coordinator = create_mock_write_coordinator();
    let path = "/.nbd/vm-1.raw";
    let inode_id = 42;

    for cycle in 0..3 {
        let node_name = format!("node-cycle-{}", cycle);
        let coordinator = Arc::new(LeaseCoordinator::new(
            Arc::clone(&store),
            "multi-handoff-db",
            create_config(&node_name),
        ));

        coordinator
            .acquire_path(path, inode_id)
            .await
            .expect(&format!("Cycle {}: acquire should succeed", cycle));

        assert!(coordinator.holds_lease_for(path));

        let flush_coordinator = create_mock_flush_coordinator();
        let prepared = coordinator
            .prepare_handoff(path, &write_coordinator, &flush_coordinator)
            .await
            .expect(&format!("Cycle {}: prepare should succeed", cycle));

        coordinator
            .complete_handoff(prepared)
            .await
            .expect(&format!("Cycle {}: complete should succeed", cycle));

        let status = coordinator.get_path_status(path).unwrap();
        assert_eq!(status.state, LeaseState::Released);

        println!("Cycle {} completed, version: {}", cycle, status.version);
    }
}

/// Test: Concurrent acquire attempts for the same path
///
/// Verifies that when multiple nodes try to acquire the same path simultaneously,
/// exactly one succeeds and the others fail with LeaseHeldByOther error.
/// This tests the S3 conditional write (ETag) mechanism.
#[tokio::test]
async fn test_concurrent_acquire_race_condition() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use zerofs::fs::lease::LeaseError;

    let store = create_test_store();
    let path = "/.nbd/contested-vm.raw";
    let inode_id = 100;

    let success_count = Arc::new(AtomicUsize::new(0));
    let failure_count = Arc::new(AtomicUsize::new(0));

    // Launch multiple concurrent acquire attempts
    let num_contenders = 10;
    let mut handles = Vec::new();

    for i in 0..num_contenders {
        let store_clone = Arc::clone(&store);
        let success_count_clone = Arc::clone(&success_count);
        let failure_count_clone = Arc::clone(&failure_count);
        let node_name = format!("node-{}", i);

        let handle = tokio::spawn(async move {
            let coordinator = LeaseCoordinator::new(
                store_clone,
                "concurrent-db",
                create_config(&node_name),
            );

            match coordinator.acquire_path(path, inode_id).await {
                Ok(()) => {
                    success_count_clone.fetch_add(1, Ordering::SeqCst);
                    println!("{} acquired the lease!", node_name);
                    Some(node_name)
                }
                Err(LeaseError::LeaseHeldByOther { holder_name, .. }) => {
                    failure_count_clone.fetch_add(1, Ordering::SeqCst);
                    println!("{} failed: lease held by {}", node_name, holder_name);
                    None
                }
                Err(LeaseError::ConcurrentModification) => {
                    failure_count_clone.fetch_add(1, Ordering::SeqCst);
                    println!("{} failed: concurrent modification", node_name);
                    None
                }
                Err(e) => {
                    panic!("{} got unexpected error: {:?}", node_name, e);
                }
            }
        });

        handles.push(handle);
    }

    // Wait for all attempts to complete
    let results: Vec<Option<String>> = futures::future::join_all(handles)
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();

    let successes = success_count.load(Ordering::SeqCst);
    let failures = failure_count.load(Ordering::SeqCst);

    println!(
        "Concurrent acquire results: {} successes, {} failures",
        successes, failures
    );

    // Exactly one should succeed
    assert_eq!(
        successes, 1,
        "Expected exactly 1 successful acquire, got {}",
        successes
    );

    // The rest should fail
    assert_eq!(
        failures,
        num_contenders - 1,
        "Expected {} failures, got {}",
        num_contenders - 1,
        failures
    );

    // Verify the winner
    let winner: Vec<String> = results.into_iter().flatten().collect();
    assert_eq!(winner.len(), 1);
    println!("Winner: {}", winner[0]);
}

/// Test: Concurrent acquire after release - first come first serve
///
/// After a lease is released, multiple nodes race to acquire it.
/// Exactly one should win.
#[tokio::test]
async fn test_concurrent_acquire_after_release() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use zerofs::fs::lease::LeaseError;

    let store = create_test_store();
    let path = "/.nbd/released-vm.raw";
    let inode_id = 101;

    // First, have a node acquire and release the lease
    let initial_node = Arc::new(LeaseCoordinator::new(
        Arc::clone(&store),
        "release-db",
        create_config("initial-holder"),
    ));
    initial_node.acquire_path(path, inode_id).await.unwrap();

    let write_coordinator = create_mock_write_coordinator();
    let flush_coordinator = create_mock_flush_coordinator();

    let prepared = initial_node
        .prepare_handoff(path, &write_coordinator, &flush_coordinator)
        .await
        .unwrap();
    initial_node.complete_handoff(prepared).await.unwrap();

    // Verify lease is released
    let status = initial_node.get_path_status(path).unwrap();
    assert_eq!(status.state, LeaseState::Released);

    // Now race to acquire
    let success_count = Arc::new(AtomicUsize::new(0));
    let num_contenders = 5;
    let mut handles = Vec::new();

    for i in 0..num_contenders {
        let store_clone = Arc::clone(&store);
        let success_count_clone = Arc::clone(&success_count);
        let node_name = format!("racer-{}", i);

        let handle = tokio::spawn(async move {
            let coordinator = LeaseCoordinator::new(
                store_clone,
                "release-db",
                create_config(&node_name),
            );

            match coordinator.acquire_path(path, inode_id).await {
                Ok(()) => {
                    success_count_clone.fetch_add(1, Ordering::SeqCst);
                    true
                }
                Err(LeaseError::LeaseHeldByOther { .. })
                | Err(LeaseError::ConcurrentModification) => false,
                Err(e) => panic!("Unexpected error: {:?}", e),
            }
        });

        handles.push(handle);
    }

    let results: Vec<bool> = futures::future::join_all(handles)
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();

    let winners: Vec<_> = results.iter().filter(|&&won| won).collect();
    println!(
        "Race after release: {} winners out of {} contenders",
        winners.len(),
        num_contenders
    );

    // Exactly one winner
    assert_eq!(
        winners.len(),
        1,
        "Expected exactly 1 winner after release race"
    );
}
