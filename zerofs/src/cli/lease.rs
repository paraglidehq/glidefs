use crate::cli::connect_rpc_client;
use crate::rpc::proto;
use anyhow::Result;
use std::path::Path;

pub async fn status(config_path: &Path, lease_path: &str) -> Result<()> {
    let client = connect_rpc_client(config_path).await?;
    let response = client.get_lease_status(lease_path).await?;

    println!("Lease Status for path: {}", lease_path);
    println!();

    if let Some(lease) = response.lease {
        let state = proto::LeaseState::try_from(lease.state)
            .map(|s| format!("{:?}", s))
            .unwrap_or_else(|_| format!("Unknown({})", lease.state));

        println!("  Holder ID:    {}", lease.holder_id);
        println!("  Holder Name:  {}", lease.holder_name);
        println!("  State:        {}", state);
        println!("  Acquired At:  {}", format_timestamp(lease.acquired_at));
        println!("  Expires At:   {}", format_timestamp(lease.expires_at));
        println!("  Version:      {}", lease.version);
        println!("  Path:         {}", lease.path);
        println!();
        println!("Local Status:");
        println!("  Holds Lease:  {}", response.holds_lease);
    } else {
        println!("No lease is currently held for this path.");
        println!();
        println!("Local Status:");
        println!("  Holds Lease:  {}", response.holds_lease);
    }

    Ok(())
}

pub async fn list(config_path: &Path) -> Result<()> {
    let client = connect_rpc_client(config_path).await?;
    let response = client.list_held_leases().await?;

    if response.leases.is_empty() {
        println!("No leases are currently held.");
        return Ok(());
    }

    println!("Held Leases:");
    println!();

    for lease in response.leases {
        let state = proto::LeaseState::try_from(lease.state)
            .map(|s| format!("{:?}", s))
            .unwrap_or_else(|_| format!("Unknown({})", lease.state));

        println!("  Path:         {}", lease.path);
        println!("  State:        {}", state);
        println!("  Holder Name:  {}", lease.holder_name);
        println!("  Expires At:   {}", format_timestamp(lease.expires_at));
        println!();
    }

    Ok(())
}

pub async fn acquire(config_path: &Path, lease_path: &str) -> Result<()> {
    let client = connect_rpc_client(config_path).await?;

    println!("Acquiring lease for path: {}", lease_path);

    let response = client.acquire_lease(lease_path).await?;

    if let Some(lease) = response.lease {
        let state = proto::LeaseState::try_from(lease.state)
            .map(|s| format!("{:?}", s))
            .unwrap_or_else(|_| format!("Unknown({})", lease.state));

        println!();
        println!("Lease acquired successfully.");
        println!("  Holder ID:    {}", lease.holder_id);
        println!("  Holder Name:  {}", lease.holder_name);
        println!("  State:        {}", state);
        println!("  Path:         {}", lease.path);
        println!("  Expires At:   {}", format_timestamp(lease.expires_at));
        println!();
        println!("This node now has exclusive write access to this path.");
    }

    Ok(())
}

pub async fn prepare_handoff(config_path: &Path, lease_path: &str) -> Result<()> {
    let client = connect_rpc_client(config_path).await?;

    println!("Preparing for handoff of path: {}", lease_path);
    println!("  - Blocking new writes to this inode");
    println!("  - Draining in-flight writes");
    println!("  - Flushing data to S3");

    let response = client.prepare_handoff(lease_path).await?;

    if let Some(lease) = response.lease {
        let state = proto::LeaseState::try_from(lease.state)
            .map(|s| format!("{:?}", s))
            .unwrap_or_else(|_| format!("Unknown({})", lease.state));

        println!();
        println!("Handoff prepared successfully.");
        println!("  State: {}", state);
        println!();
        println!("Run 'zerofs lease complete-handoff --path {}' to release the lease.", lease_path);
    }

    Ok(())
}

pub async fn complete_handoff(config_path: &Path, lease_path: &str) -> Result<()> {
    let client = connect_rpc_client(config_path).await?;

    println!("Completing handoff of path: {}", lease_path);

    let response = client.complete_handoff(lease_path).await?;

    if let Some(lease) = response.lease {
        let state = proto::LeaseState::try_from(lease.state)
            .map(|s| format!("{:?}", s))
            .unwrap_or_else(|_| format!("Unknown({})", lease.state));

        println!();
        println!("Handoff completed.");
        println!("  State: {}", state);
        println!();
        println!("Another node can now acquire the writer lease for this path.");
    }

    Ok(())
}

fn format_timestamp(ts: u64) -> String {
    use std::time::{Duration, UNIX_EPOCH};

    let datetime = UNIX_EPOCH + Duration::from_secs(ts);
    match datetime.duration_since(UNIX_EPOCH) {
        Ok(d) => {
            let secs = d.as_secs();
            // Simple UTC timestamp format
            let days = secs / 86400;
            let hours = (secs % 86400) / 3600;
            let minutes = (secs % 3600) / 60;
            let seconds = secs % 60;

            // Calculate approximate date from days since epoch
            // This is a simple approximation, not handling leap years precisely
            let year = 1970 + (days / 365);
            let day_of_year = days % 365;
            let month = day_of_year / 30 + 1;
            let day = day_of_year % 30 + 1;

            format!(
                "{:04}-{:02}-{:02} {:02}:{:02}:{:02} UTC",
                year, month, day, hours, minutes, seconds
            )
        }
        Err(_) => format!("{} (invalid)", ts),
    }
}
