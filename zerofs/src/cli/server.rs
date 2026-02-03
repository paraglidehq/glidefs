use crate::block_transformer::ZeroFsBlockTransformer;
use crate::bucket_identity;
use crate::config::Settings;
use crate::key_management;
use crate::nbd::api::ApiServer;
use crate::nbd::router::ExportRouter;
use crate::nbd::server::NBDServer;
use crate::parse_object_store::parse_url_opts;
use crate::task::spawn_named;
use anyhow::{Context, Result};
use object_store::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use tracing::info;

pub async fn run_server(config_path: PathBuf) -> Result<()> {
    use tracing_subscriber::EnvFilter;

    let filter = EnvFilter::try_from_default_env().unwrap_or(EnvFilter::new("info"));

    #[cfg(feature = "tokio-console")]
    {
        use tracing_subscriber::prelude::*;
        let console_layer = console_subscriber::spawn();
        tracing_subscriber::registry()
            .with(console_layer)
            .with(
                tracing_subscriber::fmt::layer()
                    .with_writer(std::io::stderr)
                    .with_filter(filter),
            )
            .init();
    }

    #[cfg(not(feature = "tokio-console"))]
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .init();

    let settings = Settings::from_file(&config_path)
        .with_context(|| format!("Failed to load config from {}", config_path.display()))?;

    let url = settings.storage.url.clone();
    let env_vars = settings.cloud_provider_env_vars();

    let (object_store, path_from_url) = parse_url_opts(&url.parse()?, env_vars.into_iter())?;
    let object_store: Arc<dyn object_store::ObjectStore> = Arc::from(object_store);

    let db_path = path_from_url.to_string();

    info!("Starting ZeroFS NBD server with {} backend", object_store);
    info!("Storage path: {}", db_path);

    info!("Checking bucket identity...");
    let bucket = bucket_identity::BucketIdentity::get_or_create(&object_store, &db_path).await?;

    let cache_dir = settings.cache.dir.join(bucket.cache_directory_name());
    info!(
        "Bucket ID: {}, Cache directory: {}",
        bucket.id(),
        cache_dir.display()
    );

    // Create cache directory if it doesn't exist
    std::fs::create_dir_all(&cache_dir)?;

    crate::storage_compatibility::check_if_match_support(&object_store, &db_path).await?;

    let password = settings.storage.encryption_password.clone();
    super::password::validate_password(&password)
        .map_err(|e| anyhow::anyhow!("Password validation failed: {}", e))?;

    info!("Loading or initializing encryption key from object store");
    let object_path = Path::from(db_path.clone());
    let encryption_key =
        key_management::load_or_init_encryption_key(&object_store, &object_path, &password, false)
            .await?;

    let block_transformer =
        ZeroFsBlockTransformer::new_arc(&encryption_key, settings.compression());

    let shutdown = CancellationToken::new();

    let nbd_config = settings
        .servers
        .nbd
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("NBD server configuration is required"))?;

    // Create the export router
    let router = Arc::new(ExportRouter::new(
        Arc::clone(&object_store),
        db_path,
        cache_dir,
        nbd_config.block_size(),
        Some(block_transformer),
    ));

    // Load static exports from config
    let exports = nbd_config.get_exports();
    if exports.is_empty() {
        return Err(anyhow::anyhow!(
            "No exports configured. Add exports to your config file or use the API to create them."
        ));
    }

    for export_config in exports {
        info!(
            "Loading static export '{}' ({}GB)",
            export_config.name, export_config.size_gb
        );
        router
            .create_export(export_config.clone(), false)
            .await
            .with_context(|| format!("Failed to create export '{}'", export_config.name))?;
    }

    let mut handles = Vec::new();

    // Start NBD TCP servers
    if let Some(addresses) = &nbd_config.addresses {
        for addr in addresses {
            info!("Starting NBD server on {}", addr);
            let nbd_server = NBDServer::new_tcp(Arc::clone(&router), *addr);
            let shutdown_clone = shutdown.clone();
            handles.push(spawn_named("nbd-tcp", async move {
                nbd_server.start(shutdown_clone).await
            }));
        }
    }

    // Start NBD Unix socket server
    if let Some(socket_path) = nbd_config.unix_socket.as_ref() {
        info!("Starting NBD server on Unix socket {}", socket_path.display());
        let nbd_server = NBDServer::new_unix(Arc::clone(&router), socket_path);
        let shutdown_clone = shutdown.clone();
        handles.push(spawn_named("nbd-unix", async move {
            nbd_server.start(shutdown_clone).await
        }));
    }

    // Start HTTP API server
    if let Some(api_addr) = nbd_config.api_address {
        info!("Starting HTTP API server on {}", api_addr);
        let api_server = ApiServer::new(Arc::clone(&router), api_addr);
        let shutdown_clone = shutdown.clone();
        handles.push(spawn_named("http-api", async move {
            api_server.start(shutdown_clone).await
        }));
    }

    if handles.is_empty() {
        return Err(anyhow::anyhow!(
            "No servers started. Configure at least one of: addresses, unix_socket, or api_address"
        ));
    }

    info!("ZeroFS ready. Available exports:");
    for export in router.list_exports().await {
        info!(
            "  - {} ({}GB, {})",
            export.name,
            export.size / 1_000_000_000,
            if export.readonly { "readonly" } else { "read-write" }
        );
    }
    info!("Send SIGUSR1 to drain all exports to S3 (for node maintenance)");

    // Set up signal handlers
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    let mut sigusr1 = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::user_defined1())?;

    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                info!("Received SIGINT, initiating graceful shutdown...");
                break;
            }
            _ = sigterm.recv() => {
                info!("Received SIGTERM, initiating graceful shutdown...");
                break;
            }
            _ = sigusr1.recv() => {
                info!("Received SIGUSR1, draining all exports to S3...");
                if let Err(e) = router.drain_all().await {
                    tracing::error!("Drain failed: {}", e);
                } else {
                    info!("Drain complete - all exports synced to S3");
                }
            }
        }
    }

    info!("Cancelling all servers...");
    shutdown.cancel();

    info!("Waiting for servers to exit...");
    for handle in handles {
        let _ = handle.await;
    }

    // Graceful shutdown: drain all exports
    info!("Final drain before shutdown...");
    if let Err(e) = router.shutdown().await {
        tracing::error!("Shutdown drain failed: {}", e);
    }

    info!("Shutdown complete");
    Ok(())
}
