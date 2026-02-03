use crate::checkpoint_manager::CheckpointManager;
use crate::fs::flush_coordinator::FlushCoordinator;
use crate::fs::lease::LeaseCoordinator;
use crate::fs::tracing::AccessTracer;
use crate::fs::write_coordinator::WriteCoordinator;
use crate::fs::ZeroFS;
use crate::rpc::proto::{self, admin_service_server::AdminService};
use anyhow::{Context, Result};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use tokio::net::UnixListener;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::{BroadcastStream, UnixListenerStream};
use tokio_util::sync::CancellationToken;
use tonic::{Request, Response, Status};
use tracing::info;

#[derive(Clone)]
pub struct AdminRpcServer {
    checkpoint_manager: Arc<CheckpointManager>,
    flush_coordinator: FlushCoordinator,
    write_coordinator: Arc<WriteCoordinator>,
    lease_coordinator: Option<Arc<LeaseCoordinator>>,
    tracer: AccessTracer,
    /// Reference to ZeroFS for path-to-inode resolution
    fs: Arc<ZeroFS>,
}

impl AdminRpcServer {
    pub fn new(
        checkpoint_manager: Arc<CheckpointManager>,
        flush_coordinator: FlushCoordinator,
        write_coordinator: Arc<WriteCoordinator>,
        lease_coordinator: Option<Arc<LeaseCoordinator>>,
        tracer: AccessTracer,
        fs: Arc<ZeroFS>,
    ) -> Self {
        Self {
            checkpoint_manager,
            flush_coordinator,
            write_coordinator,
            lease_coordinator,
            tracer,
            fs,
        }
    }
}

#[tonic::async_trait]
impl AdminService for AdminRpcServer {
    type WatchFileAccessStream =
        Pin<Box<dyn tokio_stream::Stream<Item = Result<proto::FileAccessEvent, Status>> + Send>>;

    async fn create_checkpoint(
        &self,
        request: Request<proto::CreateCheckpointRequest>,
    ) -> Result<Response<proto::CreateCheckpointResponse>, Status> {
        let name = request.into_inner().name;

        let info = self
            .checkpoint_manager
            .create_checkpoint(&name)
            .await
            .map_err(|e| Status::internal(format!("Failed to create checkpoint: {}", e)))?;

        Ok(Response::new(proto::CreateCheckpointResponse {
            checkpoint: Some(info.into()),
        }))
    }

    async fn list_checkpoints(
        &self,
        _request: Request<proto::ListCheckpointsRequest>,
    ) -> Result<Response<proto::ListCheckpointsResponse>, Status> {
        let checkpoints = self
            .checkpoint_manager
            .list_checkpoints()
            .await
            .map_err(|e| Status::internal(format!("Failed to list checkpoints: {}", e)))?;

        Ok(Response::new(proto::ListCheckpointsResponse {
            checkpoints: checkpoints.into_iter().map(|c| c.into()).collect(),
        }))
    }

    async fn delete_checkpoint(
        &self,
        request: Request<proto::DeleteCheckpointRequest>,
    ) -> Result<Response<proto::DeleteCheckpointResponse>, Status> {
        let name = request.into_inner().name;

        self.checkpoint_manager
            .delete_checkpoint(&name)
            .await
            .map_err(|e| Status::internal(format!("Failed to delete checkpoint: {}", e)))?;

        Ok(Response::new(proto::DeleteCheckpointResponse {}))
    }

    async fn get_checkpoint_info(
        &self,
        request: Request<proto::GetCheckpointInfoRequest>,
    ) -> Result<Response<proto::GetCheckpointInfoResponse>, Status> {
        let name = request.into_inner().name;

        let info = self
            .checkpoint_manager
            .get_checkpoint_info(&name)
            .await
            .map_err(|e| Status::internal(format!("Failed to get checkpoint info: {}", e)))?;

        match info {
            Some(checkpoint) => Ok(Response::new(proto::GetCheckpointInfoResponse {
                checkpoint: Some(checkpoint.into()),
            })),
            None => Err(Status::not_found(format!(
                "Checkpoint '{}' not found",
                name
            ))),
        }
    }

    async fn watch_file_access(
        &self,
        _request: Request<proto::WatchFileAccessRequest>,
    ) -> Result<Response<Self::WatchFileAccessStream>, Status> {
        let receiver = self.tracer.subscribe();

        let stream = BroadcastStream::new(receiver)
            .filter_map(|result| result.ok())
            .map(|event| Ok(event.into()));

        Ok(Response::new(Box::pin(stream)))
    }

    async fn flush(
        &self,
        _request: Request<proto::FlushRequest>,
    ) -> Result<Response<proto::FlushResponse>, Status> {
        self.flush_coordinator
            .flush()
            .await
            .map_err(|e| Status::internal(format!("Flush failed: {:?}", e)))?;

        Ok(Response::new(proto::FlushResponse {}))
    }

    async fn acquire_lease(
        &self,
        request: Request<proto::AcquireLeaseRequest>,
    ) -> Result<Response<proto::AcquireLeaseResponse>, Status> {
        let path = request.into_inner().path;

        let coordinator = self
            .lease_coordinator
            .as_ref()
            .ok_or_else(|| Status::unavailable("Lease coordination is not enabled"))?;

        // Resolve path to inode using ZeroFS
        let inode_id = self
            .fs
            .resolve_path_to_inode(&path)
            .await
            .map_err(|e| Status::not_found(format!("Path not found: {}", e)))?;

        // Acquire the lease for this path
        coordinator
            .acquire_path(&path, inode_id)
            .await
            .map_err(|e| Status::failed_precondition(format!("Failed to acquire lease: {}", e)))?;

        let lease = coordinator
            .get_path_status(&path)
            .ok_or_else(|| Status::internal("No lease held after acquire"))?;

        Ok(Response::new(proto::AcquireLeaseResponse {
            lease: Some(lease.into()),
        }))
    }

    async fn prepare_handoff(
        &self,
        request: Request<proto::PrepareHandoffRequest>,
    ) -> Result<Response<proto::PrepareHandoffResponse>, Status> {
        let path = request.into_inner().path;

        let coordinator = self
            .lease_coordinator
            .as_ref()
            .ok_or_else(|| Status::unavailable("Lease coordination is not enabled"))?;

        coordinator
            .prepare_handoff_path(&path, &self.write_coordinator, &self.flush_coordinator)
            .await
            .map_err(|e| Status::internal(format!("Prepare handoff failed: {}", e)))?;

        let lease = coordinator
            .get_path_status(&path)
            .ok_or_else(|| Status::internal("No lease held after prepare_handoff"))?;

        Ok(Response::new(proto::PrepareHandoffResponse {
            lease: Some(lease.into()),
        }))
    }

    async fn complete_handoff(
        &self,
        request: Request<proto::CompleteHandoffRequest>,
    ) -> Result<Response<proto::CompleteHandoffResponse>, Status> {
        let path = request.into_inner().path;

        let coordinator = self
            .lease_coordinator
            .as_ref()
            .ok_or_else(|| Status::unavailable("Lease coordination is not enabled"))?;

        coordinator
            .complete_handoff_path(&path)
            .await
            .map_err(|e| Status::internal(format!("Complete handoff failed: {}", e)))?;

        let lease = coordinator
            .get_path_status(&path)
            .ok_or_else(|| Status::internal("No lease held after complete_handoff"))?;

        Ok(Response::new(proto::CompleteHandoffResponse {
            lease: Some(lease.into()),
        }))
    }

    async fn get_lease_status(
        &self,
        request: Request<proto::GetLeaseStatusRequest>,
    ) -> Result<Response<proto::GetLeaseStatusResponse>, Status> {
        let path = request.into_inner().path;

        let (lease, holds_lease) = match &self.lease_coordinator {
            Some(coordinator) => {
                let lease = coordinator.get_path_status(&path);
                let holds_lease = coordinator.holds_lease_for(&path);
                (lease, holds_lease)
            }
            None => (None, false),
        };

        Ok(Response::new(proto::GetLeaseStatusResponse {
            lease: lease.map(|l| l.into()),
            holds_lease,
        }))
    }

    async fn list_held_leases(
        &self,
        _request: Request<proto::ListHeldLeasesRequest>,
    ) -> Result<Response<proto::ListHeldLeasesResponse>, Status> {
        let leases = match &self.lease_coordinator {
            Some(coordinator) => coordinator
                .list_held_leases()
                .into_iter()
                .map(|(_, lease)| lease.into())
                .collect(),
            None => vec![],
        };

        Ok(Response::new(proto::ListHeldLeasesResponse { leases }))
    }
}

/// Serve gRPC over TCP
pub async fn serve_tcp(
    addr: SocketAddr,
    service: AdminRpcServer,
    shutdown: CancellationToken,
) -> Result<()> {
    info!("RPC server listening on {}", addr);

    let grpc_service = proto::admin_service_server::AdminServiceServer::new(service);

    tonic::transport::Server::builder()
        .add_service(grpc_service)
        .serve_with_shutdown(addr, shutdown.cancelled_owned())
        .await
        .with_context(|| format!("Failed to run RPC TCP server on {}", addr))?;

    info!("RPC TCP server shutting down on {}", addr);
    Ok(())
}

/// Serve gRPC over Unix socket
pub async fn serve_unix(
    socket_path: PathBuf,
    service: AdminRpcServer,
    shutdown: CancellationToken,
) -> Result<()> {
    // Remove existing socket file if present
    if socket_path.exists() {
        std::fs::remove_file(&socket_path)
            .with_context(|| format!("Failed to remove existing socket file: {:?}", socket_path))?;
    }

    let listener = UnixListener::bind(&socket_path)
        .with_context(|| format!("Failed to bind RPC Unix socket to {:?}", socket_path))?;

    info!("RPC server listening on Unix socket: {:?}", socket_path);

    let uds_stream = UnixListenerStream::new(listener);

    let grpc_service = proto::admin_service_server::AdminServiceServer::new(service);

    tonic::transport::Server::builder()
        .add_service(grpc_service)
        .serve_with_incoming_shutdown(uds_stream, shutdown.cancelled_owned())
        .await
        .with_context(|| format!("Failed to run RPC Unix socket server on {:?}", socket_path))?;

    info!("RPC Unix socket server shutting down at {:?}", socket_path);
    Ok(())
}
