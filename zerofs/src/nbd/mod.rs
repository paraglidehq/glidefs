pub mod api;
pub mod block_store;
pub mod error;
pub mod handler;
pub mod lease;
pub mod metrics;
pub mod protocol;
pub mod router;
pub mod server;
pub mod state;
pub mod write_cache;

// Re-exports for library API
#[allow(unused_imports)]
pub use block_store::S3BlockStore;
#[allow(unused_imports)]
pub use metrics::{ExportMetrics, MetricsSnapshot};
#[allow(unused_imports)]
pub use router::{ExportInfo, ExportRouter, RouterError};
#[allow(unused_imports)]
pub use server::NBDServer;
#[allow(unused_imports)]
pub use state::{Active, BlockState, DeviceState, Draining, Initializing, Recovering};
#[allow(unused_imports)]
pub use write_cache::{CacheError, WriteCache, WriteCacheConfig};
