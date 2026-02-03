//! NBD server implementation with multi-export support.
//!
//! This module provides a TCP/Unix socket NBD server that supports multiple
//! exports, each with its own write-behind cache and S3 storage.

use super::error::{NBDError, Result};
use super::handler::{NBDBlockHandler, NBDDevice};
use super::protocol::*;
use super::router::ExportRouter;
use bytes::BytesMut;
use deku::prelude::*;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::{TcpListener, UnixListener};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

pub enum Transport {
    Tcp(SocketAddr),
    Unix(std::path::PathBuf),
}

/// NBD server with multi-export support.
///
/// Uses an ExportRouter to manage multiple exports, each with independent
/// storage and caching.
///
/// # Example
/// ```ignore
/// let router = Arc::new(ExportRouter::new(...));
/// router.create_export(config, false).await?;
///
/// let server = NBDServer::new_tcp(router, "0.0.0.0:10809".parse()?);
/// server.start(shutdown_token).await?;
/// ```
pub struct NBDServer {
    router: Arc<ExportRouter>,
    transport: Transport,
}

impl NBDServer {
    /// Create a TCP NBD server.
    pub fn new_tcp(router: Arc<ExportRouter>, socket: SocketAddr) -> Self {
        Self {
            router,
            transport: Transport::Tcp(socket),
        }
    }

    /// Create a Unix socket NBD server.
    pub fn new_unix(router: Arc<ExportRouter>, socket_path: impl Into<std::path::PathBuf>) -> Self {
        Self {
            router,
            transport: Transport::Unix(socket_path.into()),
        }
    }

    fn spawn_client_handler<S>(&self, stream: S, shutdown: &CancellationToken, client_name: String)
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + Sync + 'static,
    {
        let router = Arc::clone(&self.router);
        let client_shutdown = shutdown.child_token();

        tokio::spawn(async move {
            if let Err(e) = handle_client_stream(stream, router, client_shutdown).await {
                error!("Error handling NBD client {}: {}", client_name, e);
            }
        });
    }

    /// Start the NBD server.
    ///
    /// This listens for connections and spawns a handler for each client.
    /// Returns when the shutdown token is cancelled.
    pub async fn start(&self, shutdown: CancellationToken) -> std::io::Result<()> {
        match &self.transport {
            Transport::Tcp(socket) => {
                let listener = TcpListener::bind(socket).await?;
                info!("NBD server listening on {}", socket);

                loop {
                    tokio::select! {
                        _ = shutdown.cancelled() => {
                            info!("NBD TCP server shutting down on {}", socket);
                            break;
                        }
                        result = listener.accept() => {
                            let (stream, addr) = result?;
                            info!("NBD client connected from {}", addr);
                            stream.set_nodelay(true)?;
                            self.spawn_client_handler(stream, &shutdown, addr.to_string());
                        }
                    }
                }
            }
            Transport::Unix(path) => {
                // Remove existing socket file if it exists
                let _ = std::fs::remove_file(path);

                let listener = UnixListener::bind(path).map_err(|e| {
                    std::io::Error::new(
                        e.kind(),
                        format!("Failed to bind NBD Unix socket at {:?}: {}", path, e),
                    )
                })?;
                info!("NBD server listening on Unix socket {:?}", path);

                loop {
                    tokio::select! {
                        _ = shutdown.cancelled() => {
                            info!("NBD Unix socket server shutting down at {:?}", path);
                            break;
                        }
                        result = listener.accept() => {
                            let (stream, _) = result?;
                            info!("NBD client connected via Unix socket");
                            self.spawn_client_handler(stream, &shutdown, "unix".to_string());
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

async fn handle_client_stream<S>(
    stream: S,
    router: Arc<ExportRouter>,
    shutdown: CancellationToken,
) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + Sync + 'static,
{
    let (reader, writer) = tokio::io::split(stream);
    let reader = BufReader::new(reader);
    let writer = BufWriter::new(writer);

    let mut session = NBDSession::new(reader, writer, router, shutdown);
    session.perform_handshake().await?;

    match session.negotiate_options().await {
        Ok((device, handler)) => {
            info!(
                "Client selected export: {}",
                String::from_utf8_lossy(&device.name)
            );
            session.handle_transmission(device, handler).await?;
        }
        Err(NBDError::Io(ref e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
            debug!("Client disconnected cleanly after option negotiation");
            return Ok(());
        }
        Err(e) => return Err(e),
    }

    Ok(())
}

struct NBDSession<R, W> {
    reader: R,
    writer: W,
    router: Arc<ExportRouter>,
    client_no_zeroes: bool,
    shutdown: CancellationToken,
}

impl<R: AsyncRead + Unpin, W: AsyncWrite + Unpin> NBDSession<R, W> {
    fn new(
        reader: R,
        writer: W,
        router: Arc<ExportRouter>,
        shutdown: CancellationToken,
    ) -> Self {
        Self {
            reader,
            writer,
            router,
            client_no_zeroes: false,
            shutdown,
        }
    }

    async fn perform_handshake(&mut self) -> Result<()> {
        let handshake = NBDServerHandshake::new(NBD_FLAG_FIXED_NEWSTYLE | NBD_FLAG_NO_ZEROES);
        let handshake_bytes = handshake.to_bytes()?;
        self.writer.write_all(&handshake_bytes).await?;
        self.writer.flush().await?;

        let mut buf = [0u8; 4];
        self.reader.read_exact(&mut buf).await?;
        let client_flags = NBDClientFlags::from_bytes((&buf, 0))?.1;

        debug!("Client flags: 0x{:x}", client_flags.flags);

        if (client_flags.flags & NBD_FLAG_C_FIXED_NEWSTYLE) == 0 {
            return Err(NBDError::IncompatibleClient);
        }

        self.client_no_zeroes = (client_flags.flags & NBD_FLAG_C_NO_ZEROES) != 0;

        Ok(())
    }

    async fn negotiate_options(&mut self) -> Result<(NBDDevice, Arc<NBDBlockHandler>)> {
        loop {
            let mut header_buf = [0u8; NBD_OPTION_HEADER_SIZE];
            match self.reader.read_exact(&mut header_buf).await {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    debug!("Client disconnected during option negotiation");
                    return Err(NBDError::Io(e));
                }
                Err(e) => return Err(NBDError::Io(e)),
            }
            let header = NBDOptionHeader::from_bytes((&header_buf, 0))
                .map_err(|e| {
                    debug!("Raw header bytes: {:02x?}", header_buf);
                    NBDError::Protocol(format!("Invalid option header: {e}"))
                })?
                .1;

            debug!(
                "Received option: {} (length: {})",
                header.option, header.length
            );

            match header.option {
                NBD_OPT_LIST => {
                    debug!("Handling LIST option");
                    self.handle_list_option(header.length).await?;
                }
                NBD_OPT_EXPORT_NAME => {
                    debug!("Handling EXPORT_NAME option");
                    return self.handle_export_name_option(header.length).await;
                }
                NBD_OPT_INFO => {
                    debug!("Handling INFO option");
                    self.handle_info_option(header.length).await?;
                }
                NBD_OPT_GO => {
                    match self.handle_go_option(header.length).await {
                        Ok(result) => return Ok(result),
                        Err(NBDError::DeviceNotFound(_)) => {
                            // Device not found - stay in negotiation loop
                        }
                        Err(e) => return Err(e),
                    }
                }
                NBD_OPT_STRUCTURED_REPLY => {
                    debug!("Handling STRUCTURED_REPLY option");
                    self.handle_structured_reply_option(header.length).await?;
                }
                NBD_OPT_ABORT => {
                    debug!("Handling ABORT option");
                    self.send_option_reply(header.option, NBD_REP_ACK, &[])
                        .await?;
                    self.writer.flush().await?;
                    return Err(NBDError::Protocol("Client aborted".to_string()));
                }
                _ => {
                    debug!("Unknown option: {}", header.option);
                    self.drain_option_data(header.length).await?;
                    self.send_option_reply(header.option, NBD_REP_ERR_UNSUP, &[])
                        .await?;
                    self.writer.flush().await?;
                }
            }
        }
    }

    async fn handle_list_option(&mut self, length: u32) -> Result<()> {
        self.drain_option_data(length).await?;

        // List all exports from router
        let exports = self.router.list_exports().await;

        for export in exports {
            // Send NBD_REP_SERVER for each export
            let name_bytes = export.name.as_bytes();
            let mut data = Vec::with_capacity(4 + name_bytes.len());
            data.extend_from_slice(&(name_bytes.len() as u32).to_be_bytes());
            data.extend_from_slice(name_bytes);

            self.send_option_reply(NBD_OPT_LIST, NBD_REP_SERVER, &data)
                .await?;
        }

        // Send final ACK
        self.send_option_reply(NBD_OPT_LIST, NBD_REP_ACK, &[])
            .await?;
        self.writer.flush().await?;
        Ok(())
    }

    async fn handle_export_name_option(
        &mut self,
        length: u32,
    ) -> Result<(NBDDevice, Arc<NBDBlockHandler>)> {
        let mut name_buf = vec![0u8; length as usize];
        self.reader.read_exact(&mut name_buf).await?;

        let export_name = String::from_utf8_lossy(&name_buf).to_string();
        debug!(
            "Client requested export: '{}' (length: {})",
            export_name, length
        );

        // Look up handler from router
        let handler = self.router.get_handler(&export_name).await.ok_or_else(|| {
            error!("Export '{}' not found, closing connection", export_name);
            NBDError::DeviceNotFound(name_buf.clone())
        })?;

        let device = NBDDevice {
            name: name_buf,
            size: handler.device_size(),
        };

        self.writer.write_all(&device.size.to_be_bytes()).await?;
        self.writer
            .write_all(&TRANSMISSION_FLAGS.to_be_bytes())
            .await?;

        if !self.client_no_zeroes {
            self.writer
                .write_all(&[0u8; NBD_EXPORT_NAME_PADDING])
                .await?;
        }

        self.writer.flush().await?;
        Ok((device, handler))
    }

    async fn handle_info_option(&mut self, length: u32) -> Result<()> {
        let data = self.read_option_data(length).await?;

        // Parse export name from data
        if data.len() < 4 {
            self.send_option_reply(NBD_OPT_INFO, NBD_REP_ERR_INVALID, &[])
                .await?;
            self.writer.flush().await?;
            return Ok(());
        }

        let name_len = u32::from_be_bytes([data[0], data[1], data[2], data[3]]) as usize;
        if data.len() < 4 + name_len {
            self.send_option_reply(NBD_OPT_INFO, NBD_REP_ERR_INVALID, &[])
                .await?;
            self.writer.flush().await?;
            return Ok(());
        }

        let export_name = String::from_utf8_lossy(&data[4..4 + name_len]).to_string();

        // Look up handler
        let handler = match self.router.get_handler(&export_name).await {
            Some(h) => h,
            None => {
                self.send_option_reply(NBD_OPT_INFO, NBD_REP_ERR_UNKNOWN, &[])
                    .await?;
                self.writer.flush().await?;
                return Ok(());
            }
        };

        // Send NBD_INFO_EXPORT
        let mut info_data = Vec::with_capacity(12);
        info_data.extend_from_slice(&NBD_INFO_EXPORT.to_be_bytes());
        info_data.extend_from_slice(&handler.device_size().to_be_bytes());
        info_data.extend_from_slice(&TRANSMISSION_FLAGS.to_be_bytes());

        self.send_option_reply(NBD_OPT_INFO, NBD_REP_INFO, &info_data)
            .await?;

        // Send ACK
        self.send_option_reply(NBD_OPT_INFO, NBD_REP_ACK, &[])
            .await?;
        self.writer.flush().await?;
        Ok(())
    }

    async fn handle_go_option(
        &mut self,
        length: u32,
    ) -> Result<(NBDDevice, Arc<NBDBlockHandler>)> {
        let data = self.read_option_data(length).await?;

        // Parse export name
        if data.len() < 4 {
            self.send_option_reply(NBD_OPT_GO, NBD_REP_ERR_INVALID, &[])
                .await?;
            self.writer.flush().await?;
            return Err(NBDError::DeviceNotFound(Vec::new()));
        }

        let name_len = u32::from_be_bytes([data[0], data[1], data[2], data[3]]) as usize;
        if data.len() < 4 + name_len {
            self.send_option_reply(NBD_OPT_GO, NBD_REP_ERR_INVALID, &[])
                .await?;
            self.writer.flush().await?;
            return Err(NBDError::DeviceNotFound(Vec::new()));
        }

        let name_bytes = &data[4..4 + name_len];
        let export_name = String::from_utf8_lossy(name_bytes).to_string();

        debug!("GO option for export: '{}'", export_name);

        // Look up handler
        let handler = match self.router.get_handler(&export_name).await {
            Some(h) => h,
            None => {
                warn!("Export '{}' not found", export_name);
                self.send_option_reply(NBD_OPT_GO, NBD_REP_ERR_UNKNOWN, &[])
                    .await?;
                self.writer.flush().await?;
                return Err(NBDError::DeviceNotFound(name_bytes.to_vec()));
            }
        };

        let device = NBDDevice {
            name: name_bytes.to_vec(),
            size: handler.device_size(),
        };

        // Send NBD_INFO_EXPORT
        let mut info_data = Vec::with_capacity(12);
        info_data.extend_from_slice(&NBD_INFO_EXPORT.to_be_bytes());
        info_data.extend_from_slice(&device.size.to_be_bytes());
        info_data.extend_from_slice(&TRANSMISSION_FLAGS.to_be_bytes());

        self.send_option_reply(NBD_OPT_GO, NBD_REP_INFO, &info_data)
            .await?;

        // Send ACK
        self.send_option_reply(NBD_OPT_GO, NBD_REP_ACK, &[]).await?;
        self.writer.flush().await?;

        Ok((device, handler))
    }

    async fn handle_structured_reply_option(&mut self, length: u32) -> Result<()> {
        self.drain_option_data(length).await?;
        self.send_option_reply(NBD_OPT_STRUCTURED_REPLY, NBD_REP_ERR_UNSUP, &[])
            .await?;
        self.writer.flush().await?;
        Ok(())
    }

    async fn read_option_data(&mut self, length: u32) -> Result<Vec<u8>> {
        let mut data = vec![0u8; length as usize];
        self.reader.read_exact(&mut data).await?;
        Ok(data)
    }

    async fn drain_option_data(&mut self, length: u32) -> Result<()> {
        if length > 0 {
            let mut buf = vec![0u8; length as usize];
            self.reader.read_exact(&mut buf).await?;
        }
        Ok(())
    }

    async fn send_option_reply(&mut self, option: u32, reply_type: u32, data: &[u8]) -> Result<()> {
        let reply = NBDOptionReply::new(option, reply_type, data.len() as u32);
        let reply_bytes = reply.to_bytes()?;
        self.writer.write_all(&reply_bytes).await?;
        if !data.is_empty() {
            self.writer.write_all(data).await?;
        }
        Ok(())
    }

    async fn handle_transmission(
        &mut self,
        device: NBDDevice,
        handler: Arc<NBDBlockHandler>,
    ) -> Result<()> {
        let device_size = device.size;

        loop {
            let mut request_buf = [0u8; NBD_REQUEST_HEADER_SIZE];

            tokio::select! {
                _ = self.shutdown.cancelled() => {
                    debug!("NBD client handler shutting down");
                    return Ok(());
                }
                result = self.reader.read_exact(&mut request_buf) => {
                    result?;
                }
            }

            let request = NBDRequest::from_bytes((&request_buf, 0))
                .map_err(|e| NBDError::Protocol(format!("Invalid request: {e}")))?
                .1;

            debug!(
                "NBD command: {:?}, offset={}, length={}",
                request.cmd_type, request.offset, request.length
            );

            let fua = (request.flags & NBD_CMD_FLAG_FUA) != 0;

            match request.cmd_type {
                NBDCommand::Read => {
                    let result = handler.read(request.offset, request.length).await;
                    self.send_read_result(request.cookie, result).await;
                }
                NBDCommand::Write => {
                    let result = self
                        .read_write_data(&handler, request.offset, request.length, fua, device_size)
                        .await;
                    self.send_unit_result(request.cookie, result).await;
                }
                NBDCommand::Disconnect => {
                    info!("Client disconnecting");
                    return Ok(());
                }
                NBDCommand::Flush => {
                    let result = handler.flush();
                    self.send_unit_result(request.cookie, result).await;
                }
                NBDCommand::Trim => {
                    let result = handler.trim(request.offset, request.length, fua);
                    self.send_unit_result(request.cookie, result).await;
                }
                NBDCommand::WriteZeroes => {
                    let result = handler.write_zeroes(request.offset, request.length, fua);
                    self.send_unit_result(request.cookie, result).await;
                }
                NBDCommand::Cache => {
                    let result = handler.cache(request.offset, request.length);
                    self.send_unit_result(request.cookie, result).await;
                }
                NBDCommand::Unknown(cmd) => {
                    warn!("Unknown NBD command: {}", cmd);
                    self.send_unit_result(
                        request.cookie,
                        Err(super::error::CommandError::InvalidArgument),
                    )
                    .await;
                }
            }
        }
    }

    async fn read_write_data(
        &mut self,
        handler: &NBDBlockHandler,
        offset: u64,
        length: u32,
        fua: bool,
        device_size: u64,
    ) -> super::error::CommandResult<()> {
        use super::error::CommandError;

        if offset + length as u64 > device_size {
            let mut data = BytesMut::zeroed(length as usize);
            let _ = self.reader.read_exact(&mut data).await;
            return Err(CommandError::NoSpace);
        }

        if length == 0 {
            return Ok(());
        }

        let mut data = BytesMut::zeroed(length as usize);
        self.reader
            .read_exact(&mut data)
            .await
            .map_err(|_| CommandError::IoError)?;

        handler.write(offset, &data, fua)
    }

    async fn send_read_result(
        &mut self,
        cookie: u64,
        result: super::error::CommandResult<bytes::Bytes>,
    ) {
        match result {
            Ok(data) => {
                if let Err(e) = self.send_simple_reply(cookie, NBD_SUCCESS, &data).await {
                    debug!("Failed to send reply: {:?}", e);
                }
            }
            Err(e) => {
                let _ = self.send_simple_reply(cookie, e.to_errno(), &[]).await;
            }
        }
    }

    async fn send_unit_result(&mut self, cookie: u64, result: super::error::CommandResult<()>) {
        match result {
            Ok(()) => {
                if let Err(e) = self.send_simple_reply(cookie, NBD_SUCCESS, &[]).await {
                    debug!("Failed to send reply: {:?}", e);
                }
            }
            Err(e) => {
                let _ = self.send_simple_reply(cookie, e.to_errno(), &[]).await;
            }
        }
    }

    async fn send_simple_reply(&mut self, cookie: u64, error: u32, data: &[u8]) -> Result<()> {
        let reply = NBDSimpleReply::new(cookie, error);
        let reply_bytes = reply.to_bytes()?;
        self.writer.write_all(&reply_bytes).await?;
        if !data.is_empty() {
            self.writer.write_all(data).await?;
        }
        self.writer.flush().await?;
        Ok(())
    }
}
