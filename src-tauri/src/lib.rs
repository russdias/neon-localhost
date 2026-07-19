use percent_encoding::percent_decode_str;
use postgres_protocol::authentication::sasl::{ChannelBinding, ScramSha256, SCRAM_SHA_256};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        Arc, Mutex as StdMutex,
    },
    time::{SystemTime, UNIX_EPOCH},
};
use tauri::State;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{Mutex, Semaphore},
    task::JoinSet,
    time::{sleep, timeout, Duration},
};
use tokio_native_tls::{TlsConnector, TlsStream};
use url::Url;

const NEON_NEW_ENDPOINT: &str = "https://neon.new/api/v1/database";
const LOCAL_PORT: u16 = 5432;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(20);
const QUERY_TIMEOUT: Duration = Duration::from_secs(20);
const HTTP_TIMEOUT: Duration = Duration::from_secs(20);
const LOCAL_STARTUP_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_CONCURRENT_CONNECTIONS: usize = 64;
const MAX_STARTUP_PACKET_SIZE: usize = 64 * 1024;
const POSTGRES_PROTOCOL_V3: u32 = 196_608;
const SSL_REQUEST_CODE: u32 = 80_877_103;
const GSSENC_REQUEST_CODE: u32 = 80_877_104;
const CANCEL_REQUEST_CODE: u32 = 80_877_102;
const USER_AGENT: &str = concat!("neon-localhost/", env!("CARGO_PKG_VERSION"));

fn read_u32(bytes: &[u8], offset: usize) -> Option<u32> {
    Some(u32::from_be_bytes(
        bytes.get(offset..offset.checked_add(4)?)?.try_into().ok()?,
    ))
}

fn validated_claim_url(
    value: &str,
    expected_host: &str,
    expected_path: &str,
) -> Result<Url, String> {
    let url = Url::parse(value).map_err(|_| "Neon returned an invalid claim URL".to_string())?;
    let path_matches = if expected_path.ends_with('/') {
        url.path().starts_with(expected_path)
    } else {
        url.path() == expected_path
    };
    if url.scheme() != "https" || url.host_str() != Some(expected_host) || !path_matches {
        return Err("Neon returned an unexpected claim destination".to_string());
    }
    Ok(url)
}

struct ProxyState {
    lifecycle: Mutex<()>,
    task: Mutex<Option<tauri::async_runtime::JoinHandle<()>>>,
    metrics: Arc<ProxyMetrics>,
    database: Mutex<Option<LocalDatabase>>,
    upstream: Mutex<Option<Upstream>>,
    running: AtomicBool,
    generation: AtomicU64,
    cancel_registry: Arc<CancelRegistry>,
    http_client: reqwest::Client,
}

impl ProxyState {
    fn new() -> Result<Self, String> {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let http_client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(HTTP_TIMEOUT)
            .user_agent(USER_AGENT)
            .build()
            .map_err(|error| format!("Could not initialize the network client: {error}"))?;
        Ok(Self {
            lifecycle: Mutex::new(()),
            task: Mutex::new(None),
            metrics: Arc::new(ProxyMetrics::default()),
            database: Mutex::new(None),
            upstream: Mutex::new(None),
            running: AtomicBool::new(false),
            generation: AtomicU64::new(0),
            cancel_registry: Arc::new(CancelRegistry::default()),
            http_client,
        })
    }
}

#[derive(Default)]
struct ProxyMetrics {
    active_connections: AtomicUsize,
    total_connections: AtomicU64,
    bytes_to_neon: AtomicU64,
    bytes_from_neon: AtomicU64,
    failed_connections: AtomicU64,
    started_at: AtomicU64,
    last_activity: AtomicU64,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ProxyMetricsSnapshot {
    running: bool,
    active_connections: usize,
    total_connections: u64,
    bytes_to_neon: u64,
    bytes_from_neon: u64,
    failed_connections: u64,
    started_at: u64,
    last_activity: u64,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct DatabaseStorage {
    used_bytes: u64,
    limit_bytes: u64,
}

fn unix_time_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

impl ProxyMetrics {
    fn reset(&self) {
        self.active_connections.store(0, Ordering::Relaxed);
        self.total_connections.store(0, Ordering::Relaxed);
        self.bytes_to_neon.store(0, Ordering::Relaxed);
        self.bytes_from_neon.store(0, Ordering::Relaxed);
        self.failed_connections.store(0, Ordering::Relaxed);
        self.started_at.store(unix_time_millis(), Ordering::Relaxed);
        self.last_activity.store(0, Ordering::Relaxed);
    }

    fn snapshot(&self, running: bool) -> ProxyMetricsSnapshot {
        ProxyMetricsSnapshot {
            running,
            active_connections: self.active_connections.load(Ordering::Relaxed),
            total_connections: self.total_connections.load(Ordering::Relaxed),
            bytes_to_neon: self.bytes_to_neon.load(Ordering::Relaxed),
            bytes_from_neon: self.bytes_from_neon.load(Ordering::Relaxed),
            failed_connections: self.failed_connections.load(Ordering::Relaxed),
            started_at: self.started_at.load(Ordering::Relaxed),
            last_activity: self.last_activity.load(Ordering::Relaxed),
        }
    }
}

struct ActiveConnection(Arc<ProxyMetrics>);

impl ActiveConnection {
    fn begin(metrics: Arc<ProxyMetrics>) -> Self {
        metrics.active_connections.fetch_add(1, Ordering::Relaxed);
        metrics.total_connections.fetch_add(1, Ordering::Relaxed);
        metrics
            .last_activity
            .store(unix_time_millis(), Ordering::Relaxed);
        Self(metrics)
    }
}

impl Drop for ActiveConnection {
    fn drop(&mut self) {
        let _ = self.0.active_connections.fetch_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |active| Some(active.saturating_sub(1)),
        );
        self.0
            .last_activity
            .store(unix_time_millis(), Ordering::Relaxed);
    }
}

#[derive(Debug, Deserialize)]
struct NeonNewResponse {
    status: String,
    neon_project_id: String,
    connection_string: String,
    claim_url: String,
    expires_at: String,
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct LocalDatabase {
    status: String,
    project_id: String,
    local_url: String,
    remote_url: String,
    claim_url: String,
    expires_at: String,
    port: u16,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Upstream {
    host: String,
    port: u16,
    user: String,
    password: String,
    database: String,
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
struct CancelKey {
    process_id: u32,
    secret_key: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct CancelTarget {
    host: String,
    port: u16,
}

#[derive(Default)]
struct CancelRegistry(StdMutex<HashMap<CancelKey, CancelTarget>>);

impl CancelRegistry {
    fn register(self: &Arc<Self>, key: CancelKey, upstream: &Upstream) -> CancelRegistration {
        let target = CancelTarget {
            host: upstream.host.clone(),
            port: upstream.port,
        };
        self.0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(key, target.clone());
        CancelRegistration {
            registry: self.clone(),
            key,
            target,
        }
    }

    fn target(&self, key: CancelKey) -> Option<CancelTarget> {
        self.0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(&key)
            .cloned()
    }

    fn clear(&self) {
        self.0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clear();
    }
}

struct CancelRegistration {
    registry: Arc<CancelRegistry>,
    key: CancelKey,
    target: CancelTarget,
}

impl Drop for CancelRegistration {
    fn drop(&mut self) {
        let mut registry = self
            .registry
            .0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if registry.get(&self.key) == Some(&self.target) {
            registry.remove(&self.key);
        }
    }
}

fn parse_upstream_for_port(
    connection_string: &str,
    local_port: u16,
) -> Result<(Upstream, String), String> {
    let mut url = Url::parse(connection_string)
        .map_err(|error| format!("Neon returned an invalid connection URL: {error}"))?;

    if !matches!(url.scheme(), "postgres" | "postgresql") {
        return Err("Neon returned an unsupported connection URL".to_string());
    }

    let host = url
        .host_str()
        .ok_or_else(|| "The Neon connection URL does not contain a hostname".to_string())?
        .to_ascii_lowercase();
    if host != "neon.tech" && !host.ends_with(".neon.tech") {
        return Err("Neon returned a connection URL outside neon.tech".to_string());
    }
    let port = url.port().unwrap_or(5432);
    let decode = |value: &str| percent_decode_str(value).decode_utf8_lossy().into_owned();
    let user = decode(url.username());
    let password = decode(
        url.password()
            .ok_or_else(|| "The Neon connection URL does not contain a password".to_string())?,
    );
    let database = decode(url.path().trim_start_matches('/'));
    if user.is_empty() || database.is_empty() {
        return Err("The Neon connection URL is missing a user or database".to_string());
    }
    if user.contains('\0') || database.contains('\0') {
        return Err("The Neon connection URL contains invalid credentials".to_string());
    }
    if url.fragment().is_some() {
        return Err("The Neon connection URL contains an unexpected fragment".to_string());
    }
    url.set_host(Some("localhost"))
        .map_err(|_| "Could not create the local connection URL".to_string())?;
    url.set_port(Some(local_port))
        .map_err(|_| "Could not set the local proxy port".to_string())?;

    url.set_username("")
        .map_err(|_| "Could not create the passwordless local URL".to_string())?;
    url.set_password(None)
        .map_err(|_| "Could not create the passwordless local URL".to_string())?;
    url.set_query(None);
    url.query_pairs_mut().append_pair("sslmode", "disable");

    Ok((
        Upstream {
            host,
            port,
            user,
            password,
            database,
        },
        url.to_string(),
    ))
}

fn parse_upstream(connection_string: &str) -> Result<(Upstream, String), String> {
    parse_upstream_for_port(connection_string, LOCAL_PORT)
}

async fn connect_endpoint(host: &str, port: u16) -> Result<TcpStream, String> {
    let stream = timeout(CONNECT_TIMEOUT, TcpStream::connect((host, port)))
        .await
        .map_err(|_| "Timed out while connecting to Neon".to_string())?
        .map_err(|error| format!("Could not reach Neon: {error}"))?;
    stream
        .set_nodelay(true)
        .map_err(|error| format!("Could not configure the Neon connection: {error}"))?;
    Ok(stream)
}

async fn connect_tls_endpoint(host: &str, port: u16) -> Result<TlsStream<TcpStream>, String> {
    let mut stream = connect_endpoint(host, port).await?;
    stream
        .write_all(&[0, 0, 0, 8, 4, 210, 22, 47])
        .await
        .map_err(|error| format!("Could not request Neon TLS: {error}"))?;
    let mut tls_response = [0_u8; 1];
    stream
        .read_exact(&mut tls_response)
        .await
        .map_err(|error| format!("Neon did not answer the TLS request: {error}"))?;
    if tls_response[0] != b'S' {
        return Err("Neon refused the secure database connection".to_string());
    }

    let native_connector = native_tls::TlsConnector::builder()
        .min_protocol_version(Some(native_tls::Protocol::Tlsv12))
        .build()
        .map_err(|error| format!("Could not initialize TLS: {error}"))?;
    TlsConnector::from(native_connector)
        .connect(host, stream)
        .await
        .map_err(|error| format!("Could not establish Neon TLS: {error}"))
}

async fn read_backend_message<R: AsyncRead + Unpin>(stream: &mut R) -> Result<Vec<u8>, String> {
    let mut header = [0_u8; 5];
    stream
        .read_exact(&mut header)
        .await
        .map_err(|error| format!("Could not read Neon response: {error}"))?;
    let length = u32::from_be_bytes([header[1], header[2], header[3], header[4]]) as usize;
    if !(4..=16 * 1024 * 1024).contains(&length) {
        return Err("Neon returned an invalid Postgres message".to_string());
    }
    let mut message = Vec::with_capacity(length + 1);
    message.extend_from_slice(&header);
    message.resize(length + 1, 0);
    stream
        .read_exact(&mut message[5..])
        .await
        .map_err(|error| format!("Could not read Neon response: {error}"))?;
    Ok(message)
}

fn parse_storage_row(message: &[u8]) -> Result<DatabaseStorage, String> {
    if message.len() < 7 || message[0] != b'D' {
        return Err("Postgres returned an invalid storage result".to_string());
    }
    let columns = u16::from_be_bytes([message[5], message[6]]);
    if columns != 2 {
        return Err("Postgres returned an unexpected storage result".to_string());
    }

    let mut cursor = 7;
    let mut values = [0_u64; 2];
    for value in &mut values {
        if cursor + 4 > message.len() {
            return Err("Postgres returned an incomplete storage result".to_string());
        }
        let length = i32::from_be_bytes([
            message[cursor],
            message[cursor + 1],
            message[cursor + 2],
            message[cursor + 3],
        ]);
        cursor += 4;
        let end = (length >= 0)
            .then(|| cursor.checked_add(length as usize))
            .flatten()
            .filter(|end| *end <= message.len())
            .ok_or_else(|| "Postgres returned an invalid storage value".to_string())?;
        let text = std::str::from_utf8(&message[cursor..end])
            .map_err(|_| "Postgres returned a non-text storage value".to_string())?;
        *value = text
            .parse()
            .map_err(|_| "Postgres returned a non-numeric storage value".to_string())?;
        cursor = end;
    }
    if cursor != message.len() {
        return Err("Postgres returned trailing storage data".to_string());
    }
    Ok(DatabaseStorage {
        used_bytes: values[0],
        limit_bytes: values[1],
    })
}

async fn read_database_storage_inner(upstream: &Upstream) -> Result<DatabaseStorage, String> {
    let (mut stream, _) = authenticate_upstream(upstream).await?;
    let query = "select pg_database_size(current_database()), pg_size_bytes(current_setting('neon.max_cluster_size'))";
    let mut message = Vec::with_capacity(query.len() + 6);
    message.push(b'Q');
    message.extend_from_slice(&((query.len() + 5) as u32).to_be_bytes());
    message.extend_from_slice(query.as_bytes());
    message.push(0);
    stream
        .write_all(&message)
        .await
        .map_err(|error| format!("Could not request database storage: {error}"))?;

    let mut storage = None;
    loop {
        let response = read_backend_message(&mut stream).await?;
        match response[0] {
            b'D' => storage = Some(parse_storage_row(&response)?),
            b'E' => return Err("Neon could not report database storage".to_string()),
            b'Z' => {
                return storage.ok_or_else(|| "Neon did not return database storage".to_string())
            }
            _ => {}
        }
    }
}

async fn read_database_storage(upstream: &Upstream) -> Result<DatabaseStorage, String> {
    timeout(QUERY_TIMEOUT, read_database_storage_inner(upstream))
        .await
        .map_err(|_| "Timed out while reading database storage".to_string())?
}

fn password_message(payload: &[u8]) -> Vec<u8> {
    let mut message = Vec::with_capacity(payload.len() + 5);
    message.push(b'p');
    message.extend_from_slice(&((payload.len() + 4) as u32).to_be_bytes());
    message.extend_from_slice(payload);
    message
}

async fn authenticate_upstream_inner(
    upstream: &Upstream,
) -> Result<(TlsStream<TcpStream>, Vec<Vec<u8>>), String> {
    let mut stream = connect_tls_endpoint(&upstream.host, upstream.port).await?;

    let mut startup_payload = Vec::new();
    startup_payload.extend_from_slice(&196_608_u32.to_be_bytes());
    for (key, value) in [
        ("user", upstream.user.as_str()),
        ("database", upstream.database.as_str()),
        ("application_name", "neon-localhost"),
    ] {
        startup_payload.extend_from_slice(key.as_bytes());
        startup_payload.push(0);
        startup_payload.extend_from_slice(value.as_bytes());
        startup_payload.push(0);
    }
    startup_payload.push(0);
    let mut startup = Vec::with_capacity(startup_payload.len() + 4);
    startup.extend_from_slice(&((startup_payload.len() + 4) as u32).to_be_bytes());
    startup.extend_from_slice(&startup_payload);
    stream
        .write_all(&startup)
        .await
        .map_err(|error| format!("Could not start the Neon session: {error}"))?;

    let mut scram: Option<ScramSha256> = None;
    let mut startup_messages = Vec::new();
    loop {
        let message = read_backend_message(&mut stream).await?;
        match message[0] {
            b'R' => {
                if message.len() < 9 {
                    return Err("Neon returned an invalid authentication message".to_string());
                }
                let auth_type =
                    u32::from_be_bytes([message[5], message[6], message[7], message[8]]);
                match auth_type {
                    0 => startup_messages.push(message),
                    10 => {
                        if !message[9..]
                            .split(|byte| *byte == 0)
                            .any(|mechanism| mechanism == SCRAM_SHA_256.as_bytes())
                        {
                            return Err(
                                "Neon did not offer supported password authentication".to_string()
                            );
                        }
                        let client = ScramSha256::new(
                            upstream.password.as_bytes(),
                            ChannelBinding::unsupported(),
                        );
                        let mut payload = Vec::new();
                        payload.extend_from_slice(SCRAM_SHA_256.as_bytes());
                        payload.push(0);
                        payload.extend_from_slice(&(client.message().len() as u32).to_be_bytes());
                        payload.extend_from_slice(client.message());
                        stream
                            .write_all(&password_message(&payload))
                            .await
                            .map_err(|error| {
                                format!("Could not authenticate with Neon: {error}")
                            })?;
                        scram = Some(client);
                    }
                    11 => {
                        let client = scram
                            .as_mut()
                            .ok_or_else(|| "Unexpected Neon authentication response".to_string())?;
                        client.update(&message[9..]).map_err(|error| {
                            format!("Invalid Neon authentication response: {error}")
                        })?;
                        stream
                            .write_all(&password_message(client.message()))
                            .await
                            .map_err(|error| {
                                format!("Could not authenticate with Neon: {error}")
                            })?;
                    }
                    12 => {
                        scram
                            .as_mut()
                            .ok_or_else(|| "Unexpected Neon authentication response".to_string())?
                            .finish(&message[9..])
                            .map_err(|error| {
                                format!("Neon password verification failed: {error}")
                            })?;
                    }
                    _ => {
                        return Err(format!(
                            "Neon requested unsupported authentication type {auth_type}"
                        ))
                    }
                }
            }
            b'E' => return Err("Neon rejected the database credentials".to_string()),
            b'Z' => {
                startup_messages.push(message);
                return Ok((stream, startup_messages));
            }
            _ => startup_messages.push(message),
        }
    }
}

async fn authenticate_upstream(
    upstream: &Upstream,
) -> Result<(TlsStream<TcpStream>, Vec<Vec<u8>>), String> {
    timeout(HANDSHAKE_TIMEOUT, authenticate_upstream_inner(upstream))
        .await
        .map_err(|_| "Timed out while establishing the Neon database session".to_string())?
}

fn backend_cancel_key(message: &[u8]) -> Option<CancelKey> {
    if message.len() != 13 || message[0] != b'K' || read_u32(message, 1) != Some(12) {
        return None;
    }
    Some(CancelKey {
        process_id: read_u32(message, 5)?,
        secret_key: read_u32(message, 9)?,
    })
}

async fn forward_cancel_inner(target: &CancelTarget, key: CancelKey) -> Result<(), String> {
    let mut stream = connect_tls_endpoint(&target.host, target.port).await?;
    let mut packet = [0_u8; 16];
    packet[..4].copy_from_slice(&16_u32.to_be_bytes());
    packet[4..8].copy_from_slice(&CANCEL_REQUEST_CODE.to_be_bytes());
    packet[8..12].copy_from_slice(&key.process_id.to_be_bytes());
    packet[12..].copy_from_slice(&key.secret_key.to_be_bytes());
    stream
        .write_all(&packet)
        .await
        .map_err(|error| format!("Could not forward the query cancellation to Neon: {error}"))?;
    stream
        .shutdown()
        .await
        .map_err(|error| format!("Could not finish the query cancellation: {error}"))
}

async fn forward_cancel(target: &CancelTarget, key: CancelKey) -> Result<(), String> {
    timeout(HANDSHAKE_TIMEOUT, forward_cancel_inner(target, key))
        .await
        .map_err(|_| "Timed out while forwarding the query cancellation".to_string())?
}

async fn relay_direction<R, W>(
    reader: &mut R,
    writer: &mut W,
    byte_counter: &AtomicU64,
    last_activity: &AtomicU64,
) -> Result<(), String>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut buffer = vec![0_u8; 16 * 1024];
    loop {
        let read = reader
            .read(&mut buffer)
            .await
            .map_err(|error| format!("The database connection was interrupted: {error}"))?;
        if read == 0 {
            writer
                .shutdown()
                .await
                .map_err(|error| format!("Could not close the database connection: {error}"))?;
            return Ok(());
        }
        writer
            .write_all(&buffer[..read])
            .await
            .map_err(|error| format!("The database connection was interrupted: {error}"))?;
        byte_counter.fetch_add(read as u64, Ordering::Relaxed);
        last_activity.store(unix_time_millis(), Ordering::Relaxed);
    }
}

async fn proxy_connection(
    mut local: TcpStream,
    upstream: Upstream,
    metrics: Arc<ProxyMetrics>,
    cancel_registry: Arc<CancelRegistry>,
) -> Result<(), String> {
    local
        .set_nodelay(true)
        .map_err(|error| format!("Could not configure the local connection: {error}"))?;
    let startup = timeout(LOCAL_STARTUP_TIMEOUT, read_local_startup(&mut local))
        .await
        .map_err(|_| "Timed out waiting for the local Postgres startup packet".to_string())??;

    if let LocalStartup::Cancel(key) = startup {
        if let Some(target) = cancel_registry.target(key) {
            forward_cancel(&target, key).await?;
        }
        return Ok(());
    }

    let (remote, startup_messages) = authenticate_upstream(&upstream).await?;
    let _cancel_registration = startup_messages
        .iter()
        .find_map(|message| backend_cancel_key(message))
        .map(|key| cancel_registry.register(key, &upstream));
    for message in startup_messages {
        local
            .write_all(&message)
            .await
            .map_err(|error| format!("Could not initialize local Postgres client: {error}"))?;
    }

    let (mut local_read, mut local_write) = tokio::io::split(local);
    let (mut remote_read, mut remote_write) = tokio::io::split(remote);
    tokio::try_join!(
        relay_direction(
            &mut local_read,
            &mut remote_write,
            &metrics.bytes_to_neon,
            &metrics.last_activity,
        ),
        relay_direction(
            &mut remote_read,
            &mut local_write,
            &metrics.bytes_from_neon,
            &metrics.last_activity,
        )
    )?;
    Ok(())
}

#[derive(Debug, PartialEq, Eq)]
enum LocalStartup {
    Session,
    Cancel(CancelKey),
}

async fn read_local_startup<S>(local: &mut S) -> Result<LocalStartup, String>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut first_packet = [0_u8; 8];
    local
        .read_exact(&mut first_packet)
        .await
        .map_err(|error| format!("Could not read local Postgres startup: {error}"))?;

    while read_u32(&first_packet, 0) == Some(8)
        && matches!(
            read_u32(&first_packet, 4),
            Some(SSL_REQUEST_CODE) | Some(GSSENC_REQUEST_CODE)
        )
    {
        // The loopback connection uses local trust authentication. Encryption is
        // applied by the proxy for the remote hop to Neon.
        local
            .write_all(b"N")
            .await
            .map_err(|error| format!("Could not answer local encryption request: {error}"))?;
        local
            .read_exact(&mut first_packet)
            .await
            .map_err(|error| format!("Could not read local Postgres startup: {error}"))?;
    }

    let packet_length = read_u32(&first_packet, 0)
        .ok_or_else(|| "Invalid local Postgres startup packet".to_string())?
        as usize;
    let request_code = read_u32(&first_packet, 4)
        .ok_or_else(|| "Invalid local Postgres startup packet".to_string())?;
    if packet_length == 16 && request_code == CANCEL_REQUEST_CODE {
        let mut cancel_payload = [0_u8; 8];
        local
            .read_exact(&mut cancel_payload)
            .await
            .map_err(|error| format!("Could not read local cancellation request: {error}"))?;
        return Ok(LocalStartup::Cancel(CancelKey {
            process_id: read_u32(&cancel_payload, 0)
                .ok_or_else(|| "Invalid local cancellation request".to_string())?,
            secret_key: read_u32(&cancel_payload, 4)
                .ok_or_else(|| "Invalid local cancellation request".to_string())?,
        }));
    }
    if request_code != POSTGRES_PROTOCOL_V3
        || !(9..=MAX_STARTUP_PACKET_SIZE).contains(&packet_length)
    {
        return Err("Invalid local Postgres startup packet".to_string());
    }
    let mut ignored_startup = vec![0_u8; packet_length - 8];
    local
        .read_exact(&mut ignored_startup)
        .await
        .map_err(|error| format!("Could not read local Postgres startup: {error}"))?;

    Ok(LocalStartup::Session)
}

struct LocalListeners {
    ipv4: TcpListener,
    ipv6: Option<TcpListener>,
}

async fn bind_local_listeners() -> Result<LocalListeners, String> {
    let address = ("127.0.0.1", LOCAL_PORT);
    let mut last_error = None;

    for _ in 0..12 {
        match TcpListener::bind(address).await {
            Ok(ipv4) => {
                let ipv6 = match TcpListener::bind(("::1", LOCAL_PORT)).await {
                    Ok(listener) => Some(listener),
                    Err(error)
                        if matches!(
                            error.kind(),
                            std::io::ErrorKind::AddrNotAvailable | std::io::ErrorKind::Unsupported
                        ) =>
                    {
                        None
                    }
                    Err(error) => {
                        return Err(format!(
                            "Port {LOCAL_PORT} is unavailable on IPv6 localhost: {error}"
                        ));
                    }
                };
                return Ok(LocalListeners { ipv4, ipv6 });
            }
            Err(error) => {
                last_error = Some(error);
                sleep(Duration::from_millis(50)).await;
            }
        }
    }

    Err(format!(
        "Port {LOCAL_PORT} is already in use. Stop the other local Postgres service and try again: {}",
        last_error.expect("a bind attempt always sets an error")
    ))
}

async fn serve_proxy(
    listeners: LocalListeners,
    upstream: Upstream,
    metrics: Arc<ProxyMetrics>,
    cancel_registry: Arc<CancelRegistry>,
) {
    let mut connections = JoinSet::new();
    let connection_limit = Arc::new(Semaphore::new(MAX_CONCURRENT_CONNECTIONS));
    loop {
        let accepted = if let Some(ipv6) = listeners.ipv6.as_ref() {
            tokio::select! {
                result = listeners.ipv4.accept() => result,
                result = ipv6.accept() => result,
            }
        } else {
            listeners.ipv4.accept().await
        };

        let Ok((local, _)) = accepted else { break };
        let Ok(permit) = connection_limit.clone().try_acquire_owned() else {
            metrics.failed_connections.fetch_add(1, Ordering::Relaxed);
            continue;
        };
        let connection_upstream = upstream.clone();
        let connection_metrics = metrics.clone();
        let connection_cancel_registry = cancel_registry.clone();
        connections.spawn(async move {
            let _permit = permit;
            let _active = ActiveConnection::begin(connection_metrics.clone());
            match proxy_connection(
                local,
                connection_upstream,
                connection_metrics.clone(),
                connection_cancel_registry,
            )
            .await
            {
                Ok(()) => {}
                Err(_) => {
                    connection_metrics
                        .failed_connections
                        .fetch_add(1, Ordering::Relaxed);
                }
            }
        });
        while connections.try_join_next().is_some() {}
    }
    connections.shutdown().await;
}

async fn stop_existing_proxy(state: &Arc<ProxyState>) {
    state.generation.fetch_add(1, Ordering::AcqRel);
    if let Some(task) = state.task.lock().await.take() {
        task.abort();
        let _ = task.await;
    }
    state.running.store(false, Ordering::Release);
    *state.database.lock().await = None;
    *state.upstream.lock().await = None;
    state.cancel_registry.clear();
}

fn concise_http_error(detail: &str) -> String {
    const MAX_CHARS: usize = 300;
    let normalized = detail.split_whitespace().collect::<Vec<_>>().join(" ");
    if normalized.chars().count() <= MAX_CHARS {
        normalized
    } else {
        format!(
            "{}…",
            normalized.chars().take(MAX_CHARS).collect::<String>()
        )
    }
}

#[tauri::command]
async fn create_database(state: State<'_, Arc<ProxyState>>) -> Result<LocalDatabase, String> {
    let _lifecycle = state.lifecycle.lock().await;
    // Serialize lifecycle changes so overlapping UI or IPC requests cannot leave
    // the listener and the displayed database out of sync.
    stop_existing_proxy(state.inner()).await;

    let listeners = bind_local_listeners().await?;
    let response = state
        .http_client
        .post(NEON_NEW_ENDPOINT)
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .json(&serde_json::json!({ "ref": "neon-localhost" }))
        .send()
        .await
        .map_err(|error| format!("Could not contact neon.new: {error}"))?;

    if !response.status().is_success() {
        let status = response.status();
        let detail = concise_http_error(&response.text().await.unwrap_or_default());
        return Err(format!("neon.new returned {status}: {detail}"));
    }

    let database: NeonNewResponse = response
        .json()
        .await
        .map_err(|error| format!("Could not read the neon.new response: {error}"))?;
    let (upstream, local_url) = parse_upstream(&database.connection_string)?;

    // Authenticate now, so a successful UI state means the complete path is live.
    let (check, _) = authenticate_upstream(&upstream).await?;
    drop(check);

    let local_database = LocalDatabase {
        status: database.status,
        project_id: database.neon_project_id,
        local_url,
        remote_url: database.connection_string,
        claim_url: database.claim_url,
        expires_at: database.expires_at,
        port: LOCAL_PORT,
    };
    let proxy_upstream = upstream.clone();
    *state.database.lock().await = Some(local_database.clone());
    *state.upstream.lock().await = Some(upstream);
    state.metrics.reset();
    let generation = state.generation.fetch_add(1, Ordering::AcqRel) + 1;
    state.running.store(true, Ordering::Release);
    let run_state = state.inner().clone();
    let task = tauri::async_runtime::spawn(async move {
        serve_proxy(
            listeners,
            proxy_upstream,
            run_state.metrics.clone(),
            run_state.cancel_registry.clone(),
        )
        .await;
        if run_state.generation.load(Ordering::Acquire) == generation {
            run_state.running.store(false, Ordering::Release);
            *run_state.database.lock().await = None;
            *run_state.upstream.lock().await = None;
            run_state.cancel_registry.clear();
        }
    });
    *state.task.lock().await = Some(task);
    Ok(local_database)
}

#[tauri::command]
async fn stop_proxy(state: State<'_, Arc<ProxyState>>) -> Result<(), String> {
    let _lifecycle = state.lifecycle.lock().await;
    stop_existing_proxy(state.inner()).await;
    Ok(())
}

#[tauri::command]
async fn current_database(
    state: State<'_, Arc<ProxyState>>,
) -> Result<Option<LocalDatabase>, String> {
    if !state.running.load(Ordering::Acquire) {
        *state.database.lock().await = None;
        return Ok(None);
    }
    Ok(state.database.lock().await.clone())
}

#[tauri::command]
fn proxy_metrics(state: State<'_, Arc<ProxyState>>) -> ProxyMetricsSnapshot {
    state
        .metrics
        .snapshot(state.running.load(Ordering::Acquire))
}

#[tauri::command]
async fn database_storage(state: State<'_, Arc<ProxyState>>) -> Result<DatabaseStorage, String> {
    if !state.running.load(Ordering::Acquire) {
        return Err("No database is running".to_string());
    }
    let upstream = state
        .upstream
        .lock()
        .await
        .clone()
        .ok_or_else(|| "No database is running".to_string())?;
    read_database_storage(&upstream).await
}

#[tauri::command]
async fn resolve_claim_url(
    state: State<'_, Arc<ProxyState>>,
    claim_url: String,
) -> Result<String, String> {
    let claim_url = validated_claim_url(&claim_url, "neon.new", "/claim/")?;
    let response = state
        .http_client
        .get(claim_url.clone())
        .send()
        .await
        .map_err(|error| format!("Could not start the Neon claim flow: {error}"))?;

    if !response.status().is_redirection() {
        return Err(format!(
            "Neon could not start the claim flow (HTTP {}). The database is still available locally.",
            response.status().as_u16()
        ));
    }

    let location = response
        .headers()
        .get(reqwest::header::LOCATION)
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| "Neon did not provide a claim destination".to_string())?;
    let destination = claim_url
        .join(location)
        .map_err(|_| "Neon returned an invalid claim destination".to_string())?;
    let destination = validated_claim_url(destination.as_str(), "console.neon.tech", "/app/claim")?;
    Ok(destination.to_string())
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    let state = ProxyState::new().expect("could not initialize application state");
    tauri::Builder::default()
        .plugin(tauri_plugin_opener::init())
        .plugin(tauri_plugin_process::init())
        .plugin(tauri_plugin_updater::Builder::new().build())
        .manage(Arc::new(state))
        .invoke_handler(tauri::generate_handler![
            create_database,
            stop_proxy,
            current_database,
            proxy_metrics,
            database_storage,
            resolve_claim_url
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::Command;

    #[test]
    fn claim_urls_are_limited_to_neon_claim_pages() {
        assert!(validated_claim_url(
            "https://neon.new/claim/019f778c-cbe0-74bc-8ee6-84bff839d74c",
            "neon.new",
            "/claim/"
        )
        .is_ok());
        assert!(validated_claim_url(
            "https://console.neon.tech/app/claim?p=project&tr=transfer",
            "console.neon.tech",
            "/app/claim"
        )
        .is_ok());
        assert!(validated_claim_url(
            "https://example.com/app/claim?p=project",
            "console.neon.tech",
            "/app/claim"
        )
        .is_err());
        assert!(validated_claim_url(
            "http://console.neon.tech/app/claim?p=project",
            "console.neon.tech",
            "/app/claim"
        )
        .is_err());
        assert!(validated_claim_url(
            "https://console.neon.tech/app/claim-elsewhere?p=project",
            "console.neon.tech",
            "/app/claim"
        )
        .is_err());
    }

    #[test]
    fn application_state_initializes() {
        ProxyState::new().expect("application state");
    }

    #[test]
    fn postgres_storage_rows_are_parsed() {
        let mut row = vec![b'D', 0, 0, 0, 0, 0, 2];
        for value in [b"82386944".as_slice(), b"104857600".as_slice()] {
            row.extend_from_slice(&(value.len() as i32).to_be_bytes());
            row.extend_from_slice(value);
        }
        let length = (row.len() - 1) as u32;
        row[1..5].copy_from_slice(&length.to_be_bytes());

        let storage = parse_storage_row(&row).expect("valid storage row");
        assert_eq!(storage.used_bytes, 82_386_944);
        assert_eq!(storage.limit_bytes, 104_857_600);
        row.push(0);
        assert!(parse_storage_row(&row).is_err());
    }

    #[test]
    fn local_connection_urls_never_expose_neon_credentials() {
        let remote = "postgresql://neon%20user:p%40ssword@ep-example.neon.tech:5432/my%2Ddb?sslmode=require&channel_binding=require";
        let (upstream, local) = parse_upstream_for_port(remote, 15_432).expect("valid URL");

        assert_eq!(upstream.host, "ep-example.neon.tech");
        assert_eq!(upstream.user, "neon user");
        assert_eq!(upstream.password, "p@ssword");
        assert_eq!(upstream.database, "my-db");
        assert_eq!(
            local,
            "postgresql://localhost:15432/my%2Ddb?sslmode=disable"
        );
        assert!(!local.contains("neon"));
        assert!(!local.contains("password"));
    }

    #[test]
    fn upstream_error_details_are_bounded_and_single_line() {
        let detail = format!("first\nsecond {}", "x".repeat(400));
        let concise = concise_http_error(&detail);

        assert!(!concise.contains('\n'));
        assert!(concise.ends_with('…'));
        assert_eq!(concise.chars().count(), 301);
    }

    #[test]
    fn upstream_urls_are_restricted_to_postgres_on_neon() {
        for connection_string in [
            "https://user:password@ep-example.neon.tech/neondb",
            "postgresql://user:password@example.com/neondb",
            "postgresql://user:password@neon.tech.evil.example/neondb",
            "postgresql://:password@ep-example.neon.tech/neondb",
            "postgresql://user:password@ep-example.neon.tech/",
            "postgresql://user:password@ep-example.neon.tech/neondb#fragment",
            "postgresql://us%00er:password@ep-example.neon.tech/neondb",
        ] {
            assert!(
                parse_upstream(connection_string).is_err(),
                "accepted unsafe upstream URL: {connection_string}"
            );
        }
    }

    #[test]
    fn backend_key_data_is_parsed_strictly() {
        let mut message = vec![b'K'];
        message.extend_from_slice(&12_u32.to_be_bytes());
        message.extend_from_slice(&1234_u32.to_be_bytes());
        message.extend_from_slice(&5678_u32.to_be_bytes());
        assert_eq!(
            backend_cancel_key(&message),
            Some(CancelKey {
                process_id: 1234,
                secret_key: 5678,
            })
        );
        message.push(0);
        assert_eq!(backend_cancel_key(&message), None);
    }

    #[test]
    fn cancel_registration_is_removed_when_connection_ends() {
        let registry = Arc::new(CancelRegistry::default());
        let key = CancelKey {
            process_id: 1234,
            secret_key: 5678,
        };
        let upstream = Upstream {
            host: "ep-example.neon.tech".to_string(),
            port: 5432,
            user: "user".to_string(),
            password: "password".to_string(),
            database: "neondb".to_string(),
        };
        {
            let _registration = registry.register(key, &upstream);
            assert_eq!(
                registry.target(key),
                Some(CancelTarget {
                    host: upstream.host.clone(),
                    port: upstream.port,
                })
            );
        }
        assert_eq!(registry.target(key), None);
    }

    #[tokio::test]
    async fn local_startup_supports_ssl_negotiation_and_cancel_requests() {
        let (mut client, mut server) = tokio::io::duplex(64);
        let client_task = tokio::spawn(async move {
            client
                .write_all(&[0, 0, 0, 8, 4, 210, 22, 47])
                .await
                .expect("SSL request");
            let mut response = [0_u8; 1];
            client
                .read_exact(&mut response)
                .await
                .expect("SSL response");
            assert_eq!(response, [b'N']);
            client
                .write_all(&[0, 0, 0, 9, 0, 3, 0, 0, 0])
                .await
                .expect("startup packet");
        });
        assert_eq!(
            read_local_startup(&mut server)
                .await
                .expect("valid startup"),
            LocalStartup::Session
        );
        client_task.await.expect("client task");

        let (mut client, mut server) = tokio::io::duplex(32);
        let key = CancelKey {
            process_id: 1234,
            secret_key: 5678,
        };
        let client_task = tokio::spawn(async move {
            let mut packet = [0_u8; 16];
            packet[..4].copy_from_slice(&16_u32.to_be_bytes());
            packet[4..8].copy_from_slice(&CANCEL_REQUEST_CODE.to_be_bytes());
            packet[8..12].copy_from_slice(&key.process_id.to_be_bytes());
            packet[12..].copy_from_slice(&key.secret_key.to_be_bytes());
            client.write_all(&packet).await.expect("cancel request");
        });
        assert_eq!(
            read_local_startup(&mut server).await.expect("valid cancel"),
            LocalStartup::Cancel(key)
        );
        client_task.await.expect("client task");
    }

    #[tokio::test]
    async fn local_startup_rejects_unknown_protocols_and_oversized_packets() {
        for header in [[0, 0, 0, 9, 0, 4, 0, 0], [0, 1, 0, 1, 0, 3, 0, 0]] {
            let (mut client, mut server) = tokio::io::duplex(16);
            client.write_all(&header).await.expect("startup header");
            assert!(read_local_startup(&mut server).await.is_err());
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "requires NEON_LOCAL_TEST_URL, network access, and psql"]
    async fn live_proxy_accepts_a_real_postgres_query() {
        let connection_string = std::env::var("NEON_LOCAL_TEST_URL").expect("test URL");
        let listener = TcpListener::bind(("127.0.0.1", 0))
            .await
            .expect("local port");
        let local_port = listener.local_addr().expect("local address").port();
        let ipv6_listener = TcpListener::bind(("::1", local_port))
            .await
            .expect("IPv6 local port");
        let (upstream, local_url) =
            parse_upstream_for_port(&connection_string, local_port).expect("valid URL");

        let listener_task = tokio::spawn(serve_proxy(
            LocalListeners {
                ipv4: listener,
                ipv6: Some(ipv6_listener),
            },
            upstream,
            Arc::new(ProxyMetrics::default()),
            Arc::new(CancelRegistry::default()),
        ));

        // A broken client must not take down the listener.
        let mut malformed = TcpStream::connect(("127.0.0.1", local_port))
            .await
            .expect("malformed client");
        use tokio::io::AsyncWriteExt;
        malformed.write_all(b"not postgres").await.expect("junk");
        drop(malformed);

        let mut clients = Vec::new();
        for index in 0..12 {
            let client_url = local_url.clone();
            let statement = format!("stress_total_{}_{}", std::process::id(), index);
            clients.push(tokio::task::spawn_blocking(move || {
                let prepare = format!(
                    "prepare {statement}(int) as select sum(n) from stress where n <= $1"
                );
                let execute = format!("execute {statement}(5000)");
                Command::new("psql")
                    .env("PGCONNECT_TIMEOUT", "15")
                    .args([
                        &client_url,
                        "-Atq",
                        "-c",
                        "begin",
                        "-c",
                        "create temp table stress(n int, payload text)",
                        "-c",
                        "insert into stress select n, repeat(md5(n::text), 20) from generate_series(1,10000) n",
                        "-c",
                        &prepare,
                        "-c",
                        &execute,
                        "-c",
                        "select length(repeat('x',1000000))",
                        "-c",
                        "rollback",
                    ])
                    .output()
                    .expect("psql installed")
            }));
        }

        for client in clients {
            let output = client.await.expect("psql task");
            assert!(
                output.status.success(),
                "psql failed: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            assert_eq!(
                String::from_utf8_lossy(&output.stdout).trim(),
                "12502500\n1000000"
            );
        }

        // GUI-style connection: host, port, user and database only. No password,
        // SSL configuration, endpoint option, or Neon role is required locally.
        let gui_output = tokio::task::spawn_blocking(move || {
            let port = local_port.to_string();
            Command::new("psql")
                .env("PGCONNECT_TIMEOUT", "15")
                .args([
                    "-h",
                    "localhost",
                    "-p",
                    &port,
                    "-U",
                    "local",
                    "-d",
                    "neondb",
                    "-Atc",
                    "select current_database(), current_user",
                ])
                .output()
                .expect("psql installed")
        })
        .await
        .expect("psql task");
        assert!(
            gui_output.status.success(),
            "passwordless GUI-style connection failed: {}",
            String::from_utf8_lossy(&gui_output.stderr)
        );
        assert_eq!(
            String::from_utf8_lossy(&gui_output.stdout).trim(),
            "neondb|neondb_owner"
        );

        listener_task.abort();
    }
}
