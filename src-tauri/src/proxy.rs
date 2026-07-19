use crate::{
    model::Upstream,
    postgres::{
        authenticate_upstream, backend_cancel_key, forward_cancel, read_local_startup, CancelKey,
        CancelTarget, LocalStartup,
    },
};
use serde::Serialize;
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Semaphore,
    task::JoinSet,
    time::{sleep, Duration},
};

pub(crate) const LOCAL_PORT: u16 = 5432;
const MAX_CONCURRENT_CONNECTIONS: usize = 64;

#[derive(Default)]
pub(crate) struct ProxyMetrics {
    active_connections: AtomicUsize,
    total_connections: AtomicU64,
    bytes_to_neon: AtomicU64,
    bytes_from_neon: AtomicU64,
    failed_connections: AtomicU64,
    started_at: AtomicU64,
    last_activity: AtomicU64,
}

#[derive(Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ProxyMetricsSnapshot {
    running: bool,
    active_connections: usize,
    total_connections: u64,
    bytes_to_neon: u64,
    bytes_from_neon: u64,
    failed_connections: u64,
    started_at: u64,
    last_activity: u64,
}

fn unix_time_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

impl ProxyMetrics {
    pub(crate) fn reset(&self) {
        self.active_connections.store(0, Ordering::Relaxed);
        self.total_connections.store(0, Ordering::Relaxed);
        self.bytes_to_neon.store(0, Ordering::Relaxed);
        self.bytes_from_neon.store(0, Ordering::Relaxed);
        self.failed_connections.store(0, Ordering::Relaxed);
        self.started_at.store(unix_time_millis(), Ordering::Relaxed);
        self.last_activity.store(0, Ordering::Relaxed);
    }

    pub(crate) fn snapshot(&self, running: bool) -> ProxyMetricsSnapshot {
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

#[derive(Default)]
pub(crate) struct CancelRegistry(Mutex<HashMap<CancelKey, CancelTarget>>);

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

    pub(crate) fn clear(&self) {
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
    let mut buffer = [0_u8; 16 * 1024];
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
    match read_local_startup(&mut local).await? {
        LocalStartup::Cancel(key) => {
            if let Some(target) = cancel_registry.target(key) {
                forward_cancel(&target, key).await?;
            }
            return Ok(());
        }
        LocalStartup::Session => {}
    }

    let (remote, startup_messages) = authenticate_upstream(&upstream).await?;
    let _registration = startup_messages
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
            &metrics.last_activity
        ),
        relay_direction(
            &mut remote_read,
            &mut local_write,
            &metrics.bytes_from_neon,
            &metrics.last_activity
        )
    )?;
    Ok(())
}

pub(crate) struct LocalListeners {
    pub(crate) ipv4: TcpListener,
    pub(crate) ipv6: Option<TcpListener>,
}

pub(crate) async fn bind_local_listeners() -> Result<LocalListeners, String> {
    let mut last_error = None;
    for _ in 0..12 {
        match TcpListener::bind(("127.0.0.1", LOCAL_PORT)).await {
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
                        ))
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
    let detail = last_error
        .map(|error| error.to_string())
        .unwrap_or_else(|| "unknown bind error".to_string());
    Err(format!("Port {LOCAL_PORT} is already in use. Stop the other local Postgres service and try again: {detail}"))
}

pub(crate) async fn serve_proxy(
    listeners: LocalListeners,
    upstream: Upstream,
    metrics: Arc<ProxyMetrics>,
    cancel_registry: Arc<CancelRegistry>,
) {
    let mut connections = JoinSet::new();
    let connection_limit = Arc::new(Semaphore::new(MAX_CONCURRENT_CONNECTIONS));
    loop {
        let accepted = if let Some(ipv6) = listeners.ipv6.as_ref() {
            tokio::select! { result = listeners.ipv4.accept() => result, result = ipv6.accept() => result }
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
        let connection_registry = cancel_registry.clone();
        connections.spawn(async move {
            let _permit = permit;
            let _active = ActiveConnection::begin(connection_metrics.clone());
            if proxy_connection(
                local,
                connection_upstream,
                connection_metrics.clone(),
                connection_registry,
            )
            .await
            .is_err()
            {
                connection_metrics
                    .failed_connections
                    .fetch_add(1, Ordering::Relaxed);
            }
        });
        while connections.try_join_next().is_some() {}
    }
    connections.shutdown().await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::postgres::parse_upstream_for_port;
    use std::process::Command;

    fn upstream() -> Upstream {
        Upstream {
            host: "ep-example.neon.tech".into(),
            port: 5432,
            user: "user".into(),
            password: "password".into(),
            database: "neondb".into(),
        }
    }

    #[test]
    fn active_connection_updates_metrics_for_its_lifetime() {
        let metrics = Arc::new(ProxyMetrics::default());
        {
            let _active = ActiveConnection::begin(metrics.clone());
            let snapshot = metrics.snapshot(true);
            assert_eq!(snapshot.active_connections, 1);
            assert_eq!(snapshot.total_connections, 1);
            assert!(snapshot.last_activity > 0);
        }
        assert_eq!(metrics.snapshot(true).active_connections, 0);

        metrics.reset();
        let snapshot = metrics.snapshot(false);
        assert_eq!(snapshot.total_connections, 0);
        assert_eq!(snapshot.active_connections, 0);
        assert!(snapshot.started_at > 0);
    }

    #[test]
    fn cancel_registration_is_scoped_to_the_connection() {
        let registry = Arc::new(CancelRegistry::default());
        let key = CancelKey {
            process_id: 1234,
            secret_key: 5678,
        };
        {
            let _registration = registry.register(key, &upstream());
            assert_eq!(registry.target(key).expect("target").port, 5432);
        }
        assert_eq!(registry.target(key), None);
    }

    #[test]
    fn stale_cancel_registration_cannot_remove_its_replacement() {
        let registry = Arc::new(CancelRegistry::default());
        let key = CancelKey {
            process_id: 1234,
            secret_key: 5678,
        };
        let original = registry.register(key, &upstream());
        let mut replacement_upstream = upstream();
        replacement_upstream.host = "ep-replacement.neon.tech".into();
        let replacement = registry.register(key, &replacement_upstream);

        drop(original);
        assert_eq!(
            registry.target(key).expect("replacement").host,
            "ep-replacement.neon.tech"
        );
        drop(replacement);
        assert_eq!(registry.target(key), None);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "requires NEON_LOCAL_TEST_URL, network access, and psql"]
    async fn live_proxy_queries_and_cancels_against_neon() {
        let connection_string = std::env::var("NEON_LOCAL_TEST_URL").expect("test URL");
        let ipv4 = TcpListener::bind(("127.0.0.1", 0))
            .await
            .expect("local port");
        let local_port = ipv4.local_addr().expect("local address").port();
        let ipv6 = TcpListener::bind(("::1", local_port)).await.ok();
        let (upstream, local_url) =
            parse_upstream_for_port(&connection_string, local_port).expect("valid URL");
        let task = tokio::spawn(serve_proxy(
            LocalListeners { ipv4, ipv6 },
            upstream,
            Arc::new(ProxyMetrics::default()),
            Arc::new(CancelRegistry::default()),
        ));

        let query_url = local_url.clone();
        let query = tokio::task::spawn_blocking(move || {
            Command::new("psql")
                .env("PGCONNECT_TIMEOUT", "15")
                .args([&query_url, "-Atc", "select 42"])
                .output()
                .expect("psql installed")
        })
        .await
        .expect("query task");
        assert!(
            query.status.success(),
            "{}",
            String::from_utf8_lossy(&query.stderr)
        );
        assert_eq!(String::from_utf8_lossy(&query.stdout).trim(), "42");

        let cancellation = tokio::task::spawn_blocking(move || {
            Command::new("sh")
                .arg("-c")
                .arg("psql \"$DATABASE_URL\" -v ON_ERROR_STOP=1 -c 'select pg_sleep(30)' & pid=$!; sleep 1; kill -INT \"$pid\"; wait \"$pid\"")
                .env("DATABASE_URL", local_url)
                .env("PGCONNECT_TIMEOUT", "15")
                .output()
                .expect("cancellation process")
        })
        .await
        .expect("cancellation task");
        assert!(
            !cancellation.status.success(),
            "canceled query unexpectedly succeeded"
        );
        assert!(
            String::from_utf8_lossy(&cancellation.stderr).contains("canceling statement"),
            "unexpected cancellation output: {}",
            String::from_utf8_lossy(&cancellation.stderr)
        );
        task.abort();
    }
}
