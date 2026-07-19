mod model;
mod neon;
mod postgres;
mod proxy;

use crate::{
    model::{DatabaseStorage, LocalDatabase, Upstream},
    postgres::{authenticate_upstream, parse_upstream_for_port, read_database_storage},
    proxy::{
        bind_local_listeners, serve_proxy, CancelRegistry, ProxyMetrics, ProxyMetricsSnapshot,
        LOCAL_PORT,
    },
};
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc,
};
use tauri::State;
use tokio::sync::Mutex;

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
        Ok(Self {
            lifecycle: Mutex::new(()),
            task: Mutex::new(None),
            metrics: Arc::new(ProxyMetrics::default()),
            database: Mutex::new(None),
            upstream: Mutex::new(None),
            running: AtomicBool::new(false),
            generation: AtomicU64::new(0),
            cancel_registry: Arc::new(CancelRegistry::default()),
            http_client: neon::http_client()?,
        })
    }
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

#[tauri::command]
async fn create_database(state: State<'_, Arc<ProxyState>>) -> Result<LocalDatabase, String> {
    let _lifecycle = state.lifecycle.lock().await;
    stop_existing_proxy(state.inner()).await;

    // Reserve localhost before provisioning so another service cannot claim the
    // port between the availability check and proxy startup.
    let listeners = bind_local_listeners().await?;
    let database = neon::provision_database(&state.http_client).await?;
    let (upstream, local_url) = parse_upstream_for_port(&database.connection_string, LOCAL_PORT)?;

    // A successful command means the full route, including Neon TLS and SCRAM,
    // has been verified rather than only the local listener.
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
    *state.database.lock().await = Some(local_database.clone());
    *state.upstream.lock().await = Some(upstream.clone());
    state.metrics.reset();
    let generation = state.generation.fetch_add(1, Ordering::AcqRel) + 1;
    state.running.store(true, Ordering::Release);

    let run_state = state.inner().clone();
    let task = tauri::async_runtime::spawn(async move {
        serve_proxy(
            listeners,
            upstream,
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
    neon::resolve_claim_url(&state.http_client, &claim_url).await
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

    #[test]
    fn application_state_initializes() {
        ProxyState::new().expect("application state");
    }
}
