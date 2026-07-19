import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { invoke } from "@tauri-apps/api/core";
import { getVersion } from "@tauri-apps/api/app";
import { openUrl } from "@tauri-apps/plugin-opener";
import { relaunch } from "@tauri-apps/plugin-process";
import { check, type Update } from "@tauri-apps/plugin-updater";
import "./App.css";

type LocalDatabase = {
  status: string;
  projectId: string;
  localUrl: string;
  remoteUrl: string;
  claimUrl: string;
  expiresAt: string;
  port: number;
};

type Format = "url" | "env" | "psql";
type Copied = "local" | "remote" | null;
type UpdateState = "idle" | "checking" | "available" | "current" | "downloading" | "installing" | "restarting" | "error";

type ProxyMetrics = {
  running: boolean;
  activeConnections: number;
  totalConnections: number;
  bytesToNeon: number;
  bytesFromNeon: number;
  failedConnections: number;
  startedAt: number;
  lastActivity: number;
};

type DatabaseStorage = {
  usedBytes: number;
  limitBytes: number;
};

const previewDatabase: LocalDatabase = {
  status: "ready",
  projectId: "silent-heart-aid5g7xu",
  localUrl: "postgresql://localhost:5432/neondb?sslmode=disable",
  remoteUrl: "postgresql://neondb_owner:preview-password@ep-silent-heart-aid5g7xu-pooler.c-3.us-east-1.aws.neon.tech/neondb?sslmode=require",
  claimUrl: "https://console.neon.tech/app/projects/silent-heart-aid5g7xu",
  expiresAt: new Date(Date.now() + 72 * 3_600_000).toISOString(),
  port: 5432,
};

const previewUpdate = {
  version: "0.2.0",
  currentVersion: "0.1.1",
  body: "Faster startup, improved connection reliability, and a more polished update experience.",
} as unknown as Update;

const emptyMetrics: ProxyMetrics = {
  running: false,
  activeConnections: 0,
  totalConnections: 0,
  bytesToNeon: 0,
  bytesFromNeon: 0,
  failedConnections: 0,
  startedAt: Date.now(),
  lastActivity: 0,
};

function formatBytes(bytes: number) {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(bytes < 10 * 1024 ? 1 : 0)} KB`;
  if (bytes < 1024 * 1024 * 1024) {
    const megabytes = bytes / (1024 * 1024);
    return `${megabytes.toFixed(megabytes >= 100 ? 0 : 1)} MB`;
  }
  return `${(bytes / (1024 * 1024 * 1024)).toFixed(1)} GB`;
}

function DatabaseIcon() {
  return (
    <svg aria-hidden="true" viewBox="0 0 24 24">
      <ellipse cx="12" cy="5" rx="7.5" ry="3" />
      <path d="M4.5 5v7c0 1.7 3.4 3 7.5 3s7.5-1.3 7.5-3V5" />
      <path d="M4.5 12v7c0 1.7 3.4 3 7.5 3s7.5-1.3 7.5-3v-7" />
    </svg>
  );
}

function InfoIcon() {
  return <svg aria-hidden="true" viewBox="0 0 24 24"><circle cx="12" cy="12" r="9" /><path d="M12 11v6M12 7.5v.5" /></svg>;
}

function CopyIcon() {
  return <svg aria-hidden="true" viewBox="0 0 24 24"><rect x="8" y="8" width="11" height="11" rx="2" /><path d="M16 8V6a2 2 0 0 0-2-2H6a2 2 0 0 0-2 2v8a2 2 0 0 0 2 2h2" /></svg>;
}

function ChevronIcon({ open = false }: { open?: boolean }) {
  return <svg className={open ? "open" : ""} aria-hidden="true" viewBox="0 0 24 24"><path d="m9 6 6 6-6 6" /></svg>;
}

function StopIcon() {
  return <svg aria-hidden="true" viewBox="0 0 24 24"><rect x="6.5" y="6.5" width="11" height="11" rx="2" /></svg>;
}

function ExternalIcon() {
  return <svg aria-hidden="true" viewBox="0 0 24 24"><path d="M14 5h5v5M19 5l-8 8" /><path d="M17 13v5a1 1 0 0 1-1 1H6a1 1 0 0 1-1-1V8a1 1 0 0 1 1-1h5" /></svg>;
}

function LockIcon() {
  return <svg aria-hidden="true" viewBox="0 0 24 24"><rect x="5" y="10" width="14" height="10" rx="2" /><path d="M8 10V7a4 4 0 0 1 8 0v3" /></svg>;
}

function ClockIcon() {
  return <svg aria-hidden="true" viewBox="0 0 24 24"><circle cx="12" cy="12" r="8.5" /><path d="M12 7.5V12l3 2" /></svg>;
}

function RefreshIcon() {
  return <svg aria-hidden="true" viewBox="0 0 24 24"><path d="M19 8V4l-2 2a8 8 0 1 0 2.2 8" /><path d="M19 4h-4" /></svg>;
}

function UpdateIcon() {
  return <svg aria-hidden="true" viewBox="0 0 24 24"><path d="M12 4v11M8 11l4 4 4-4" /><path d="M5 19h14" /></svg>;
}

function CloseIcon() {
  return <svg aria-hidden="true" viewBox="0 0 24 24"><path d="m7 7 10 10M17 7 7 17" /></svg>;
}

function App() {
  const previewMode = import.meta.env.DEV ? new URLSearchParams(window.location.search).get("preview") : null;
  const isPreview = previewMode !== null;
  const isRunningPreview = previewMode === "running" || previewMode === "update";
  const isCreatingPreview = previewMode === "creating";
  const [database, setDatabase] = useState<LocalDatabase | null>(isRunningPreview ? previewDatabase : null);
  const [restoring, setRestoring] = useState(!isPreview);
  const [creating, setCreating] = useState(isCreatingPreview);
  const [stopping, setStopping] = useState(false);
  const [error, setError] = useState("");
  const [format, setFormat] = useState<Format>("url");
  const [copied, setCopied] = useState<Copied>(null);
  const [showDetails, setShowDetails] = useState(false);
  const [revealRemote, setRevealRemote] = useState(false);
  const [claiming, setClaiming] = useState(false);
  const [claimError, setClaimError] = useState("");
  const [metrics, setMetrics] = useState<ProxyMetrics>(isRunningPreview ? {
    ...emptyMetrics,
    running: true,
    activeConnections: 2,
    totalConnections: 14,
    bytesToNeon: 183_420,
    bytesFromNeon: 624_180,
    lastActivity: Date.now(),
  } : emptyMetrics);
  const [storage, setStorage] = useState<DatabaseStorage | null>(isRunningPreview ? {
    usedBytes: 82_386_944,
    limitBytes: 104_857_600,
  } : null);
  const [storageRefreshing, setStorageRefreshing] = useState(false);
  const [storageRefreshError, setStorageRefreshError] = useState(false);
  const storageRefreshInFlight = useRef(false);
  const storageRequestId = useRef(0);
  const copiedTimer = useRef<number | null>(null);
  const [now, setNow] = useState(Date.now());
  const [appVersion, setAppVersion] = useState("");
  const [availableUpdate, setAvailableUpdate] = useState<Update | null>(previewMode === "update" ? previewUpdate : null);
  const [updateState, setUpdateState] = useState<UpdateState>(previewMode === "update" ? "available" : "idle");
  const [updateError, setUpdateError] = useState("");
  const [showUpdate, setShowUpdate] = useState(previewMode === "update");
  const [downloadedBytes, setDownloadedBytes] = useState(0);
  const [downloadSize, setDownloadSize] = useState<number | null>(null);
  const updateCheckInFlight = useRef(false);
  const downloadedBytesRef = useRef(0);

  useEffect(() => {
    if (isPreview) return;
    let cancelled = false;
    invoke<LocalDatabase | null>("current_database")
      .then((current) => {
        if (!cancelled && current) setDatabase(current);
      })
      .catch(() => undefined)
      .finally(() => {
        if (!cancelled) setRestoring(false);
      });
    return () => { cancelled = true; };
  }, [isPreview]);

  const checkForUpdates = useCallback(async (showResult = true) => {
    if (isPreview || updateCheckInFlight.current) return;
    updateCheckInFlight.current = true;
    setUpdateState("checking");
    setUpdateError("");
    try {
      const result = await check({ target: "macos-universal", timeout: 15_000 });
      setAvailableUpdate(result);
      if (result) {
        setUpdateState("available");
        setShowUpdate(true);
      } else {
        setUpdateState("current");
        if (showResult) setShowUpdate(true);
      }
    } catch (reason) {
      setUpdateState("error");
      setUpdateError("Neon Localhost couldn’t check for updates. Check your internet connection and try again.");
      if (showResult) setShowUpdate(true);
      console.error("Update check failed", reason);
    } finally {
      updateCheckInFlight.current = false;
    }
  }, [isPreview]);

  useEffect(() => {
    if (isPreview) return;
    void getVersion().then(setAppVersion).catch(() => undefined);
    const initialCheck = window.setTimeout(() => void checkForUpdates(false), 2_000);
    const recurringCheck = window.setInterval(() => void checkForUpdates(false), 6 * 60 * 60 * 1_000);
    return () => {
      window.clearTimeout(initialCheck);
      window.clearInterval(recurringCheck);
    };
  }, [checkForUpdates, isPreview]);

  useEffect(() => {
    if (!database) return;
    const timer = window.setInterval(() => setNow(Date.now()), 30_000);
    return () => window.clearInterval(timer);
  }, [database]);

  useEffect(() => () => {
    if (copiedTimer.current !== null) window.clearTimeout(copiedTimer.current);
  }, []);

  useEffect(() => {
    if (!database || isPreview) return;
    let cancelled = false;
    const refresh = async () => {
      try {
        const result = await invoke<ProxyMetrics>("proxy_metrics");
        if (!cancelled && !result.running) {
          setDatabase(null);
          setError("The local proxy stopped unexpectedly. Create the database again to reconnect.");
        } else if (!cancelled) {
          setMetrics(result);
        }
      } catch {
        // The database connection remains usable if activity metrics are unavailable.
      }
    };
    void refresh();
    const timer = window.setInterval(refresh, 1_000);
    return () => {
      cancelled = true;
      window.clearInterval(timer);
    };
  }, [database, isPreview]);

  const refreshStorage = useCallback(async () => {
    if (!database || storageRefreshInFlight.current) return;
    const requestId = ++storageRequestId.current;
    storageRefreshInFlight.current = true;
    setStorageRefreshing(true);
    setStorageRefreshError(false);
    try {
      if (!isPreview) {
        const result = await invoke<DatabaseStorage>("database_storage");
        if (storageRequestId.current === requestId) setStorage(result);
      }
    } catch {
      if (storageRequestId.current === requestId) setStorageRefreshError(true);
    } finally {
      if (storageRequestId.current === requestId) {
        storageRefreshInFlight.current = false;
        setStorageRefreshing(false);
      }
    }
  }, [database, isPreview]);

  useEffect(() => {
    if (!database) {
      storageRequestId.current += 1;
      storageRefreshInFlight.current = false;
      setStorage(null);
      setStorageRefreshing(false);
      return;
    }
    if (!isPreview) void refreshStorage();
    const timer = window.setInterval(() => void refreshStorage(), 30_000);
    return () => {
      window.clearInterval(timer);
      storageRequestId.current += 1;
      storageRefreshInFlight.current = false;
    };
  }, [database, isPreview, refreshStorage]);

  const timeLeft = useMemo(() => {
    if (!database) return "";
    const milliseconds = Math.max(0, new Date(database.expiresAt).getTime() - now);
    const hours = Math.floor(milliseconds / 3_600_000);
    const minutes = Math.floor((milliseconds % 3_600_000) / 60_000);
    return `${hours}h ${minutes}m`;
  }, [database, now]);

  const localValue = useMemo(() => {
    if (!database) return "";
    if (format === "env") return `DATABASE_URL="${database.localUrl}"`;
    if (format === "psql") return `psql '${database.localUrl}'`;
    return database.localUrl;
  }, [database, format]);

  const remoteDisplay = useMemo(() => {
    if (!database || revealRemote) return database?.remoteUrl ?? "";
    return database.remoteUrl.replace(/(postgres(?:ql)?:\/\/[^:]+:)([^@]+)(@)/, "$1••••••••$3");
  }, [database, revealRemote]);

  const storagePercent = storage && storage.limitBytes > 0
    ? Math.min(100, (storage.usedBytes / storage.limitBytes) * 100)
    : 0;
  const storageTone = storagePercent >= 90 ? "danger" : storagePercent >= 75 ? "warning" : "";

  async function createDatabase() {
    setCreating(true);
    setError("");
    try {
      const result = await invoke<LocalDatabase>("create_database");
      setDatabase(result);
      setNow(Date.now());
    } catch (reason) {
      setError(String(reason));
    } finally {
      setCreating(false);
    }
  }

  async function copy(field: Exclude<Copied, null>, value: string) {
    try {
      await navigator.clipboard.writeText(value);
      setCopied(field);
      if (copiedTimer.current !== null) window.clearTimeout(copiedTimer.current);
      copiedTimer.current = window.setTimeout(() => {
        setCopied(null);
        copiedTimer.current = null;
      }, 1400);
    } catch {
      setError("Could not copy to the clipboard.");
    }
  }

  async function stopProxy() {
    if (stopping) return;
    setStopping(true);
    setError("");
    try {
      await invoke("stop_proxy");
      setDatabase(null);
      setShowDetails(false);
      setRevealRemote(false);
    } catch (reason) {
      setError(`Could not stop the local proxy: ${String(reason)}`);
    } finally {
      setStopping(false);
    }
  }

  async function openAbout() {
    try {
      await openUrl("https://neon.new");
    } catch (reason) {
      setError(`Could not open neon.new: ${String(reason)}`);
    }
  }

  async function claimDatabase() {
    if (!database) return;
    setClaiming(true);
    setClaimError("");
    try {
      const destination = await invoke<string>("resolve_claim_url", { claimUrl: database.claimUrl });
      await openUrl(destination);
    } catch (reason) {
      setClaimError(String(reason));
    } finally {
      setClaiming(false);
    }
  }

  async function installUpdate() {
    if (!availableUpdate || updateState === "downloading" || updateState === "installing" || updateState === "restarting") return;
    setUpdateState("downloading");
    setUpdateError("");
    downloadedBytesRef.current = 0;
    setDownloadedBytes(0);
    setDownloadSize(null);
    try {
      await availableUpdate.downloadAndInstall((event) => {
        if (event.event === "Started") {
          setDownloadSize(event.data.contentLength ?? null);
        } else if (event.event === "Progress") {
          downloadedBytesRef.current += event.data.chunkLength;
          setDownloadedBytes(downloadedBytesRef.current);
        } else if (event.event === "Finished") {
          setUpdateState("installing");
        }
      }, { timeout: 120_000 });
      setUpdateState("restarting");
      if (database) {
        try {
          await invoke("stop_proxy");
        } catch {
          // Relaunching also closes the local listener; do not strand an installed update.
        }
      }
      await relaunch();
    } catch (reason) {
      setUpdateState("error");
      setUpdateError("The update couldn’t be installed. Your current version is unchanged; please try again.");
      console.error("Update install failed", reason);
    }
  }

  const updateProgress = downloadSize && downloadSize > 0
    ? Math.min(100, Math.round((downloadedBytes / downloadSize) * 100))
    : null;
  const updateBusy = updateState === "checking" || updateState === "downloading" || updateState === "installing" || updateState === "restarting";

  return (
    <main className="mac-window">
      <aside className="sidebar">
        <div className="titlebar-drag" data-tauri-drag-region />
        <div className="sidebar-title">Local Databases</div>
        <div className="source-heading">On This Mac</div>
        <nav>
          {database ? (
            <button type="button" className="database-row selected" aria-current="page">
              <span className="database-glyph"><DatabaseIcon /></span>
              <span className="database-label"><strong>neondb</strong><small>localhost:{database.port}</small></span>
              <span className="status-dot" />
            </button>
          ) : (
            <div className="no-databases">No local databases</div>
          )}
        </nav>

        <button type="button" className={`update-button ${availableUpdate ? "has-update" : ""}`} onClick={() => availableUpdate ? setShowUpdate(true) : void checkForUpdates(true)} disabled={updateBusy}>
          <UpdateIcon />
          <span><strong>{availableUpdate ? "Update available" : updateState === "checking" ? "Checking for updates…" : "Check for Updates"}</strong>{appVersion && <small>{availableUpdate ? `Version ${availableUpdate.version}` : `Version ${appVersion}`}</small>}</span>
          {availableUpdate && <i />}
        </button>
        <button type="button" className="about-button" onClick={openAbout}>
          <InfoIcon /><span>About neon.new</span>
        </button>
      </aside>

      <section className="main-pane">
        <header className="toolbar" data-tauri-drag-region>
          <div className="toolbar-title">
            <h1>{database ? "neondb" : "Neon Localhost"}</h1>
            {database && <span className="running-label"><i />Running</span>}
          </div>
          {database && (
            <button type="button" className="stop-button" onClick={stopProxy} disabled={stopping} title="Stop local database" aria-busy={stopping}>
              <StopIcon /> {stopping ? "Stopping…" : "Stop"}
            </button>
          )}
        </header>

        <div className={`pane-content ${database ? "has-database" : ""}`}>
          {restoring ? (
            <div className="loading-state"><span /><p>Checking local proxy…</p></div>
          ) : !database ? (
            <div className={`empty-state ${creating ? "creating" : ""}`}>
              <span className="empty-icon"><DatabaseIcon /></span>
              <span className="empty-kicker">Local Postgres, powered by Neon</span>
              <h2>{creating ? "Creating your database" : "No database running"}</h2>
              <p>{creating ? "Neon is provisioning Postgres and preparing a secure local connection." : "Create a temporary Postgres database that works like it is installed directly on this Mac."}</p>
              <div className="empty-specs">
                <div><span>Local address</span><code>localhost:5432</code></div>
                <div><span>Authentication</span><strong>No password</strong></div>
                <div><span>Available for</span><strong>72 hours</strong></div>
              </div>
              <button type="button" className={`primary-button ${creating ? "is-loading" : ""}`} onClick={createDatabase} disabled={creating} aria-busy={creating}>
                {creating && <span className="spinner" />}
                {creating ? "Creating…" : "Create Local Database"}
              </button>
              {creating ? (
                <div className="creation-feedback" role="status">
                  <div className="creation-track"><span /></div>
                  <p>Provisioning Neon and securing <code>localhost:5432</code></p>
                </div>
              ) : (
                <span className="empty-note">No account required · Claim it later to keep it</span>
              )}
            </div>
          ) : (
            <div className="database-content">
              <section className="connection-section">
                <div className="section-title-row">
                  <div>
                    <span className="section-eyebrow">Local Postgres</span>
                    <h2>Connect to your database</h2>
                    <p>Copy a connection string or enter the details in any database client.</p>
                  </div>
                  <div className="segmented-control" aria-label="Connection format">
                    <button type="button" aria-pressed={format === "url"} className={format === "url" ? "active" : ""} onClick={() => setFormat("url")}>URL</button>
                    <button type="button" aria-pressed={format === "env"} className={format === "env" ? "active" : ""} onClick={() => setFormat("env")}>.env</button>
                    <button type="button" aria-pressed={format === "psql"} className={format === "psql" ? "active" : ""} onClick={() => setFormat("psql")}>psql</button>
                  </div>
                </div>

                <div className="connection-panel">
                  <div className="connection-field">
                    <code>{localValue}</code>
                    <button type="button" className={copied === "local" ? "copied" : ""} onClick={() => copy("local", localValue)}>
                      {copied === "local" ? <span className="checkmark">✓</span> : <CopyIcon />}
                      {copied === "local" ? "Copied" : "Copy"}
                    </button>
                  </div>

                  <div className="connection-facts">
                    <div><span>Host</span><strong>localhost</strong></div>
                    <div><span>Port</span><strong>{database.port}</strong></div>
                    <div><span>Database</span><strong>neondb</strong></div>
                    <div><span>Password</span><strong>None</strong></div>
                  </div>
                </div>
              </section>

              <div className="security-note">
                <LockIcon />
                <span>No local password. Your connection from this Mac to Neon is encrypted.</span>
              </div>

              <div className="section-eyebrow database-eyebrow">Database</div>
              <section className="neon-section">
                <button type="button" className="disclosure-row" onClick={() => setShowDetails(!showDetails)} aria-expanded={showDetails}>
                  <span className="remote-icon"><DatabaseIcon /></span>
                  <span className="disclosure-copy"><strong>Remote Neon database</strong><small>TLS encrypted · Credentials managed by Neon Localhost</small></span>
                  <span className="expiry">{timeLeft} left</span>
                  <span className="disclosure-chevron"><ChevronIcon open={showDetails} /></span>
                </button>

                <div className={`storage-summary ${storageTone} ${storageRefreshing ? "updating" : ""}`} aria-label="Database storage usage">
                  <span className="storage-label">Storage</span>
                  <div className="storage-meter">
                    <div className="storage-track" role="progressbar" aria-label="Storage used" aria-valuemin={0} aria-valuemax={100} aria-valuenow={Math.round(storagePercent)}><i style={{ width: `${storagePercent}%` }} /></div>
                    <small>{storageRefreshError ? "Couldn’t update" : storageRefreshing ? "Updating…" : storage ? `${Math.round(storagePercent)}% used` : "Checking usage…"}</small>
                  </div>
                  <strong>{storage ? `${formatBytes(storage.usedBytes)} / ${formatBytes(storage.limitBytes)}` : "—"}</strong>
                  <button type="button" className="storage-refresh" onClick={refreshStorage} disabled={storageRefreshing} title="Refresh storage usage" aria-label="Refresh storage usage">
                    <RefreshIcon />
                  </button>
                </div>

                {showDetails && (
                  <div className="details-panel">
                    <div className="detail-row"><span>Project</span><code>{database.projectId}</code></div>
                    <div className="detail-row"><span>Connection</span><strong><i />Encrypted with TLS</strong></div>
                    <div className="remote-row">
                      <span>Remote URL</span>
                      <div className="remote-value">
                        <code>{remoteDisplay}</code>
                        <button type="button" aria-pressed={revealRemote} onClick={() => setRevealRemote(!revealRemote)}>{revealRemote ? "Hide" : "Reveal"}</button>
                        <button type="button" title="Copy remote URL" onClick={() => copy("remote", database.remoteUrl)}>{copied === "remote" ? "Copied" : "Copy"}</button>
                      </div>
                    </div>
                  </div>
                )}
              </section>

              <footer className="temporary-footer">
                <ClockIcon />
                <div><strong>This database is temporary</strong><span>Claim it before the timer ends to keep your data.</span></div>
                <button type="button" onClick={claimDatabase} disabled={claiming}>
                  {claiming ? "Opening…" : "Claim Database"} {!claiming && <ExternalIcon />}
                </button>
              </footer>
              {claimError && <div className="claim-error" role="alert"><InfoIcon /><span>{claimError}</span></div>}
            </div>
          )}

          {error && <div className="error-message" role="alert">{error}</div>}
        </div>

        <footer className={`status-bar ${database ? "ready" : creating ? "starting" : ""}`} aria-live="polite">
          <div className="status-primary"><i />{database ? "Proxy ready" : creating ? "Starting proxy" : "Proxy stopped"}</div>
          {database && <><span className="status-divider" /><code>localhost:{database.port}</code></>}
          <div className="status-secondary">
            {database ? (
              <>
                <span>{metrics.activeConnections} active</span>
                <span>{formatBytes(metrics.bytesToNeon + metrics.bytesFromNeon)} transferred</span>
                {metrics.failedConnections > 0 && <span className="status-failures">{metrics.failedConnections} failed</span>}
              </>
            ) : creating ? "Creating database…" : "No database running"}
          </div>
        </footer>
      </section>

      {showUpdate && (
        <div className="update-overlay" role="presentation" onMouseDown={(event) => { if (event.target === event.currentTarget && !updateBusy) setShowUpdate(false); }}>
          <section className="update-sheet" role="dialog" aria-modal="true" aria-labelledby="update-title">
            <button type="button" className="update-close" onClick={() => setShowUpdate(false)} disabled={updateBusy} aria-label="Close"><CloseIcon /></button>
            <span className={`update-mark ${availableUpdate ? "available" : ""}`}><UpdateIcon /></span>
            {updateState === "current" ? (
              <>
                <h2 id="update-title">You’re up to date</h2>
                <p>Neon Localhost {appVersion || ""} is the newest version available.</p>
                <button type="button" className="sheet-secondary" onClick={() => setShowUpdate(false)}>Done</button>
              </>
            ) : updateState === "error" ? (
              <>
                <h2 id="update-title">Couldn’t update</h2>
                <p>{updateError}</p>
                <div className="sheet-actions"><button type="button" className="sheet-secondary" onClick={() => setShowUpdate(false)}>Cancel</button><button type="button" className="sheet-primary" onClick={() => void checkForUpdates(true)}>Try Again</button></div>
              </>
            ) : availableUpdate ? (
              <>
                <span className="update-kicker">Version {availableUpdate.version}</span>
                <h2 id="update-title">A new version is ready</h2>
                <p>{availableUpdate.body?.trim() || "This update includes the latest improvements and fixes."}</p>
                {database && !updateBusy && <div className="update-warning"><InfoIcon /><span>Installing restarts the app and stops the current local database connection.</span></div>}
                {(updateState === "downloading" || updateState === "installing" || updateState === "restarting") && (
                  <div className="update-progress" role="status">
                    <div><i style={{ width: updateProgress === null ? "32%" : `${updateProgress}%` }} /></div>
                    <span>{updateState === "downloading" ? updateProgress === null ? "Downloading update…" : `Downloading… ${updateProgress}%` : updateState === "installing" ? "Installing update…" : "Restarting Neon Localhost…"}</span>
                  </div>
                )}
                <div className="sheet-actions"><button type="button" className="sheet-secondary" onClick={() => setShowUpdate(false)} disabled={updateBusy}>Later</button><button type="button" className="sheet-primary" onClick={installUpdate} disabled={updateBusy}>{updateState === "downloading" ? "Downloading…" : updateState === "installing" ? "Installing…" : updateState === "restarting" ? "Restarting…" : database ? "Install & Restart" : "Install Update"}</button></div>
              </>
            ) : (
              <><h2 id="update-title">Checking for updates</h2><p>Looking for the newest version of Neon Localhost…</p><div className="update-progress indeterminate"><div><i /></div></div></>
            )}
          </section>
        </div>
      )}
    </main>
  );
}

export default App;
