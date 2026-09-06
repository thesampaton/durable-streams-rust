//! Standalone server, configuration inspection, and stream transfer CLI.

#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        reason = "test setup and assertions fail the test on error"
    )
)]
use axum_server::{
    Handle, from_tcp,
    tls_rustls::{RustlsConfig, from_tcp_rustls},
};
use clap::{Parser, Subcommand, ValueEnum};
use durable_streams_server::{
    config::{
        AcidBackend, Config, ConfigLoadOptions, DeploymentProfile, StorageMode, TransportMode,
    },
    startup::{
        StartupError, StartupPhase, bind_tcp_listener, build_tls_server_config, log_phase,
        log_startup_failure, log_transport_summary, preflight_tls_files,
    },
    storage::{Storage, acid::AcidStorage, file::FileStorage, memory::InMemoryStorage},
    streams::{StreamListEntry, StreamService},
    transfer::{
        export::{ExportOptions, export_streams},
        import::{ConflictPolicy, ImportOptions, import_streams},
    },
};
use std::fs;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

// ── CLI ─────────────────────────────────────────────────────────────

/// Durable Streams protocol server.
#[derive(Parser)]
#[command(version, about)]
struct Cli {
    /// Configuration profile (loads config/<name>.toml after config/default.toml)
    #[arg(long, global = true, default_value = "default")]
    profile: String,

    /// Extra TOML configuration file to load last
    #[arg(long, global = true)]
    config: Option<PathBuf>,

    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    /// Start the HTTP server (default when no subcommand is given)
    Serve,
    /// List all streams with their metadata from configured local storage
    List {
        /// Output as JSON instead of a table
        #[arg(long)]
        json: bool,
        /// Full URL for explicit remote admin listing, e.g. <http://127.0.0.1:4437/admin/streams>
        #[arg(long)]
        url: Option<String>,
    },
    /// Export streams to JSON
    Export {
        /// Output file path (defaults to stdout)
        #[arg(long, short)]
        output: Option<PathBuf>,

        /// Only export streams matching these names (repeatable)
        #[arg(long)]
        stream: Vec<String>,
    },
    /// Import streams from JSON
    Import {
        /// Input file path (defaults to stdin)
        #[arg(long, short)]
        input: Option<PathBuf>,

        /// How to handle streams that already exist
        #[arg(long, value_enum, default_value_t = ConflictArg::Skip)]
        on_conflict: ConflictArg,
    },
}

#[derive(Clone, Copy, ValueEnum)]
enum ConflictArg {
    /// Skip streams that already exist
    Skip,
    /// Fail if any stream already exists
    Fail,
    /// Delete and recreate existing streams
    Replace,
}

impl From<ConflictArg> for ConflictPolicy {
    fn from(arg: ConflictArg) -> Self {
        match arg {
            ConflictArg::Skip => Self::Skip,
            ConflictArg::Fail => Self::Fail,
            ConflictArg::Replace => Self::Replace,
        }
    }
}

// ── Startup & runtime ───────────────────────────────────────────────

struct AppRuntime {
    config: Config,
    addr: SocketAddr,
}

impl AppRuntime {
    fn new(config: Config, profile: &DeploymentProfile) -> Result<Self, StartupError> {
        log_phase(StartupPhase::ValidateConfig);
        let addr = config
            .bind_socket_addr()
            .map_err(StartupError::config_validation)?;
        config.validate().map_err(StartupError::config_validation)?;
        config
            .validate_profile(profile)
            .map_err(StartupError::config_validation)?;

        for warning in config.warnings() {
            tracing::warn!("{warning}");
        }

        tracing::info!(
            bind_address = %addr,
            storage.mode = config.storage.mode.as_str(),
            limits.max_memory_bytes = config.limits.max_memory_bytes,
            limits.max_stream_bytes = config.limits.max_stream_bytes,
            "configuration validated"
        );

        log_phase(StartupPhase::ResolveTransport);
        log_transport_summary(&config);

        log_phase(StartupPhase::CheckTlsFiles);
        preflight_tls_files(&config)?;
        if config.tls_enabled() {
            tracing::info!("TLS file preflight passed");
        }

        Ok(Self { config, addr })
    }

    fn cleanup() {
        tracing::info!("Runtime cleanup completed");
    }
}

// ── Main ────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    let mut load_options = ConfigLoadOptions::default();
    load_options.profile = DeploymentProfile::from(cli.profile);
    load_options.config_override = cli.config;

    log_phase(StartupPhase::LoadConfig);
    let config = match Config::from_sources(&load_options) {
        Ok(config) => config,
        Err(err) => {
            let startup_err = StartupError::config_load(err);
            eprintln!("{startup_err}");
            std::process::exit(1);
        }
    };

    match cli.command.unwrap_or(Command::Serve) {
        Command::Serve => {
            tracing_subscriber::registry()
                .with(
                    tracing_subscriber::EnvFilter::try_from_default_env()
                        .unwrap_or_else(|_| config.observability.rust_log.clone().into()),
                )
                .with(tracing_subscriber::fmt::layer())
                .init();

            if let Err(err) = run_serve(config, &load_options.profile).await {
                log_startup_failure(&err);
                std::process::exit(1);
            }
        }
        Command::List { json, url } => {
            let result = if let Some(url) = url.as_deref() {
                run_list_http(url, json).await
            } else {
                run_list_local(&config, json)
            };
            if let Err(err) = result {
                eprintln!("{err}");
                std::process::exit(1);
            }
        }
        Command::Export { output, stream } => {
            if let Err(err) = run_with_storage(&config, |storage| {
                run_export(storage, output.as_ref(), stream)
            }) {
                eprintln!("{err}");
                std::process::exit(1);
            }
        }
        Command::Import { input, on_conflict } => {
            if let Err(err) = run_with_storage(&config, |storage| {
                run_import(storage, input.as_ref(), on_conflict.into())
            }) {
                eprintln!("{err}");
                std::process::exit(1);
            }
        }
    }
}

// ── Storage construction (shared between serve and CLI command paths) ──

fn build_in_memory_storage(config: &Config) -> InMemoryStorage {
    InMemoryStorage::new(
        config.limits.max_memory_bytes,
        config.limits.max_stream_bytes,
    )
}

fn build_file_storage(config: &Config) -> Result<FileStorage, String> {
    let sync_on_append = config.storage.mode.sync_on_append();
    let storage = FileStorage::new(
        &config.storage.data_dir,
        config.limits.max_memory_bytes,
        config.limits.max_stream_bytes,
        sync_on_append,
    )
    .map_err(|e| format!("failed to initialize file storage: {e}"))?;
    tracing::info!(
        storage.dir = config.storage.data_dir,
        storage.sync_on_append = sync_on_append,
        "file storage initialized"
    );
    Ok(storage)
}

fn build_acid_storage(config: &Config) -> Result<AcidStorage, String> {
    let storage = AcidStorage::new(
        &config.storage.data_dir,
        config.storage.acid_shard_count,
        config.limits.max_memory_bytes,
        config.limits.max_stream_bytes,
        config.storage.acid_backend,
    )
    .map_err(|e| format!("failed to initialize acid storage: {e}"))?;
    tracing::info!(
        storage.backend = config.storage.acid_backend.as_str(),
        storage.dir = config.storage.data_dir,
        storage.shards = config.storage.acid_shard_count,
        "acid storage initialized"
    );
    Ok(storage)
}

fn run_with_storage<F>(config: &Config, f: F) -> Result<(), String>
where
    F: FnOnce(&dyn Storage) -> Result<(), String>,
{
    match config.storage.mode {
        StorageMode::Memory => f(&build_in_memory_storage(config)),
        StorageMode::FileFast | StorageMode::FileDurable => f(&build_file_storage(config)?),
        StorageMode::Acid => f(&build_acid_storage(config)?),
    }
}

// ── List command ────────────────────────────────────────────────────

fn run_list_local(config: &Config, json: bool) -> Result<(), String> {
    if config.storage.mode == StorageMode::Acid
        && config.storage.acid_backend == AcidBackend::InMemory
    {
        return Err(
            "cannot list local streams for storage.acid_backend='in-memory': storage is process-local and has no durable state to inspect; use acid_backend='file', or pass --url to query an explicitly enabled admin endpoint"
                .to_string(),
        );
    }
    match config.storage.mode {
        StorageMode::Memory => Err(
            "cannot list local streams for storage.mode='memory': in-memory storage is process-local and has no durable state to inspect; use file or acid storage, or pass --url to query an explicitly enabled admin endpoint"
                .to_string(),
        ),
        StorageMode::FileFast | StorageMode::FileDurable => {
            let service = StreamService::new(Arc::new(build_file_storage(config)?));
            let entries = service
                .list_entries()
                .map_err(|e| format!("failed to list streams: {e}"))?;
            print_stream_entries(&entries, json);
            Ok(())
        }
        StorageMode::Acid => {
            let service = StreamService::new(Arc::new(build_acid_storage(config)?));
            let entries = service
                .list_entries()
                .map_err(|e| format!("failed to list streams: {e}"))?;
            print_stream_entries(&entries, json);
            Ok(())
        }
    }
}

async fn run_list_http(list_url: &str, json: bool) -> Result<(), String> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .map_err(|e| format!("failed to build HTTP client: {e}"))?;
    let response = client
        .get(list_url)
        .header("Accept", "application/json")
        .send()
        .await
        .map_err(|e| format!("failed to connect to server at {list_url}: {e}"))?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(format!("server returned {status}: {body}"));
    }

    let entries: Vec<StreamListEntry> = response
        .json()
        .await
        .map_err(|e| format!("failed to parse response from {list_url}: {e}"))?;

    print_stream_entries(&entries, json);
    Ok(())
}

fn print_stream_entries(entries: &[StreamListEntry], json: bool) {
    if json {
        print_streams_json(entries);
    } else {
        print_streams_table(entries);
    }
}

fn print_streams_json(entries: &[StreamListEntry]) {
    // Keep the CLI's established JSON contract independent of the admin response.
    let entries: Vec<serde_json::Value> = entries
        .iter()
        .map(|entry| {
            serde_json::json!({
                "name": entry.name,
                "status": if entry.closed { "closed" } else { "open" },
                "message_count": entry.message_count,
                "total_bytes": entry.total_bytes,
                "content_type": entry.content_type,
                "created_at": entry.created_at.to_rfc3339(),
                "updated_at": entry.updated_at.map(|t| t.to_rfc3339()),
                "ttl_seconds": entry.ttl_seconds,
                "expires_at": entry.expires_at.map(|t| t.to_rfc3339()),
            })
        })
        .collect();
    println!(
        "{}",
        serde_json::to_string_pretty(&entries).expect("JSON serialization should not fail")
    );
}

fn print_streams_table(entries: &[StreamListEntry]) {
    if entries.is_empty() {
        println!("No streams found.");
        return;
    }

    println!(
        "{:<30} {:<8} {:>10} {:>12} {:<24} {:<22} {:<22}",
        "Name", "Status", "Messages", "Bytes", "Content-Type", "Created", "Updated"
    );
    println!("{}", "-".repeat(132));

    for entry in entries {
        let status = if entry.closed { "closed" } else { "open" };
        let bytes = format_bytes(entry.total_bytes);
        let created = entry.created_at.format("%Y-%m-%d %H:%M:%S").to_string();
        let updated = entry.updated_at.map_or_else(
            || "-".to_string(),
            |t| t.format("%Y-%m-%d %H:%M:%S").to_string(),
        );

        println!(
            "{:<30} {:<8} {:>10} {:>12} {:<24} {:<22} {:<22}",
            truncate(&entry.name, 30),
            status,
            entry.message_count,
            bytes,
            truncate(&entry.content_type, 24),
            created,
            updated
        );
    }

    println!();
    println!("{} stream(s) total", entries.len());
}

#[allow(clippy::cast_precision_loss)]
fn format_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = 1024 * KB;
    const GB: u64 = 1024 * MB;

    if bytes >= GB {
        format!("{:.1} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.1} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.1} KB", bytes as f64 / KB as f64)
    } else {
        format!("{bytes} B")
    }
}

fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_string()
    } else {
        let head_len = max.saturating_sub(3);
        if head_len == 0 {
            return "...".chars().take(max).collect();
        }

        let safe_end = s
            .char_indices()
            .map(|(idx, _)| idx)
            .take_while(|idx| *idx <= head_len)
            .last()
            .unwrap_or(0);

        format!("{}...", &s[..safe_end])
    }
}

// ── Export command ───────────────────────────────────────────────────

fn run_export(
    storage: &dyn Storage,
    output: Option<&PathBuf>,
    streams: Vec<String>,
) -> Result<(), String> {
    let options = ExportOptions {
        stream_names: streams,
    };

    let stats = if let Some(path) = output {
        let file =
            fs::File::create(path).map_err(|e| format!("failed to create output file: {e}"))?;
        export_streams(storage, &options, file).map_err(|e| format!("export failed: {e}"))?
    } else {
        let stdout = std::io::stdout().lock();
        export_streams(storage, &options, stdout).map_err(|e| format!("export failed: {e}"))?
    };

    if output.is_some() {
        eprintln!(
            "Exported {} stream(s) with {} message(s)",
            stats.streams_exported, stats.messages_exported
        );
    }

    Ok(())
}

// ── Import command ──────────────────────────────────────────────────

fn run_import(
    storage: &dyn Storage,
    input: Option<&PathBuf>,
    on_conflict: ConflictPolicy,
) -> Result<(), String> {
    let options = ImportOptions {
        conflict_policy: on_conflict,
    };

    let stats = if let Some(path) = input {
        let file = fs::File::open(path).map_err(|e| format!("failed to open input file: {e}"))?;
        import_streams(storage, file, &options).map_err(|e| format!("import failed: {e}"))?
    } else {
        let stdin = std::io::stdin().lock();
        import_streams(storage, stdin, &options).map_err(|e| format!("import failed: {e}"))?
    };

    eprintln!(
        "Imported {} stream(s), skipped {}, {} message(s) total",
        stats.streams_imported, stats.streams_skipped, stats.messages_imported
    );

    Ok(())
}

// ── Server ──────────────────────────────────────────────────────────

async fn run_serve(config: Config, profile: &DeploymentProfile) -> Result<(), StartupError> {
    let runtime = AppRuntime::new(config, profile)?;

    let serve_result = match runtime.config.storage.mode {
        StorageMode::Memory => {
            serve(Arc::new(build_in_memory_storage(&runtime.config)), &runtime).await
        }
        StorageMode::FileFast | StorageMode::FileDurable => {
            let storage = build_file_storage(&runtime.config).map_err(StartupError::runtime)?;
            serve(Arc::new(storage), &runtime).await
        }
        StorageMode::Acid => {
            let storage = build_acid_storage(&runtime.config).map_err(StartupError::runtime)?;
            serve(Arc::new(storage), &runtime).await
        }
    };

    AppRuntime::cleanup();
    serve_result
}

async fn serve<S: Storage + 'static>(
    storage: Arc<S>,
    runtime: &AppRuntime,
) -> Result<(), StartupError> {
    let ready = Arc::new(AtomicBool::new(false));
    let shutdown = CancellationToken::new();
    let server = durable_streams_server::Server::new(
        durable_streams_server::StreamService::new(storage),
        &runtime.config,
        durable_streams_server::RouterOptions::default()
            .with_readiness(Arc::clone(&ready))
            .with_shutdown(shutdown.clone()),
    )
    .map_err(|e| StartupError::runtime(e.to_string()))?
    .start()
    .map_err(|e| StartupError::runtime(e.to_string()))?;
    let app = server.router();
    let handle = Handle::new();

    // Storage is already initialised (new() is synchronous); mark ready.
    ready.store(true, Ordering::Release);

    let shutdown_handle = handle.clone();
    tokio::spawn(async move {
        wait_for_shutdown_signal().await;
        tracing::info!("Shutdown signal received, beginning graceful drain");
        shutdown.cancel();
        shutdown_handle.graceful_shutdown(Some(Duration::from_secs(30)));
    });

    let tls = match runtime.config.transport.mode {
        TransportMode::Http => None,
        TransportMode::Tls | TransportMode::Mtls => {
            log_phase(StartupPhase::BuildTlsContext);
            let server_config = build_tls_server_config(&runtime.config)?;
            tracing::info!(
                transport.mode = runtime.config.transport.mode.as_str(),
                "TLS context built successfully"
            );
            Some(RustlsConfig::from_config(Arc::new(server_config)))
        }
    };

    log_phase(StartupPhase::BindListener);
    let listener = bind_tcp_listener(runtime.addr)?;
    log_bound_endpoints(runtime);
    log_phase(StartupPhase::StartServer);

    let service = app.into_make_service_with_connect_info::<SocketAddr>();
    let addr = runtime.addr;
    let bind_err = |error| StartupError::bind(addr, error);
    let runtime_err = |e: std::io::Error| StartupError::runtime(e.to_string());

    match tls {
        Some(tls) => from_tcp_rustls(listener, tls)
            .map_err(bind_err)?
            .handle(handle)
            .serve(service)
            .await
            .map_err(runtime_err)?,
        None => from_tcp(listener)
            .map_err(bind_err)?
            .handle(handle)
            .serve(service)
            .await
            .map_err(runtime_err)?,
    }

    server
        .shutdown()
        .await
        .map_err(|e| StartupError::runtime(e.to_string()))?;
    Ok(())
}

fn log_bound_endpoints(runtime: &AppRuntime) {
    let scheme = if runtime.config.tls_enabled() {
        "https"
    } else {
        "http"
    };
    tracing::info!(
        bind_address = %runtime.addr,
        scheme,
        "server listening"
    );
    tracing::info!("Health check: {scheme}://{}/healthz", runtime.addr);
    tracing::info!("Readiness:    {scheme}://{}/readyz", runtime.addr);
    tracing::info!(
        "Protocol base: {scheme}://{}{}/",
        runtime.addr,
        runtime.config.http.stream_base_path
    );
}

async fn wait_for_shutdown_signal() {
    let ctrl_c = async {
        if let Err(e) = tokio::signal::ctrl_c().await {
            tracing::error!("Failed to install Ctrl+C handler: {e}");
        }
    };

    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut signal) => {
                signal.recv().await;
            }
            Err(e) => {
                tracing::error!("Failed to install SIGTERM handler: {e}");
                std::future::pending::<()>().await;
            }
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        () = ctrl_c => {},
        () = terminate => {},
    }
}

#[cfg(test)]
mod tests {
    use super::{run_list_local, truncate};
    use durable_streams_server::Config;

    #[test]
    fn truncate_respects_utf8_boundaries() {
        assert_eq!(truncate("你好世界", 7), "你...");
        assert_eq!(truncate("🙂🙂🙂", 6), "...");
    }

    #[test]
    fn truncate_handles_small_limits() {
        assert_eq!(truncate("abcdef", 2), "..");
        assert_eq!(truncate("abcdef", 3), "...");
    }

    #[test]
    fn local_list_rejects_memory_storage() {
        let err = run_list_local(&Config::default(), true).expect_err("memory list should fail");
        assert!(err.contains("storage.mode='memory'"));
        assert!(err.contains("--url"));
    }
}
