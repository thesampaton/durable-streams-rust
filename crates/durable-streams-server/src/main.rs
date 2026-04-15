use axum_server::{
    Handle, from_tcp,
    tls_rustls::{RustlsConfig, from_tcp_rustls},
};
use clap::{Parser, Subcommand, ValueEnum};
use durable_streams_server::{
    config::{Config, ConfigLoadOptions, DeploymentProfile, StorageMode, TransportMode},
    router,
    startup::{
        StartupError, StartupPhase, bind_tcp_listener, build_tls_server_config, log_phase,
        log_startup_failure, log_transport_summary, preflight_tls_files,
    },
    storage::{Storage, acid::AcidStorage, file::FileStorage, memory::InMemoryStorage},
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
    /// List all streams with their metadata
    List {
        /// Output as JSON instead of a table
        #[arg(long)]
        json: bool,
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

    let load_options = ConfigLoadOptions {
        profile: DeploymentProfile::from(cli.profile),
        config_override: cli.config,
        ..ConfigLoadOptions::default()
    };

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
        Command::List { json } => {
            if let Err(err) = run_with_storage(&config, |storage| run_list(storage, json)) {
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

// ── Storage factory for CLI commands ────────────────────────────────

fn run_with_storage<F>(config: &Config, f: F) -> Result<(), String>
where
    F: FnOnce(&dyn Storage) -> Result<(), String>,
{
    match config.storage.mode {
        StorageMode::Memory => {
            let storage = InMemoryStorage::new(
                config.limits.max_memory_bytes,
                config.limits.max_stream_bytes,
            );
            f(&storage)
        }
        StorageMode::FileFast | StorageMode::FileDurable => {
            let sync_on_append = config.storage.mode.sync_on_append();
            let storage = FileStorage::new(
                &config.storage.data_dir,
                config.limits.max_memory_bytes,
                config.limits.max_stream_bytes,
                sync_on_append,
            )
            .map_err(|e| format!("Failed to initialize file storage: {e}"))?;
            f(&storage)
        }
        StorageMode::Acid => {
            let storage = AcidStorage::new(
                &config.storage.data_dir,
                config.storage.acid_shard_count,
                config.limits.max_memory_bytes,
                config.limits.max_stream_bytes,
                config.storage.acid_backend,
            )
            .map_err(|e| format!("Failed to initialize acid storage: {e}"))?;
            f(&storage)
        }
    }
}

// ── List command ────────────────────────────────────────────────────

fn run_list(storage: &dyn Storage, json: bool) -> Result<(), String> {
    let streams = storage
        .list_streams()
        .map_err(|e| format!("failed to list streams: {e}"))?;

    if json {
        print_streams_json(&streams);
    } else {
        print_streams_table(&streams);
    }
    Ok(())
}

fn print_streams_json(streams: &[(String, durable_streams_server::storage::StreamMetadata)]) {
    use serde::Serialize;

    #[derive(Serialize)]
    struct StreamInfo {
        name: String,
        status: String,
        message_count: u64,
        total_bytes: u64,
        content_type: String,
        created_at: String,
        updated_at: Option<String>,
        ttl_seconds: Option<u64>,
        expires_at: Option<String>,
    }

    let entries: Vec<StreamInfo> = streams
        .iter()
        .map(|(name, meta)| StreamInfo {
            name: name.clone(),
            status: if meta.closed {
                "closed".to_string()
            } else {
                "open".to_string()
            },
            message_count: meta.message_count,
            total_bytes: meta.total_bytes,
            content_type: meta.config.content_type.clone(),
            created_at: meta.created_at.to_rfc3339(),
            updated_at: meta.updated_at.map(|t| t.to_rfc3339()),
            ttl_seconds: meta.config.ttl_seconds,
            expires_at: meta.config.expires_at.map(|t| t.to_rfc3339()),
        })
        .collect();

    println!(
        "{}",
        serde_json::to_string_pretty(&entries).expect("JSON serialization should not fail")
    );
}

fn print_streams_table(streams: &[(String, durable_streams_server::storage::StreamMetadata)]) {
    if streams.is_empty() {
        println!("No streams found.");
        return;
    }

    println!(
        "{:<30} {:<8} {:>10} {:>12} {:<24} {:<22} {:<22}",
        "Name", "Status", "Messages", "Bytes", "Content-Type", "Created", "Updated"
    );
    println!("{}", "-".repeat(132));

    for (name, meta) in streams {
        let status = if meta.closed { "closed" } else { "open" };
        let bytes = format_bytes(meta.total_bytes);
        let created = meta.created_at.format("%Y-%m-%d %H:%M:%S").to_string();
        let updated = meta.updated_at.map_or_else(
            || "-".to_string(),
            |t| t.format("%Y-%m-%d %H:%M:%S").to_string(),
        );

        println!(
            "{:<30} {:<8} {:>10} {:>12} {:<24} {:<22} {:<22}",
            truncate(name, 30),
            status,
            meta.message_count,
            bytes,
            truncate(&meta.config.content_type, 24),
            created,
            updated
        );
    }

    println!();
    println!("{} stream(s) total", streams.len());
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

#[cfg(test)]
mod tests {
    use super::truncate;

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
            let storage = Arc::new(InMemoryStorage::new(
                runtime.config.limits.max_memory_bytes,
                runtime.config.limits.max_stream_bytes,
            ));
            serve(storage, &runtime).await
        }
        StorageMode::FileFast | StorageMode::FileDurable => {
            let sync_on_append = runtime.config.storage.mode.sync_on_append();
            tracing::info!(
                storage.dir = runtime.config.storage.data_dir,
                storage.sync_on_append = sync_on_append,
                "file storage initialized"
            );
            let storage = Arc::new(
                FileStorage::new(
                    &runtime.config.storage.data_dir,
                    runtime.config.limits.max_memory_bytes,
                    runtime.config.limits.max_stream_bytes,
                    sync_on_append,
                )
                .map_err(|e| {
                    StartupError::runtime(format!("failed to initialize file storage: {e}"))
                })?,
            );
            serve(storage, &runtime).await
        }
        StorageMode::Acid => {
            tracing::info!(
                storage.backend = runtime.config.storage.acid_backend.as_str(),
                storage.dir = runtime.config.storage.data_dir,
                storage.shards = runtime.config.storage.acid_shard_count,
                "acid storage initialized"
            );
            let storage = Arc::new(
                AcidStorage::new(
                    &runtime.config.storage.data_dir,
                    runtime.config.storage.acid_shard_count,
                    runtime.config.limits.max_memory_bytes,
                    runtime.config.limits.max_stream_bytes,
                    runtime.config.storage.acid_backend,
                )
                .map_err(|e| {
                    StartupError::runtime(format!("failed to initialize acid storage: {e}"))
                })?,
            );
            serve(storage, &runtime).await
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
    let app = router::build_router_with_ready(
        storage,
        &runtime.config,
        Some(Arc::clone(&ready)),
        shutdown.clone(),
    );
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

    match runtime.config.transport.mode {
        TransportMode::Http => {
            log_phase(StartupPhase::BindListener);
            let listener = bind_tcp_listener(runtime.addr)?;
            log_bound_endpoints(runtime);
            log_phase(StartupPhase::StartServer);
            from_tcp(listener)
                .map_err(|error| StartupError::bind(runtime.addr, error))?
                .handle(handle)
                .serve(app.into_make_service_with_connect_info::<SocketAddr>())
                .await
                .map_err(|e| StartupError::runtime(e.to_string()))?;
        }
        TransportMode::Tls | TransportMode::Mtls => {
            log_phase(StartupPhase::BuildTlsContext);
            let server_config = build_tls_server_config(&runtime.config)?;
            let tls = RustlsConfig::from_config(Arc::new(server_config));
            tracing::info!(
                transport.mode = runtime.config.transport.mode.as_str(),
                "TLS context built successfully"
            );

            log_phase(StartupPhase::BindListener);
            let listener = bind_tcp_listener(runtime.addr)?;
            log_bound_endpoints(runtime);
            log_phase(StartupPhase::StartServer);
            from_tcp_rustls(listener, tls)
                .map_err(|error| StartupError::bind(runtime.addr, error))?
                .handle(handle)
                .serve(app.into_make_service_with_connect_info::<SocketAddr>())
                .await
                .map_err(|e| StartupError::runtime(e.to_string()))?;
        }
    }

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
