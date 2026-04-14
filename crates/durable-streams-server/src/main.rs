use axum_server::{Handle, tls_rustls::RustlsConfig};
use clap::{Parser, Subcommand, ValueEnum};
use durable_streams_server::{
    config::{Config, ConfigLoadOptions, StorageMode},
    router,
    storage::{Storage, acid::AcidStorage, file::FileStorage, memory::InMemoryStorage},
    transfer::{
        export::{ExportOptions, export_streams},
        import::{ConflictPolicy, ImportOptions, import_streams},
    },
};
use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
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
    fn new(config: Config) -> Result<Self, String> {
        let addr = format!("0.0.0.0:{}", config.port)
            .parse::<SocketAddr>()
            .map_err(|e| format!("failed to parse bind address: {e}"))?;
        Ok(Self { config, addr })
    }

    fn provision(&self) {
        tracing::info!("Starting durable streams server on {}", self.addr);
        tracing::info!(
            "Max memory: {} bytes, Max per stream: {} bytes",
            self.config.max_memory_bytes,
            self.config.max_stream_bytes
        );
        tracing::info!("Storage mode: {}", self.config.storage_mode.as_str());
        if self.config.tls_enabled() {
            tracing::info!("Transport: direct TLS enabled");
        } else {
            tracing::info!("Transport: plain HTTP (terminate TLS at proxy/edge)");
        }
    }

    fn validate(&self) -> Result<(), String> {
        self.config.validate()?;
        if let (Some(cert), Some(key)) = (&self.config.tls_cert_path, &self.config.tls_key_path) {
            ensure_regular_file(cert)?;
            ensure_regular_file(key)?;
        }
        Ok(())
    }

    fn cleanup() {
        tracing::info!("Runtime cleanup completed");
    }
}

fn ensure_regular_file(path: &str) -> Result<(), String> {
    let metadata = std::fs::metadata(Path::new(path))
        .map_err(|e| format!("failed to stat path '{path}': {e}"))?;
    if !metadata.is_file() {
        return Err(format!("path is not a regular file: '{path}'"));
    }
    Ok(())
}

// ── Main ────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    let load_options = ConfigLoadOptions {
        profile: cli.profile,
        config_override: cli.config,
        ..ConfigLoadOptions::default()
    };

    let config = match Config::from_sources(&load_options) {
        Ok(config) => config,
        Err(err) => {
            eprintln!("{err}");
            std::process::exit(1);
        }
    };

    match cli.command.unwrap_or(Command::Serve) {
        Command::Serve => {
            tracing_subscriber::registry()
                .with(
                    tracing_subscriber::EnvFilter::try_from_default_env()
                        .unwrap_or_else(|_| config.rust_log.clone().into()),
                )
                .with(tracing_subscriber::fmt::layer())
                .init();

            if let Err(err) = run_serve(config).await {
                tracing::error!("{err}");
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
    match config.storage_mode {
        StorageMode::Memory => {
            let storage = InMemoryStorage::new(config.max_memory_bytes, config.max_stream_bytes);
            f(&storage)
        }
        StorageMode::FileFast | StorageMode::FileDurable => {
            let sync_on_append = config.storage_mode.sync_on_append();
            let storage = FileStorage::new(
                &config.data_dir,
                config.max_memory_bytes,
                config.max_stream_bytes,
                sync_on_append,
            )
            .map_err(|e| format!("Failed to initialize file storage: {e}"))?;
            f(&storage)
        }
        StorageMode::Acid => {
            let storage = AcidStorage::new(
                &config.data_dir,
                config.acid_shard_count,
                config.max_memory_bytes,
                config.max_stream_bytes,
                config.acid_backend,
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
        format!("{}...", &s[..max.saturating_sub(3)])
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

async fn run_serve(config: Config) -> Result<(), String> {
    let runtime = AppRuntime::new(config)?;
    runtime.provision();
    runtime.validate()?;

    let serve_result = match runtime.config.storage_mode {
        StorageMode::Memory => {
            let storage = Arc::new(InMemoryStorage::new(
                runtime.config.max_memory_bytes,
                runtime.config.max_stream_bytes,
            ));
            serve(storage, &runtime).await
        }
        StorageMode::FileFast | StorageMode::FileDurable => {
            let sync_on_append = runtime.config.storage_mode.sync_on_append();
            tracing::info!(
                "File storage dir: {}, sync on append: {}",
                runtime.config.data_dir,
                sync_on_append
            );
            let storage = Arc::new(
                FileStorage::new(
                    &runtime.config.data_dir,
                    runtime.config.max_memory_bytes,
                    runtime.config.max_stream_bytes,
                    sync_on_append,
                )
                .map_err(|e| format!("Failed to initialize file storage: {e}"))?,
            );
            serve(storage, &runtime).await
        }
        StorageMode::Acid => {
            tracing::info!(
                "Acid storage backend: {}, dir: {}, shards: {}",
                runtime.config.acid_backend.as_str(),
                runtime.config.data_dir,
                runtime.config.acid_shard_count
            );
            let storage = Arc::new(
                AcidStorage::new(
                    &runtime.config.data_dir,
                    runtime.config.acid_shard_count,
                    runtime.config.max_memory_bytes,
                    runtime.config.max_stream_bytes,
                    runtime.config.acid_backend,
                )
                .map_err(|e| format!("Failed to initialize acid storage: {e}"))?,
            );
            serve(storage, &runtime).await
        }
    };

    AppRuntime::cleanup();
    serve_result
}

async fn serve<S: Storage + 'static>(storage: Arc<S>, runtime: &AppRuntime) -> Result<(), String> {
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

    tracing::info!("Server listening on {}", runtime.addr);
    if runtime.config.tls_enabled() {
        tracing::info!("Health check: https://{}/healthz", runtime.addr);
        tracing::info!("Readiness:    https://{}/readyz", runtime.addr);
        tracing::info!(
            "Protocol base: https://{}{}/",
            runtime.addr,
            runtime.config.stream_base_path
        );
    } else {
        tracing::info!("Health check: http://{}/healthz", runtime.addr);
        tracing::info!("Readiness:    http://{}/readyz", runtime.addr);
        tracing::info!(
            "Protocol base: http://{}{}/",
            runtime.addr,
            runtime.config.stream_base_path
        );
    }

    let shutdown_handle = handle.clone();
    tokio::spawn(async move {
        wait_for_shutdown_signal().await;
        tracing::info!("Shutdown signal received, beginning graceful drain");
        shutdown.cancel();
        shutdown_handle.graceful_shutdown(Some(Duration::from_secs(30)));
    });

    if let (Some(cert_path), Some(key_path)) =
        (&runtime.config.tls_cert_path, &runtime.config.tls_key_path)
    {
        let tls = RustlsConfig::from_pem_file(cert_path, key_path)
            .await
            .map_err(|e| format!("failed to load TLS config: {e}"))?;
        axum_server::bind_rustls(runtime.addr, tls)
            .handle(handle)
            .serve(app.into_make_service())
            .await
            .map_err(|e| format!("server error: {e}"))?;
    } else {
        axum_server::bind(runtime.addr)
            .handle(handle)
            .serve(app.into_make_service())
            .await
            .map_err(|e| format!("server error: {e}"))?;
    }

    Ok(())
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
