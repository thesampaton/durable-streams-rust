//! Thin CLI for JSON and JSONL persistence, replication, and send workflows.

use clap::{Args, Parser, Subcommand, ValueEnum};
use durable_streams_client::{
    Client, ClientConfig, ClientConfigLoader, IdempotentProducer, IdempotentProducerConfig,
    JournalDirection, JournalStreamIdentity, JsonJournal, LiveMode, ReadReplica, ReadRequest,
    RequestOptions, load_json_input,
};
use std::error::Error;
use std::path::PathBuf;
use std::time::Duration;
use url::Url;

#[derive(Parser)]
#[command(name = "durable-streams-json")]
#[command(
    about = "JSON and JSONL ingest, replication, and send workflows for durable-streams-client"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Persist(PersistCommand),
    Replicate(ReplicateCommand),
    Send(SendCommand),
}

#[derive(Args)]
struct PersistCommand {
    #[arg(long)]
    journal: PathBuf,
    #[arg(long)]
    stream: String,
    #[arg(long)]
    content_type: String,
    #[arg(long)]
    input: PathBuf,
    #[arg(long, value_enum, default_value_t = CliDirection::Inbound)]
    direction: CliDirection,
    #[arg(long)]
    next_offset: Option<String>,
}

#[derive(Args)]
struct ReplicateCommand {
    #[command(flatten)]
    client: ClientArgs,
    #[arg(long)]
    journal: PathBuf,
    #[arg(long)]
    stream: String,
    #[arg(long)]
    content_type: String,
    #[arg(long, value_enum, default_value_t = CliLiveMode::CatchUp)]
    live: CliLiveMode,
    #[arg(long)]
    timeout_ms: Option<u64>,
    #[arg(long)]
    max_chunks: Option<usize>,
    #[arg(long)]
    wait_for_up_to_date: bool,
}

#[derive(Args)]
struct SendCommand {
    #[command(flatten)]
    client: ClientArgs,
    #[arg(long)]
    stream: String,
    #[arg(long)]
    content_type: String,
    #[arg(long)]
    input: PathBuf,
    #[arg(long)]
    producer_id: String,
    #[arg(long, default_value_t = 0)]
    producer_epoch: i64,
    #[arg(long, default_value_t = false)]
    auto_claim: bool,
    #[arg(long)]
    journal: Option<PathBuf>,
}

#[derive(Args)]
struct ClientArgs {
    #[arg(long)]
    config: Option<PathBuf>,
    #[arg(long)]
    base_url: Option<String>,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum CliDirection {
    Inbound,
    Outbound,
}

impl From<CliDirection> for JournalDirection {
    fn from(value: CliDirection) -> Self {
        match value {
            CliDirection::Inbound => Self::Inbound,
            CliDirection::Outbound => Self::Outbound,
        }
    }
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum CliLiveMode {
    CatchUp,
    LongPoll,
    Auto,
}

impl From<CliLiveMode> for LiveMode {
    fn from(value: CliLiveMode) -> Self {
        match value {
            CliLiveMode::CatchUp => Self::CatchUp,
            CliLiveMode::LongPoll => Self::LongPoll,
            CliLiveMode::Auto => Self::Auto,
        }
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    let cli = Cli::parse();
    match cli.command {
        Command::Persist(command) => persist(command)?,
        Command::Replicate(command) => replicate(command).await?,
        Command::Send(command) => send(command).await?,
    }
    Ok(())
}

fn persist(command: PersistCommand) -> Result<(), Box<dyn Error>> {
    let input = load_json_input(&command.input)?;
    let stream = JournalStreamIdentity::new(command.stream, command.content_type)?;
    let mut journal = JsonJournal::open(&command.journal, stream)?;
    let appended = journal.append_values(
        command.direction.into(),
        input.values().iter().cloned(),
        command.next_offset,
        None,
    )?;

    println!(
        "{}",
        serde_json::json!({
            "command": "persist",
            "format": format!("{:?}", input.format()).to_ascii_lowercase(),
            "appended": appended.len(),
            "total": journal.records().len(),
            "resumeOffset": journal.resume_offset(),
        })
    );
    Ok(())
}

async fn replicate(command: ReplicateCommand) -> Result<(), Box<dyn Error>> {
    let client = Client::new(load_client_config(&command.client)?)?;
    let stream = JournalStreamIdentity::new(command.stream, command.content_type)?;
    let mut replica = ReadReplica::open(client, &command.journal, stream)?;
    let result = replica
        .replicate(ReadRequest {
            offset: None,
            live: command.live.into(),
            timeout: command.timeout_ms.map(Duration::from_millis),
            max_chunks: command.max_chunks,
            wait_for_up_to_date: command.wait_for_up_to_date,
            cursor: None,
            if_none_match: None,
            options: RequestOptions::default(),
        })
        .await?;

    println!(
        "{}",
        serde_json::json!({
            "command": "replicate",
            "appended": result.appended,
            "total": result.total,
            "nextOffset": result.next_offset,
            "upToDate": result.up_to_date,
            "streamClosed": result.stream_closed,
        })
    );
    Ok(())
}

async fn send(command: SendCommand) -> Result<(), Box<dyn Error>> {
    let client = Client::new(load_client_config(&command.client)?)?;
    let input = load_json_input(&command.input)?;
    let stream = JournalStreamIdentity::new(command.stream.clone(), command.content_type.clone())?;
    let producer = IdempotentProducer::new(
        client,
        command.stream,
        command.content_type,
        RequestOptions::default(),
        IdempotentProducerConfig {
            producer_id: command.producer_id,
            epoch: command.producer_epoch,
            auto_claim: command.auto_claim,
            ..IdempotentProducerConfig::default()
        },
    )?;

    let response = producer.append_json_values(input.values()).await?;
    if let Some(journal_path) = command.journal {
        let mut journal = JsonJournal::open(journal_path, stream)?;
        journal.append_values(
            JournalDirection::Outbound,
            input.into_values(),
            response.next_offset.clone(),
            Some(producer.progress().await),
        )?;
    }

    println!(
        "{}",
        serde_json::json!({
            "command": "send",
            "status": response.status,
            "nextOffset": response.next_offset,
            "streamClosed": response.stream_closed,
            "producer": producer.progress().await,
        })
    );
    Ok(())
}

fn load_client_config(args: &ClientArgs) -> Result<ClientConfig, Box<dyn Error>> {
    let mut config = if let Some(path) = &args.config {
        ClientConfigLoader::load_from_path(path)?
    } else {
        ClientConfigLoader::default().load()?
    };

    if let Some(base_url) = &args.base_url {
        config.base_url = Url::parse(base_url)?;
    }

    Ok(config)
}
