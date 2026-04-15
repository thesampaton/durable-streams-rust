#![allow(missing_docs)]
#![allow(dead_code)]

use base64::Engine;
use bytes::Bytes;
use durable_streams_client::{
    Client, ClientConfig, Error, ErrorCode, ErrorKind, IdempotentProducer,
    IdempotentProducerConfig, LiveMode, RequestOptions,
    raw::{
        AppendRequest, CloseStreamRequest, ConnectRequest, CreateStreamRequest, DeleteRequest,
        HeadRequest, ProducerRequest, ReadRequest,
    },
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::{self, BufRead, Write};
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
enum Command {
    Init {
        #[serde(rename = "serverUrl")]
        server_url: String,
        #[serde(rename = "timeoutMs")]
        timeout_ms: Option<u64>,
    },
    Create {
        path: String,
        #[serde(rename = "contentType")]
        content_type: Option<String>,
        #[serde(rename = "ttlSeconds")]
        ttl_seconds: Option<u64>,
        #[serde(rename = "expiresAt")]
        expires_at: Option<String>,
        headers: Option<HashMap<String, String>>,
        closed: Option<bool>,
        data: Option<String>,
    },
    Connect {
        path: String,
        headers: Option<HashMap<String, String>>,
    },
    Append {
        path: String,
        data: String,
        binary: Option<bool>,
        seq: Option<i64>,
        headers: Option<HashMap<String, String>>,
        #[serde(rename = "producerId")]
        producer_id: Option<String>,
        #[serde(rename = "producerEpoch")]
        producer_epoch: Option<i64>,
        #[serde(rename = "producerSeq")]
        producer_seq: Option<i64>,
    },
    Read {
        path: String,
        offset: Option<String>,
        live: Option<serde_json::Value>,
        #[serde(rename = "timeoutMs")]
        timeout_ms: Option<u64>,
        #[serde(rename = "maxChunks")]
        max_chunks: Option<usize>,
        #[serde(rename = "waitForUpToDate")]
        wait_for_up_to_date: Option<bool>,
        headers: Option<HashMap<String, String>>,
    },
    Head {
        path: String,
        headers: Option<HashMap<String, String>>,
    },
    Delete {
        path: String,
        headers: Option<HashMap<String, String>>,
    },
    Close {
        path: String,
        data: Option<String>,
        content_type: Option<String>,
    },
    Shutdown,
    #[serde(rename = "set-dynamic-header")]
    SetDynamicHeader {
        name: String,
        #[serde(rename = "valueType")]
        value_type: String,
        #[serde(rename = "initialValue")]
        initial_value: Option<String>,
    },
    #[serde(rename = "set-dynamic-param")]
    SetDynamicParam {
        name: String,
        #[serde(rename = "valueType")]
        value_type: String,
    },
    #[serde(rename = "clear-dynamic")]
    ClearDynamic,
    Validate {
        target: ValidateTarget,
    },
    #[serde(rename = "idempotent-append")]
    IdempotentAppend {
        path: String,
        data: String,
        #[serde(rename = "producerId")]
        producer_id: String,
        epoch: Option<i64>,
        #[serde(rename = "autoClaim")]
        auto_claim: Option<bool>,
        headers: Option<HashMap<String, String>>,
    },
    #[serde(rename = "idempotent-append-batch")]
    IdempotentAppendBatch {
        path: String,
        items: Vec<BatchItem>,
        #[serde(rename = "producerId")]
        producer_id: String,
        epoch: Option<i64>,
        #[serde(rename = "autoClaim")]
        auto_claim: Option<bool>,
        #[serde(rename = "maxInFlight")]
        max_in_flight: Option<usize>,
        headers: Option<HashMap<String, String>>,
    },
    #[serde(rename = "idempotent-close")]
    IdempotentClose {
        path: String,
        #[serde(rename = "producerId")]
        producer_id: String,
        epoch: Option<i64>,
        data: Option<String>,
        #[serde(rename = "autoClaim")]
        auto_claim: Option<bool>,
        headers: Option<HashMap<String, String>>,
    },
    #[serde(rename = "idempotent-detach")]
    IdempotentDetach {
        path: String,
        #[serde(rename = "producerId")]
        producer_id: String,
        epoch: Option<i64>,
        headers: Option<HashMap<String, String>>,
    },
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum BatchItem {
    Value(String),
    Wrapped {
        #[serde(rename = "data")]
        data: String,
    },
}

impl BatchItem {
    fn into_data(self) -> String {
        match self {
            Self::Value(value) | Self::Wrapped { data: value } => value,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(tag = "target", rename_all = "camelCase")]
enum ValidateTarget {
    #[serde(alias = "retry-options")]
    RetryOptions {
        #[serde(rename = "maxRetries")]
        max_retries: Option<i64>,
        #[serde(rename = "initialDelayMs")]
        initial_delay_ms: Option<i64>,
        #[serde(rename = "maxDelayMs")]
        max_delay_ms: Option<i64>,
        multiplier: Option<f64>,
    },
    #[serde(alias = "idempotent-producer")]
    IdempotentProducer {
        #[serde(rename = "producerId")]
        producer_id: Option<String>,
        epoch: Option<i64>,
        #[serde(rename = "maxBatchBytes")]
        max_batch_bytes: Option<i64>,
        #[serde(rename = "maxBatchItems")]
        max_batch_items: Option<i64>,
    },
}

#[derive(Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
struct SuccessResult {
    #[serde(rename = "type")]
    result_type: String,
    success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    status: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    offset: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    final_offset: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    up_to_date: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    stream_closed: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    content_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    headers_sent: Option<HashMap<String, String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    params_sent: Option<HashMap<String, String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    chunks: Option<Vec<ReadChunkResult>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    client_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    client_version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    features: Option<FeatureFlags>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ReadChunkResult {
    data: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    offset: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct FeatureFlags {
    #[serde(flatten)]
    read_modes: ReadModeFeatures,
    #[serde(flatten)]
    data: DataFeatures,
    #[serde(flatten)]
    protocol: ProtocolFeatures,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ReadModeFeatures {
    sse: bool,
    long_poll: bool,
    auto: bool,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct DataFeatures {
    batching: bool,
    streaming: bool,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ProtocolFeatures {
    dynamic_headers: bool,
    strict_zero_validation: bool,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ErrorResult {
    #[serde(rename = "type")]
    result_type: &'static str,
    success: bool,
    command_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    status: Option<u16>,
    error_code: &'static str,
    message: String,
}

#[derive(Default)]
struct DynamicValue {
    kind: String,
    counter: u64,
    token: Option<String>,
}

struct AdapterState {
    client: Option<Client>,
    timeout_ms: u64,
    server_url: Option<String>,
    content_types: HashMap<String, String>,
    producers: HashMap<String, Arc<IdempotentProducer>>,
    dynamic_headers: HashMap<String, DynamicValue>,
    dynamic_params: HashMap<String, DynamicValue>,
}

impl Default for AdapterState {
    fn default() -> Self {
        Self {
            client: None,
            timeout_ms: 30_000,
            server_url: None,
            content_types: HashMap::new(),
            producers: HashMap::new(),
            dynamic_headers: HashMap::new(),
            dynamic_params: HashMap::new(),
        }
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let stdin = io::stdin();
    let mut stdout = io::stdout();
    let state = Arc::new(Mutex::new(AdapterState::default()));

    for line in stdin.lock().lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }

        let command: Command = serde_json::from_str(&line)?;
        let result = handle_command(state.clone(), command).await;
        let mut encoded = serde_json::to_string(&result)?;
        encoded = encoded
            .replace('\u{0085}', "\\u0085")
            .replace('\u{2028}', "\\u2028")
            .replace('\u{2029}', "\\u2029");
        writeln!(stdout, "{encoded}")?;
        stdout.flush()?;
        if matches!(result, AdapterOutput::Success(SuccessResult { result_type, .. }) if result_type == "shutdown")
        {
            break;
        }
    }

    Ok(())
}

#[derive(Debug, Serialize)]
#[serde(untagged)]
enum AdapterOutput {
    Success(SuccessResult),
    Error(ErrorResult),
}

struct CreateParams {
    path: String,
    content_type: Option<String>,
    ttl_seconds: Option<u64>,
    expires_at: Option<String>,
    headers: Option<HashMap<String, String>>,
    closed: Option<bool>,
    data: Option<String>,
}

struct AppendParams {
    path: String,
    data: String,
    binary: Option<bool>,
    seq: Option<i64>,
    headers: Option<HashMap<String, String>>,
    producer_id: Option<String>,
    producer_epoch: Option<i64>,
    producer_seq: Option<i64>,
}

struct ReadParams {
    path: String,
    offset: Option<String>,
    live: Option<serde_json::Value>,
    timeout_ms: Option<u64>,
    max_chunks: Option<usize>,
    wait_for_up_to_date: Option<bool>,
    headers: Option<HashMap<String, String>>,
}

async fn handle_command(state: Arc<Mutex<AdapterState>>, command: Command) -> AdapterOutput {
    match command {
        Command::Init {
            server_url,
            timeout_ms,
        } => handle_init(&state, server_url, timeout_ms).await,
        Command::Create {
            path,
            content_type,
            ttl_seconds,
            expires_at,
            headers,
            closed,
            data,
        } => handle_create(&state, CreateParams { path, content_type, ttl_seconds, expires_at, headers, closed, data }).await,
        Command::Connect { path, headers } => handle_connect(&state, path, headers).await,
        Command::Append {
            path,
            data,
            binary,
            seq,
            headers,
            producer_id,
            producer_epoch,
            producer_seq,
        } => handle_append(&state, AppendParams { path, data, binary, seq, headers, producer_id, producer_epoch, producer_seq }).await,
        Command::Read {
            path,
            offset,
            live,
            timeout_ms,
            max_chunks,
            wait_for_up_to_date,
            headers,
        } => handle_read(&state, ReadParams { path, offset, live, timeout_ms, max_chunks, wait_for_up_to_date, headers }).await,
        Command::Head { path, headers } => handle_head(&state, path, headers).await,
        Command::Delete { path, headers } => handle_delete(&state, path, headers).await,
        Command::Close {
            path,
            data,
            content_type,
        } => handle_close(&state, path, data, content_type).await,
        Command::SetDynamicHeader {
            name,
            value_type,
            initial_value,
        } => handle_set_dynamic_header(&state, name, value_type, initial_value).await,
        Command::SetDynamicParam { name, value_type } => {
            handle_set_dynamic_param(&state, name, value_type).await
        }
        Command::ClearDynamic => handle_clear_dynamic(&state).await,
        Command::Validate { target } => handle_validate(target),
        Command::IdempotentAppend {
            path,
            data,
            producer_id,
            epoch,
            auto_claim,
            headers,
        } => handle_idempotent_append(&state, path, data, producer_id, epoch, auto_claim, headers).await,
        Command::IdempotentAppendBatch {
            path,
            items,
            producer_id,
            epoch,
            auto_claim,
            max_in_flight: _,
            headers,
        } => handle_idempotent_append_batch(&state, path, items, producer_id, epoch, auto_claim, headers).await,
        Command::IdempotentClose {
            path,
            producer_id,
            epoch,
            data,
            auto_claim,
            headers,
        } => handle_idempotent_close(&state, path, producer_id, epoch, data, auto_claim, headers).await,
        Command::IdempotentDetach {
            path,
            producer_id,
            epoch,
            headers: _,
        } => handle_idempotent_detach(&state, path, producer_id, epoch).await,
        Command::Shutdown => AdapterOutput::Success(empty_success("shutdown")),
    }
}

async fn handle_init(
    state: &Arc<Mutex<AdapterState>>,
    server_url: String,
    timeout_ms: Option<u64>,
) -> AdapterOutput {
    let config = ClientConfig {
        base_url: match url::Url::parse(&server_url) {
            Ok(url) => url,
            Err(error) => {
                return AdapterOutput::Error(error_output(
                    "init",
                    None,
                    ErrorCode::InvalidArgument,
                    error.to_string(),
                ));
            }
        },
        ..ClientConfig::default()
    };
    let client = match Client::new(config) {
        Ok(client) => client,
        Err(error) => return AdapterOutput::Error(error_to_output("init", None, &error)),
    };

    let mut state = state.lock().await;
    state.client = Some(client);
    state.server_url = Some(server_url);
    state.timeout_ms = timeout_ms.unwrap_or(30_000);
    state.content_types.clear();
    state.producers.clear();
    state.dynamic_headers.clear();
    state.dynamic_params.clear();

    AdapterOutput::Success(SuccessResult {
        result_type: "init".to_string(),
        success: true,
        client_name: Some("durable-streams-client-rust".to_string()),
        client_version: Some(env!("CARGO_PKG_VERSION").to_string()),
        features: Some(FeatureFlags {
            read_modes: ReadModeFeatures {
                sse: true,
                long_poll: true,
                auto: true,
            },
            data: DataFeatures {
                batching: false,
                streaming: true,
            },
            protocol: ProtocolFeatures {
                dynamic_headers: true,
                strict_zero_validation: true,
            },
        }),
        ..Default::default()
    })
}

async fn handle_create(
    state: &Arc<Mutex<AdapterState>>,
    params: CreateParams,
) -> AdapterOutput {
    let CreateParams { path, content_type, ttl_seconds, expires_at, headers, closed, data } = params;
    let mut state = state.lock().await;
    let Some(client) = state.client.clone() else {
        return AdapterOutput::Error(error_output(
            "create",
            None,
            ErrorCode::InternalError,
            "client adapter not initialized",
        ));
    };
    let content_type = content_type.unwrap_or_else(|| "application/octet-stream".to_string());
    let request = CreateStreamRequest {
        content_type: content_type.clone(),
        ttl_seconds,
        expires_at,
        closed: closed.unwrap_or(false),
        body: data.map(Bytes::from),
        options: RequestOptions {
            headers: headers.unwrap_or_default(),
            query: HashMap::new(),
        },
    };
    match client.create_raw(&path, &request).await {
        Ok(result) => {
            state.content_types.insert(path.clone(), content_type);
            AdapterOutput::Success(SuccessResult {
                result_type: "create".to_string(),
                success: true,
                status: Some(result.status),
                offset: result.next_offset,
                stream_closed: Some(result.stream_closed),
                ..Default::default()
            })
        }
        Err(error) => AdapterOutput::Error(error_to_output("create", Some(&path), &error)),
    }
}

async fn handle_connect(
    state: &Arc<Mutex<AdapterState>>,
    path: String,
    headers: Option<HashMap<String, String>>,
) -> AdapterOutput {
    let mut state = state.lock().await;
    let Some(client) = state.client.clone() else {
        return AdapterOutput::Error(error_output(
            "connect",
            None,
            ErrorCode::InternalError,
            "client adapter not initialized",
        ));
    };
    match client
        .connect_raw(
            &path,
            &ConnectRequest {
                options: RequestOptions {
                    headers: headers.unwrap_or_default(),
                    query: HashMap::new(),
                },
            },
        )
        .await
    {
        Ok(result) => {
            if let Some(content_type) = &result.content_type {
                state
                    .content_types
                    .insert(path.clone(), content_type.clone());
            }
            AdapterOutput::Success(SuccessResult {
                result_type: "connect".to_string(),
                success: true,
                status: Some(result.status),
                offset: result.offset,
                stream_closed: Some(result.stream_closed),
                ..Default::default()
            })
        }
        Err(error) => AdapterOutput::Error(error_to_output("connect", Some(&path), &error)),
    }
}

async fn handle_append(
    state: &Arc<Mutex<AdapterState>>,
    params: AppendParams,
) -> AdapterOutput {
    let AppendParams { path, data, binary, seq, headers, producer_id, producer_epoch, producer_seq } = params;
    let mut state = state.lock().await;
    let Some(client) = state.client.clone() else {
        return AdapterOutput::Error(error_output(
            "append",
            None,
            ErrorCode::InternalError,
            "client adapter not initialized",
        ));
    };
    let (headers_sent, params_sent, merged_headers) = resolve_dynamic(&mut state, headers);
    let body = if binary.unwrap_or(false) {
        match base64::engine::general_purpose::STANDARD.decode(data.as_bytes()) {
            Ok(body) => Bytes::from(body),
            Err(error) => {
                return AdapterOutput::Error(error_output(
                    "append",
                    None,
                    ErrorCode::ParseError,
                    error.to_string(),
                ));
            }
        }
    } else {
        Bytes::from(data)
    };
    let request = AppendRequest {
        body,
        content_type: Some(
            state
                .content_types
                .get(&path)
                .cloned()
                .unwrap_or_else(|| "application/octet-stream".to_string()),
        ),
        stream_seq: seq.map(|value| value.to_string()),
        producer: producer_id.map(|producer_id| ProducerRequest {
            producer_id,
            producer_epoch: producer_epoch.unwrap_or(0),
            producer_seq: producer_seq.unwrap_or(0),
        }),
        options: RequestOptions {
            headers: merged_headers,
            query: HashMap::new(),
        },
    };
    match client.append_raw(&path, &request).await {
        Ok(result) => AdapterOutput::Success(SuccessResult {
            result_type: "append".to_string(),
            success: true,
            status: Some(200),
            offset: result.next_offset,
            stream_closed: Some(result.stream_closed),
            headers_sent: Some(headers_sent).filter(|m| !m.is_empty()),
            params_sent: Some(params_sent).filter(|m| !m.is_empty()),
            ..Default::default()
        }),
        Err(error) => AdapterOutput::Error(error_to_output("append", Some(&path), &error)),
    }
}

async fn handle_read(
    state: &Arc<Mutex<AdapterState>>,
    params: ReadParams,
) -> AdapterOutput {
    let ReadParams { path, offset, live, timeout_ms, max_chunks, wait_for_up_to_date, headers } = params;
    let mut state = state.lock().await;
    let Some(client) = state.client.clone() else {
        return AdapterOutput::Error(error_output(
            "read",
            None,
            ErrorCode::InternalError,
            "client adapter not initialized",
        ));
    };
    let (headers_sent, params_sent, merged_headers) = resolve_dynamic(&mut state, headers);
    let live = match live {
        Some(serde_json::Value::Bool(true)) => LiveMode::Auto,
        Some(serde_json::Value::String(value)) if value == "long-poll" => LiveMode::LongPoll,
        Some(serde_json::Value::String(value)) if value == "sse" => LiveMode::Sse,
        _ => LiveMode::CatchUp,
    };
    let request = ReadRequest {
        offset: offset.clone(),
        live,
        timeout: Some(std::time::Duration::from_millis(
            timeout_ms.unwrap_or(state.timeout_ms),
        )),
        max_chunks,
        wait_for_up_to_date: wait_for_up_to_date.unwrap_or(false),
        cursor: None,
        if_none_match: None,
        options: RequestOptions {
            headers: merged_headers,
            query: HashMap::new(),
        },
    };
    match client.read_raw(&path, &request).await {
        Ok(result) => {
            let chunks = result
                .chunks
                .iter()
                .map(|chunk| ReadChunkResult {
                    data: String::from_utf8_lossy(&chunk.data).to_string(),
                    offset: Some(chunk.next_offset.clone()),
                })
                .collect::<Vec<_>>();
            AdapterOutput::Success(SuccessResult {
                result_type: "read".to_string(),
                success: true,
                status: Some(result.status),
                offset: Some(result.next_offset),
                up_to_date: Some(result.up_to_date),
                stream_closed: Some(result.stream_closed),
                headers_sent: Some(headers_sent).filter(|m| !m.is_empty()),
                params_sent: Some(params_sent).filter(|m| !m.is_empty()),
                chunks: Some(chunks),
                ..Default::default()
            })
        }
        Err(error) => {
            AdapterOutput::Error(read_error_output(&path, live, offset.as_deref(), &error))
        }
    }
}

async fn handle_head(
    state: &Arc<Mutex<AdapterState>>,
    path: String,
    headers: Option<HashMap<String, String>>,
) -> AdapterOutput {
    let mut state = state.lock().await;
    let Some(client) = state.client.clone() else {
        return AdapterOutput::Error(error_output(
            "head",
            None,
            ErrorCode::InternalError,
            "client adapter not initialized",
        ));
    };
    match client
        .head_raw(
            &path,
            &HeadRequest {
                options: RequestOptions {
                    headers: headers.unwrap_or_default(),
                    query: HashMap::new(),
                },
            },
        )
        .await
    {
        Ok(result) => {
            if let Some(content_type) = &result.content_type {
                state
                    .content_types
                    .insert(path.clone(), content_type.clone());
            }
            AdapterOutput::Success(SuccessResult {
                result_type: "head".to_string(),
                success: true,
                status: Some(result.status),
                offset: result.offset,
                stream_closed: Some(result.stream_closed),
                content_type: result.content_type,
                ..Default::default()
            })
        }
        Err(error) => AdapterOutput::Error(error_to_output("head", Some(&path), &error)),
    }
}

async fn handle_delete(
    state: &Arc<Mutex<AdapterState>>,
    path: String,
    headers: Option<HashMap<String, String>>,
) -> AdapterOutput {
    let state = state.lock().await;
    let Some(client) = state.client.clone() else {
        return AdapterOutput::Error(error_output(
            "delete",
            None,
            ErrorCode::InternalError,
            "client adapter not initialized",
        ));
    };
    match client
        .delete_raw(
            &path,
            &DeleteRequest {
                options: RequestOptions {
                    headers: headers.unwrap_or_default(),
                    query: HashMap::new(),
                },
            },
        )
        .await
    {
        Ok(_result) => AdapterOutput::Success(SuccessResult {
            result_type: "delete".to_string(),
            success: true,
            status: Some(200),
            ..Default::default()
        }),
        Err(error) => AdapterOutput::Error(error_to_output("delete", Some(&path), &error)),
    }
}

async fn handle_close(
    state: &Arc<Mutex<AdapterState>>,
    path: String,
    data: Option<String>,
    content_type: Option<String>,
) -> AdapterOutput {
    let state = state.lock().await;
    let Some(client) = state.client.clone() else {
        return AdapterOutput::Error(error_output(
            "close",
            None,
            ErrorCode::InternalError,
            "client adapter not initialized",
        ));
    };
    let content_type = content_type.or_else(|| state.content_types.get(&path).cloned());
    match client
        .close_raw(
            &path,
            &CloseStreamRequest {
                body: data.map(Bytes::from),
                content_type,
                producer: None,
                options: RequestOptions::default(),
            },
        )
        .await
    {
        Ok(result) => AdapterOutput::Success(SuccessResult {
            result_type: "close".to_string(),
            success: true,
            status: Some(200),
            final_offset: Some(result.final_offset),
            stream_closed: Some(result.stream_closed),
            ..Default::default()
        }),
        Err(error) => AdapterOutput::Error(error_to_output("close", Some(&path), &error)),
    }
}

async fn handle_set_dynamic_header(
    state: &Arc<Mutex<AdapterState>>,
    name: String,
    value_type: String,
    initial_value: Option<String>,
) -> AdapterOutput {
    let mut state = state.lock().await;
    state.dynamic_headers.insert(
        name,
        DynamicValue {
            kind: value_type,
            counter: 0,
            token: initial_value,
        },
    );
    AdapterOutput::Success(empty_success("set-dynamic-header"))
}

async fn handle_set_dynamic_param(
    state: &Arc<Mutex<AdapterState>>,
    name: String,
    value_type: String,
) -> AdapterOutput {
    let mut state = state.lock().await;
    state.dynamic_params.insert(
        name,
        DynamicValue {
            kind: value_type,
            counter: 0,
            token: None,
        },
    );
    AdapterOutput::Success(empty_success("set-dynamic-param"))
}

async fn handle_clear_dynamic(state: &Arc<Mutex<AdapterState>>) -> AdapterOutput {
    let mut state = state.lock().await;
    state.dynamic_headers.clear();
    state.dynamic_params.clear();
    AdapterOutput::Success(empty_success("clear-dynamic"))
}

fn handle_validate(target: ValidateTarget) -> AdapterOutput {
    match validate_target(target) {
        Ok(()) => AdapterOutput::Success(empty_success("validate")),
        Err(error) => AdapterOutput::Error(error_output(
            "validate",
            None,
            ErrorCode::InvalidArgument,
            error,
        )),
    }
}

async fn handle_idempotent_append(
    state: &Arc<Mutex<AdapterState>>,
    path: String,
    data: String,
    producer_id: String,
    epoch: Option<i64>,
    auto_claim: Option<bool>,
    headers: Option<HashMap<String, String>>,
) -> AdapterOutput {
    let mut state = state.lock().await;
    let Some(client) = state.client.clone() else {
        return AdapterOutput::Error(error_output(
            "idempotent-append",
            None,
            ErrorCode::InternalError,
            "client adapter not initialized",
        ));
    };
    match get_or_create_producer(
        client,
        &mut state,
        &path,
        &producer_id,
        epoch.unwrap_or(0),
        auto_claim.unwrap_or(false),
        headers.unwrap_or_default(),
    ) {
        Ok(producer) => match producer.append(data.into_bytes()).await {
            Ok(_) => AdapterOutput::Success(SuccessResult {
                result_type: "idempotent-append".to_string(),
                success: true,
                status: Some(200),
                ..Default::default()
            }),
            Err(error) => AdapterOutput::Error(error_to_output(
                "idempotent-append",
                Some(&path),
                &error,
            )),
        },
        Err(error) => {
            AdapterOutput::Error(error_to_output("idempotent-append", Some(&path), &error))
        }
    }
}

async fn handle_idempotent_append_batch(
    state: &Arc<Mutex<AdapterState>>,
    path: String,
    items: Vec<BatchItem>,
    producer_id: String,
    epoch: Option<i64>,
    auto_claim: Option<bool>,
    headers: Option<HashMap<String, String>>,
) -> AdapterOutput {
    let state = state.lock().await;
    let Some(client) = state.client.clone() else {
        return AdapterOutput::Error(error_output(
            "idempotent-append-batch",
            None,
            ErrorCode::InternalError,
            "client adapter not initialized",
        ));
    };
    match create_producer(
        client,
        &path,
        &producer_id,
        epoch.unwrap_or(0),
        auto_claim.unwrap_or(false),
        headers.unwrap_or_default(),
        state.content_types.get(&path).cloned(),
    ) {
        Ok(producer) => {
            let items = items
                .into_iter()
                .map(BatchItem::into_data)
                .map(String::into_bytes)
                .collect::<Vec<_>>();
            match producer.append_batch(&items).await {
                Ok(_) => {
                    let _ = producer.detach();
                    AdapterOutput::Success(SuccessResult {
                        result_type: "idempotent-append-batch".to_string(),
                        success: true,
                        status: Some(200),
                        ..Default::default()
                    })
                }
                Err(error) => AdapterOutput::Error(error_to_output(
                    "idempotent-append-batch",
                    Some(&path),
                    &error,
                )),
            }
        }
        Err(error) => AdapterOutput::Error(error_to_output(
            "idempotent-append-batch",
            Some(&path),
            &error,
        )),
    }
}

async fn handle_idempotent_close(
    state: &Arc<Mutex<AdapterState>>,
    path: String,
    producer_id: String,
    epoch: Option<i64>,
    data: Option<String>,
    auto_claim: Option<bool>,
    headers: Option<HashMap<String, String>>,
) -> AdapterOutput {
    let mut state = state.lock().await;
    let Some(client) = state.client.clone() else {
        return AdapterOutput::Error(error_output(
            "idempotent-close",
            None,
            ErrorCode::InternalError,
            "client adapter not initialized",
        ));
    };
    match get_or_create_producer(
        client,
        &mut state,
        &path,
        &producer_id,
        epoch.unwrap_or(0),
        auto_claim.unwrap_or(false),
        headers.unwrap_or_default(),
    ) {
        Ok(producer) => match producer.close(data.map(String::into_bytes)).await {
            Ok(result) => AdapterOutput::Success(SuccessResult {
                result_type: "idempotent-close".to_string(),
                success: true,
                status: Some(200),
                final_offset: Some(result.final_offset),
                stream_closed: Some(result.stream_closed),
                ..Default::default()
            }),
            Err(error) => AdapterOutput::Error(error_to_output(
                "idempotent-close",
                Some(&path),
                &error,
            )),
        },
        Err(error) => {
            AdapterOutput::Error(error_to_output("idempotent-close", Some(&path), &error))
        }
    }
}

async fn handle_idempotent_detach(
    state: &Arc<Mutex<AdapterState>>,
    path: String,
    producer_id: String,
    epoch: Option<i64>,
) -> AdapterOutput {
    let mut state = state.lock().await;
    state
        .producers
        .remove(&producer_key(&path, &producer_id, epoch.unwrap_or(0)));
    AdapterOutput::Success(SuccessResult {
        result_type: "idempotent-detach".to_string(),
        success: true,
        status: Some(200),
        ..Default::default()
    })
}

fn empty_success(result_type: &str) -> SuccessResult {
    SuccessResult {
        result_type: result_type.to_string(),
        success: true,
        ..Default::default()
    }
}

fn error_output(
    command_type: &str,
    status: Option<u16>,
    error_code: ErrorCode,
    message: impl Into<String>,
) -> ErrorResult {
    ErrorResult {
        result_type: "error",
        success: false,
        command_type: command_type.to_string(),
        status,
        error_code: match error_code {
            ErrorCode::NetworkError => "NETWORK_ERROR",
            ErrorCode::Timeout => "TIMEOUT",
            ErrorCode::Conflict => "CONFLICT",
            ErrorCode::NotFound => "NOT_FOUND",
            ErrorCode::SequenceConflict => "SEQUENCE_CONFLICT",
            ErrorCode::StreamClosed => "STREAM_CLOSED",
            ErrorCode::InvalidOffset => "INVALID_OFFSET",
            ErrorCode::UnexpectedStatus => "UNEXPECTED_STATUS",
            ErrorCode::ParseError => "PARSE_ERROR",
            ErrorCode::InternalError => "INTERNAL_ERROR",
            ErrorCode::NotSupported => "NOT_SUPPORTED",
            ErrorCode::InvalidArgument => "INVALID_ARGUMENT",
        },
        message: message.into(),
    }
}

fn error_to_output(command_type: &str, path: Option<&str>, error: &Error) -> ErrorResult {
    match error {
        Error::Http(http) => {
            let message = contextualize_message(http.message.clone(), path);
            let lower_message = message.to_ascii_lowercase();
            let error_code = match http.kind {
                ErrorKind::NotFound => ErrorCode::NotFound,
                ErrorKind::Conflict
                    if http.producer_expected_seq.is_some()
                        || http.producer_received_seq.is_some() =>
                {
                    ErrorCode::SequenceConflict
                }
                ErrorKind::Conflict
                    if lower_message.contains("stream-seq")
                        || lower_message.contains("sequence") =>
                {
                    ErrorCode::SequenceConflict
                }
                ErrorKind::Conflict if lower_message.contains("closed") => ErrorCode::StreamClosed,
                ErrorKind::Conflict => ErrorCode::Conflict,
                ErrorKind::StreamClosed => ErrorCode::StreamClosed,
                ErrorKind::InvalidOffset => ErrorCode::InvalidOffset,
                _ => ErrorCode::UnexpectedStatus,
            };
            error_output(
                command_type,
                Some(http.status.as_u16()),
                error_code,
                message,
            )
        }
        other => error_output(
            command_type,
            None,
            match other.kind() {
                ErrorKind::InvalidArgument => ErrorCode::InvalidArgument,
                ErrorKind::Parse => ErrorCode::ParseError,
                ErrorKind::Network => ErrorCode::NetworkError,
                ErrorKind::Timeout => ErrorCode::Timeout,
                _ => ErrorCode::InternalError,
            },
            contextualize_message(other.to_string(), path),
        ),
    }
}

fn contextualize_message(message: String, path: Option<&str>) -> String {
    match path {
        Some(path) if !message.contains(path) => format!("{message} [{path}]"),
        _ => message,
    }
}

fn read_error_output(
    path: &str,
    live: LiveMode,
    offset: Option<&str>,
    error: &Error,
) -> ErrorResult {
    if live == LiveMode::Sse
        && matches!(error, Error::Http(http) if matches!(http.kind, ErrorKind::InvalidOffset))
        && offset.is_none_or(|value| value == "now")
    {
        return error_output(
            "read",
            match error {
                Error::Http(http) => Some(http.status.as_u16()),
                _ => None,
            },
            ErrorCode::ParseError,
            contextualize_message(error.to_string(), Some(path)),
        );
    }

    error_to_output("read", Some(path), error)
}

fn validate_target(target: ValidateTarget) -> Result<(), String> {
    match target {
        ValidateTarget::RetryOptions {
            max_retries,
            initial_delay_ms,
            max_delay_ms,
            multiplier,
        } => {
            if max_retries.is_some_and(|value| value < 0) {
                return Err("maxRetries must not be negative".to_string());
            }
            if initial_delay_ms.is_some_and(|value| value <= 0) {
                return Err("initialDelayMs must be greater than zero".to_string());
            }
            if let (Some(initial), Some(max)) = (initial_delay_ms, max_delay_ms)
                && max < initial
            {
                return Err(
                    "maxDelayMs must be greater than or equal to initialDelayMs".to_string(),
                );
            }
            if multiplier.is_some_and(|value| value < 1.0) {
                return Err("multiplier must be at least 1.0".to_string());
            }
            Ok(())
        }
        ValidateTarget::IdempotentProducer {
            producer_id,
            epoch,
            max_batch_bytes,
            max_batch_items,
        } => {
            if max_batch_bytes.is_some_and(|value| value < 0) {
                return Err("maxBatchBytes must not be negative".to_string());
            }
            if max_batch_items.is_some_and(|value| value < 0) {
                return Err("maxBatchItems must not be negative".to_string());
            }

            durable_streams_client::IdempotentProducerConfig {
                producer_id: producer_id.unwrap_or_else(|| "test-producer".to_string()),
                epoch: epoch.unwrap_or(0),
                auto_claim: false,
                max_batch_bytes: usize::try_from(max_batch_bytes.unwrap_or(1024 * 1024_i64))
                    .expect("max_batch_bytes must fit in usize"),
                max_batch_items: max_batch_items
                    .map(|value| usize::try_from(value).expect("max_batch_items must fit in usize")),
            }
            .validate()
            .map_err(|error| error.to_string())
        }
    }
}

fn resolve_dynamic(
    state: &mut AdapterState,
    explicit_headers: Option<HashMap<String, String>>,
) -> (
    HashMap<String, String>,
    HashMap<String, String>,
    HashMap<String, String>,
) {
    let mut headers_sent = HashMap::new();
    let mut params_sent = HashMap::new();
    let mut headers = explicit_headers.unwrap_or_default();

    for (name, dynamic) in &mut state.dynamic_headers {
        let value = next_dynamic_value(dynamic);
        headers_sent.insert(name.clone(), value.clone());
        headers.entry(name.clone()).or_insert(value);
    }
    for (name, dynamic) in &mut state.dynamic_params {
        params_sent.insert(name.clone(), next_dynamic_value(dynamic));
    }

    (headers_sent, params_sent, headers)
}

fn next_dynamic_value(value: &mut DynamicValue) -> String {
    match value.kind.as_str() {
        "counter" => {
            value.counter += 1;
            value.counter.to_string()
        }
        "timestamp" => {
            value.counter += 1;
            format!("{}", chrono::Utc::now().timestamp_millis())
        }
        "token" => value.token.clone().unwrap_or_default(),
        _ => String::new(),
    }
}

fn get_or_create_producer(
    client: Client,
    state: &mut AdapterState,
    path: &str,
    producer_id: &str,
    epoch: i64,
    auto_claim: bool,
    headers: HashMap<String, String>,
) -> Result<Arc<IdempotentProducer>, Error> {
    let key = producer_key(path, producer_id, epoch);
    if let Some(producer) = state.producers.get(&key) {
        return Ok(producer.clone());
    }

    let producer = create_producer(
        client,
        path,
        producer_id,
        epoch,
        auto_claim,
        headers,
        state.content_types.get(path).cloned(),
    )?;
    state.producers.insert(key, producer.clone());
    Ok(producer)
}

fn create_producer(
    client: Client,
    path: &str,
    producer_id: &str,
    epoch: i64,
    auto_claim: bool,
    headers: HashMap<String, String>,
    content_type: Option<String>,
) -> Result<Arc<IdempotentProducer>, Error> {
    let content_type = content_type.unwrap_or_else(|| "application/octet-stream".to_string());
    let producer = Arc::new(IdempotentProducer::new(
        client,
        path.to_string(),
        content_type,
        RequestOptions {
            headers,
            query: HashMap::new(),
        },
        IdempotentProducerConfig {
            producer_id: producer_id.to_string(),
            epoch,
            auto_claim,
            max_batch_bytes: 1024 * 1024,
            max_batch_items: None,
        },
    )?);
    Ok(producer)
}

fn producer_key(path: &str, producer_id: &str, epoch: i64) -> String {
    format!("{path}|{producer_id}|{epoch}")
}
