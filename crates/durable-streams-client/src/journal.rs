//! File-backed JSONL journaling for local client persistence.

use crate::Error;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

const JOURNAL_RECORD_VERSION: u32 = 1;

/// Logical direction of one journaled record.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JournalDirection {
    /// Messages replicated from the server into a local cache.
    Inbound,
    /// Messages prepared for later producer recovery work.
    Outbound,
}

/// Stream identity bound to one journal file.
///
/// V1 journaling is intentionally restricted to JSON streams so replayed
/// records can be exposed back to callers as `serde_json::Value`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JournalStreamIdentity {
    /// Durable Streams path.
    pub path: String,
    /// Stream content type.
    pub content_type: String,
}

impl JournalStreamIdentity {
    /// Construct and validate a stream identity for JSON journaling.
    ///
    /// # Errors
    ///
    /// Returns an error if the path is empty or the content type is not
    /// `application/json`.
    pub fn new(path: impl Into<String>, content_type: impl Into<String>) -> Result<Self, Error> {
        let stream = Self {
            path: path.into(),
            content_type: content_type.into(),
        };
        stream.validate()?;
        Ok(stream)
    }

    fn validate(&self) -> Result<(), Error> {
        if self.path.trim().is_empty() {
            return Err(Error::invalid_argument(
                "journal stream path must not be empty",
            ));
        }
        if !self.content_type.starts_with("application/json") {
            return Err(Error::invalid_argument(format!(
                "journal only supports application/json content types, got '{}'",
                self.content_type
            )));
        }
        Ok(())
    }
}

/// Reserved producer metadata for later outbound persistence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProducerJournalProgress {
    /// Producer identifier.
    pub producer_id: String,
    /// Current claimed epoch.
    pub epoch: i64,
    /// Next sequence the producer plans to send.
    pub next_seq: i64,
    /// Latest server offset acknowledged for this producer state.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub acked_server_offset: Option<String>,
    /// Latest local journal position acknowledged by this producer state.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub acked_local_seq: Option<u64>,
}

/// One persisted JSONL journal record.
///
/// Each line carries one logical JSON value plus enough metadata to rebuild the
/// local cache and derive the last durable server offset after replay.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JournalRecord {
    /// Journal schema version.
    pub version: u32,
    /// Stream identity persisted in the file.
    pub stream: JournalStreamIdentity,
    /// Inbound or outbound direction.
    pub direction: JournalDirection,
    /// Monotonic local sequence number within the journal.
    pub local_seq: u64,
    /// Batch identifier used to drop only incomplete trailing writes.
    pub batch_seq: u64,
    /// Zero-based index of this record within its batch.
    pub batch_index: u32,
    /// Total number of records in this batch.
    pub batch_len: u32,
    /// Replicated JSON payload.
    pub payload: Value,
    /// Latest durable server next offset acknowledged for this batch.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_offset: Option<String>,
    /// Local observation time for this payload.
    pub observed_at: DateTime<Utc>,
    /// Local persistence time for this payload.
    pub persisted_at: DateTime<Utc>,
    /// Reserved producer metadata for forward compatibility.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub producer: Option<ProducerJournalProgress>,
}

/// Opened append-only JSONL journal.
///
/// Replay is fail-open for a trailing damaged suffix: an incomplete last line
/// or malformed trailing record is ignored without discarding the valid prefix.
/// Earlier structural inconsistencies, such as a stream identity mismatch or an
/// unsupported schema version, still fail the open operation.
pub struct JsonJournal {
    path: PathBuf,
    stream: JournalStreamIdentity,
    file: File,
    records: Vec<JournalRecord>,
    resume_offset: Option<String>,
    next_local_seq: u64,
    next_batch_seq: u64,
}

impl JsonJournal {
    /// Open or create a JSONL journal for one JSON stream.
    ///
    /// Existing files are replayed immediately to rebuild committed records and
    /// derive the last persisted server offset.
    ///
    /// # Errors
    ///
    /// Returns an error if the stream identity is invalid, the file cannot be
    /// opened, or replay encounters a version mismatch or stream identity
    /// conflict.
    pub fn open(path: impl AsRef<Path>, stream: JournalStreamIdentity) -> Result<Self, Error> {
        stream.validate()?;
        let path = path.as_ref().to_path_buf();
        let replay = replay_journal(&path, &stream)?;
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&path)?;

        // Truncate any trailing partial writes so new appends start from a
        // clean newline boundary. Without this, a partial record left by a
        // crash would be concatenated with the next append, corrupting both.
        file.set_len(replay.valid_bytes)?;

        Ok(Self {
            path,
            stream,
            file,
            records: replay.records,
            resume_offset: replay.resume_offset,
            next_local_seq: replay.next_local_seq,
            next_batch_seq: replay.next_batch_seq,
        })
    }

    /// Return the backing journal file path.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Return the bound stream identity.
    #[must_use]
    pub fn stream(&self) -> &JournalStreamIdentity {
        &self.stream
    }

    /// Return the last persisted server offset, if any.
    #[must_use]
    pub fn resume_offset(&self) -> Option<&str> {
        self.resume_offset.as_deref()
    }

    /// Borrow all committed journal records.
    #[must_use]
    pub fn records(&self) -> &[JournalRecord] {
        &self.records
    }

    /// Borrow only the persisted JSON payloads.
    ///
    /// Values are returned in committed local sequence order.
    #[must_use]
    pub fn values(&self) -> impl ExactSizeIterator<Item = &Value> + '_ {
        self.records.iter().map(|record| &record.payload)
    }

    /// Return whether the journal has no committed records.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    /// Append one logical batch of JSON values.
    ///
    /// All values in the batch share the same durable `next_offset`. Replay
    /// only advances the resume offset once the full batch has been observed,
    /// which avoids moving the cursor past partially written trailing data.
    ///
    /// # Errors
    ///
    /// Returns an error if the batch length exceeds `u32::MAX`, JSON
    /// serialization fails, or the underlying file write fails.
    ///
    /// # Panics
    ///
    /// Panics if the batch length exceeds `u64::MAX` (only possible on
    /// platforms where `usize` is wider than 64 bits).
    pub fn append_values(
        &mut self,
        direction: JournalDirection,
        values: impl IntoIterator<Item = Value>,
        next_offset: Option<String>,
        producer: Option<&ProducerJournalProgress>,
    ) -> Result<Vec<JournalRecord>, Error> {
        let values = values.into_iter().collect::<Vec<_>>();
        if values.is_empty() {
            // Still advance the resume offset so the caller doesn't re-request
            // the same position when the server returns an empty page.
            if let Some(offset) = next_offset {
                self.resume_offset = Some(offset);
            }
            return Ok(Vec::new());
        }

        let now = Utc::now();
        let batch_seq = self.next_batch_seq;
        let batch_len = u32::try_from(values.len())
            .map_err(|_| Error::invalid_argument("journal batch is too large"))?;
        let mut appended = Vec::with_capacity(values.len());

        for (index, payload) in values.into_iter().enumerate() {
            let record = JournalRecord {
                version: JOURNAL_RECORD_VERSION,
                stream: self.stream.clone(),
                direction,
                local_seq: self.next_local_seq + u64::try_from(index).expect("usize fits in u64"),
                batch_seq,
                batch_index: u32::try_from(index).expect("usize fits in u32"),
                batch_len,
                payload,
                next_offset: next_offset.clone(),
                observed_at: now,
                persisted_at: now,
                producer: producer.cloned(),
            };
            let line = serde_json::to_vec(&record)?;
            self.file.write_all(&line)?;
            self.file.write_all(b"\n")?;
            appended.push(record);
        }

        self.file.flush()?;
        self.file.sync_data()?;

        self.next_local_seq += u64::try_from(appended.len()).expect("appended count fits in u64");
        self.next_batch_seq += 1;
        if let Some(offset) = appended
            .last()
            .and_then(|record| record.next_offset.as_ref())
            .cloned()
        {
            self.resume_offset = Some(offset);
        }
        self.records.extend(appended.iter().cloned());
        Ok(appended)
    }
}

#[derive(Debug, Default)]
struct ReplayState {
    records: Vec<JournalRecord>,
    resume_offset: Option<String>,
    next_local_seq: u64,
    next_batch_seq: u64,
    /// Byte length of the validated file content. Trailing partial writes
    /// beyond this point should be truncated before appending new data.
    valid_bytes: u64,
}

fn replay_journal(path: &Path, stream: &JournalStreamIdentity) -> Result<ReplayState, Error> {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(ReplayState::default());
        }
        Err(error) => return Err(error.into()),
    };

    let complete_len = bytes
        .iter()
        .rposition(|byte| *byte == b'\n')
        .map_or(0, |index| index + 1);
    let complete = &bytes[..complete_len];

    let mut parsed = Vec::new();
    for raw_line in complete.split(|byte| *byte == b'\n') {
        let line = trim_ascii_whitespace(raw_line);
        if line.is_empty() {
            continue;
        }

        let Ok(record) = serde_json::from_slice::<JournalRecord>(line) else {
            break;
        };
        if record.version != JOURNAL_RECORD_VERSION {
            return Err(Error::parse(format!(
                "unsupported journal record version {}",
                record.version
            )));
        }
        if record.stream != *stream {
            return Err(Error::parse(format!(
                "journal stream mismatch: expected '{}' with content type '{}', found '{}' with '{}'",
                stream.path, stream.content_type, record.stream.path, record.stream.content_type
            )));
        }
        parsed.push(record);
    }

    let mut state = commit_valid_prefix(parsed);
    state.valid_bytes = u64::try_from(complete_len).unwrap_or(u64::MAX);
    Ok(state)
}

fn commit_valid_prefix(records: Vec<JournalRecord>) -> ReplayState {
    let mut state = ReplayState::default();
    let mut pending = Vec::new();
    let mut pending_batch_seq = 0_u64;
    let mut pending_batch_len = 0_u32;
    let mut pending_next_offset: Option<String> = None;

    for record in records {
        let expected_local_seq = state.records.len() as u64 + pending.len() as u64;
        if record.local_seq != expected_local_seq {
            break;
        }

        if pending.is_empty() {
            if record.batch_index != 0 {
                break;
            }
            pending_batch_seq = record.batch_seq;
            pending_batch_len = record.batch_len;
            pending_next_offset.clone_from(&record.next_offset);
        } else if record.batch_seq != pending_batch_seq {
            if pending.len() != usize::try_from(pending_batch_len).expect("u32 fits in usize") {
                break;
            }
            commit_batch(
                &mut state,
                &mut pending,
                pending_batch_seq,
                pending_next_offset.take(),
            );
            if record.batch_index != 0 {
                break;
            }
            pending_batch_seq = record.batch_seq;
            pending_batch_len = record.batch_len;
            pending_next_offset.clone_from(&record.next_offset);
        } else if record.batch_index != u32::try_from(pending.len()).expect("batch fits in u32")
            || record.batch_len != pending_batch_len
            || pending_next_offset != record.next_offset
        {
            break;
        }

        pending.push(record);
    }

    if !pending.is_empty()
        && pending.len() == usize::try_from(pending_batch_len).expect("u32 fits in usize")
    {
        commit_batch(
            &mut state,
            &mut pending,
            pending_batch_seq,
            pending_next_offset.take(),
        );
    }

    state
}

fn commit_batch(
    state: &mut ReplayState,
    pending: &mut Vec<JournalRecord>,
    batch_seq: u64,
    next_offset: Option<String>,
) {
    if let Some(offset) = next_offset {
        state.resume_offset = Some(offset);
    }
    state.next_batch_seq = batch_seq + 1;
    state.next_local_seq += u64::try_from(pending.len()).expect("pending fits in u64");
    state.records.append(pending);
}

fn trim_ascii_whitespace(bytes: &[u8]) -> &[u8] {
    let start = bytes
        .iter()
        .position(|byte| !byte.is_ascii_whitespace())
        .unwrap_or(bytes.len());
    let end = bytes
        .iter()
        .rposition(|byte| !byte.is_ascii_whitespace())
        .map_or(start, |index| index + 1);
    &bytes[start..end]
}

#[cfg(test)]
mod tests {
    use super::{JournalDirection, JournalStreamIdentity, JsonJournal, ProducerJournalProgress};
    use serde_json::json;
    use std::fs;
    use std::io::Write;
    use tempfile::tempdir;

    #[test]
    fn rebuilds_state_after_reopen() {
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("journal.jsonl");
        let stream = JournalStreamIdentity::new("/orders", "application/json").expect("stream");

        {
            let mut journal = JsonJournal::open(&path, stream.clone()).expect("open journal");
            journal
                .append_values(
                    JournalDirection::Inbound,
                    vec![json!({"id": 1}), json!({"id": 2})],
                    Some("2".to_string()),
                    None,
                )
                .expect("append");
        }

        let reopened = JsonJournal::open(&path, stream).expect("reopen journal");
        assert_eq!(reopened.resume_offset(), Some("2"));
        assert_eq!(
            reopened.values().cloned().collect::<Vec<_>>(),
            vec![json!({"id": 1}), json!({"id": 2})]
        );
    }

    #[test]
    fn ignores_trailing_partial_line() {
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("journal.jsonl");
        let stream = JournalStreamIdentity::new("/orders", "application/json").expect("stream");
        let mut journal = JsonJournal::open(&path, stream.clone()).expect("open journal");
        journal
            .append_values(
                JournalDirection::Inbound,
                vec![json!({"id": 1})],
                Some("1".to_string()),
                None,
            )
            .expect("append");
        fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .expect("open raw file")
            .write_all(br#"{"version":1,"stream":{"path":"/orders""#)
            .expect("write partial line");

        let reopened = JsonJournal::open(&path, stream).expect("reopen journal");
        assert_eq!(
            reopened.values().cloned().collect::<Vec<_>>(),
            vec![json!({"id": 1})]
        );
        assert_eq!(reopened.resume_offset(), Some("1"));
    }

    #[test]
    fn ignores_trailing_corrupt_suffix_after_valid_prefix() {
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("journal.jsonl");
        let stream = JournalStreamIdentity::new("/orders", "application/json").expect("stream");
        let mut journal = JsonJournal::open(&path, stream.clone()).expect("open journal");
        journal
            .append_values(
                JournalDirection::Inbound,
                vec![json!({"id": 1})],
                Some("1".to_string()),
                None,
            )
            .expect("append");
        fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .expect("open raw file")
            .write_all(b"not-json\n")
            .expect("write corrupt line");

        let reopened = JsonJournal::open(&path, stream).expect("reopen journal");
        assert_eq!(
            reopened.values().cloned().collect::<Vec<_>>(),
            vec![json!({"id": 1})]
        );
    }

    #[test]
    fn preserves_forward_compatible_producer_metadata() {
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("journal.jsonl");
        let stream = JournalStreamIdentity::new("/orders", "application/json").expect("stream");
        let mut journal = JsonJournal::open(&path, stream.clone()).expect("open journal");
        journal
            .append_values(
                JournalDirection::Outbound,
                vec![json!({"id": 7})],
                Some("9".to_string()),
                Some(&ProducerJournalProgress {
                    producer_id: "producer-a".to_string(),
                    epoch: 3,
                    next_seq: 11,
                    acked_server_offset: Some("9".to_string()),
                    acked_local_seq: Some(0),
                }),
            )
            .expect("append");

        let reopened = JsonJournal::open(&path, stream).expect("reopen journal");
        assert_eq!(
            reopened.records()[0].producer.as_ref().expect("producer"),
            &ProducerJournalProgress {
                producer_id: "producer-a".to_string(),
                epoch: 3,
                next_seq: 11,
                acked_server_offset: Some("9".to_string()),
                acked_local_seq: Some(0),
            }
        );
    }
}
