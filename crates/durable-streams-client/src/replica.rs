//! Read-side replication backed by the local JSONL journal.

use crate::client::Client;
use crate::error::Error;
use crate::journal::{JournalDirection, JournalRecord, JournalStreamIdentity, JsonJournal};
use crate::model::{LiveMode, ReadPayload, ReadRequest};
use serde_json::Value;
use std::path::Path;

/// Result of one replication step against the server.
///
/// This reports only what was durably appended to the local journal during the
/// current step, not how many messages may have been visible remotely.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadReplicaResult {
    /// Number of newly appended journal records.
    pub appended: usize,
    /// Journal size after this replication step.
    pub total: usize,
    /// Most recent persisted server offset.
    pub next_offset: Option<String>,
    /// Whether the server reported the reader is up to date.
    pub up_to_date: bool,
    /// Whether the stream is closed.
    pub stream_closed: bool,
}

/// Read-side replication session for one JSON stream.
///
/// The session treats the local journal as a durable cache. On startup it
/// replays the journal, derives the last persisted server offset, and uses that
/// offset for the next read automatically.
pub struct ReadReplica {
    client: Client,
    journal: JsonJournal,
}

impl ReadReplica {
    /// Open a local journal and bind it to a client-backed replication session.
    ///
    /// # Errors
    ///
    /// Returns an error if the journal cannot be opened or replayed.
    pub fn open(
        client: Client,
        journal_path: impl AsRef<Path>,
        stream: JournalStreamIdentity,
    ) -> Result<Self, Error> {
        let journal = JsonJournal::open(journal_path, stream)?;
        Ok(Self { client, journal })
    }

    /// Borrow the underlying journal.
    #[must_use]
    pub fn journal(&self) -> &JsonJournal {
        &self.journal
    }

    /// Borrow the replicated JSON values in journal order.
    #[must_use]
    pub fn values(&self) -> impl ExactSizeIterator<Item = &Value> + '_ {
        self.journal.values()
    }

    /// Return the last persisted server offset, if any.
    #[must_use]
    pub fn resume_offset(&self) -> Option<&str> {
        self.journal.resume_offset()
    }

    /// Replicate one read step from the server into the journal.
    ///
    /// The passed request must not set `offset`; the replica derives it from
    /// the journal. SSE reads are rejected in v1 because the replication API is
    /// defined around discrete persisted catch-up steps.
    ///
    /// # Errors
    ///
    /// Returns an error if the request specifies an explicit offset or SSE
    /// live mode, the server read fails, or journaling the response fails.
    pub async fn replicate(
        &mut self,
        mut request: ReadRequest,
    ) -> Result<ReadReplicaResult, Error> {
        if request.offset.is_some() {
            return Err(Error::invalid_argument(
                "read replica request offsets are derived from the journal",
            ));
        }

        request.offset = self.journal.resume_offset().map(ToOwned::to_owned);
        if matches!(request.live, LiveMode::Sse) {
            return Err(Error::invalid_argument(
                "read replica currently supports catch_up, long_poll, and auto reads",
            ));
        }

        let response = self
            .client
            .read_raw(self.journal.stream().path.as_str(), &request)
            .await?;
        let next_offset = response.next_offset.clone();
        let up_to_date = response.up_to_date;
        let stream_closed = response.stream_closed;
        let appended_records = self.journal.append_values(
            JournalDirection::Inbound,
            response_json_values(response.payload)?,
            Some(next_offset.clone()),
            None,
        )?;

        Ok(ReadReplicaResult {
            appended: appended_records.len(),
            total: self.journal.records().len(),
            next_offset: self.journal.resume_offset().map(ToOwned::to_owned),
            up_to_date,
            stream_closed,
        })
    }

    /// Return all committed journal records.
    #[must_use]
    pub fn records(&self) -> &[JournalRecord] {
        self.journal.records()
    }
}

fn response_json_values(payload: Option<ReadPayload>) -> Result<Vec<Value>, Error> {
    match payload {
        Some(ReadPayload::Json(values)) => Ok(values),
        Some(ReadPayload::Bytes(_)) => Err(Error::parse(
            "read replica only supports application/json payloads",
        )),
        None => Ok(Vec::new()),
    }
}
