//! Integration coverage for transfer roundtrip.

#![allow(
    clippy::unwrap_used,
    reason = "test setup and assertions fail the test on error"
)]

use bytes::Bytes;
use durable_streams_server::{
    InMemoryStorage, Storage,
    protocol::offset::Offset,
    storage::StreamOptions,
    transfer::{
        export::{ExportOptions, export_streams},
        format::ExportDocument,
        import::{ConflictPolicy, ImportOptions, import_streams},
    },
};

#[test]
fn export_uses_canonical_offsets_for_linear_streams() {
    let storage = InMemoryStorage::new(1024 * 1024, 1024 * 1024);
    storage
        .create_stream("orders", StreamOptions::new("text/plain".to_string()))
        .unwrap();
    storage
        .append_batch(
            "orders",
            vec![Bytes::from("hi"), Bytes::from("there"), Bytes::from("!")],
            "text/plain",
            None,
            false,
        )
        .map(|result| result.next_offset)
        .unwrap();

    let mut output = Vec::new();
    export_streams(
        &storage,
        &ExportOptions {
            stream_names: Vec::new(),
        },
        &mut output,
    )
    .unwrap();

    let doc: ExportDocument = serde_json::from_slice(&output).unwrap();
    let offsets: Vec<_> = doc.streams[0]
        .messages
        .iter()
        .map(|message| message.offset.clone())
        .collect();

    assert_eq!(
        offsets,
        vec![
            Offset::new(0, 0).to_string(),
            Offset::new(1, 2).to_string(),
            Offset::new(2, 7).to_string(),
        ]
    );
}

#[test]
fn export_uses_canonical_offsets_for_forked_streams() {
    let storage = InMemoryStorage::new(1024 * 1024, 1024 * 1024);
    storage
        .create_stream("source", StreamOptions::new("text/plain".to_string()))
        .unwrap();
    storage
        .append_batch(
            "source",
            vec![Bytes::from("aa"), Bytes::from("bbb")],
            "text/plain",
            None,
            false,
        )
        .map(|result| result.next_offset)
        .unwrap();
    let fork_offset = Offset::new(1, 2);
    storage
        .create_fork(
            "fork",
            "source",
            Some(&fork_offset),
            StreamOptions::new("text/plain".to_string()),
        )
        .unwrap();
    storage
        .append("fork", Bytes::from("c"), "text/plain")
        .map(|result| result.start_offset)
        .unwrap();

    let mut output = Vec::new();
    export_streams(
        &storage,
        &ExportOptions {
            stream_names: vec!["fork".to_string()],
        },
        &mut output,
    )
    .unwrap();

    let doc: ExportDocument = serde_json::from_slice(&output).unwrap();
    let exported_stream = &doc.streams[0];
    let offsets: Vec<_> = exported_stream
        .messages
        .iter()
        .map(|message| message.offset.clone())
        .collect();

    assert_eq!(
        offsets,
        vec![Offset::new(0, 0).to_string(), Offset::new(1, 2).to_string()]
    );
}

#[test]
fn export_import_round_trip_restores_messages() {
    let source = InMemoryStorage::new(1024 * 1024, 1024 * 1024);
    source
        .create_stream("events", StreamOptions::new("text/plain".to_string()))
        .unwrap();
    source
        .append_batch(
            "events",
            vec![Bytes::from("one"), Bytes::from("two")],
            "text/plain",
            None,
            false,
        )
        .map(|result| result.next_offset)
        .unwrap();
    source.close_stream("events").unwrap();

    let mut output = Vec::new();
    export_streams(
        &source,
        &ExportOptions {
            stream_names: Vec::new(),
        },
        &mut output,
    )
    .unwrap();

    let target = InMemoryStorage::new(1024 * 1024, 1024 * 1024);
    let stats = import_streams(
        &target,
        output.as_slice(),
        &ImportOptions {
            conflict_policy: ConflictPolicy::Fail,
        },
    )
    .unwrap();

    assert_eq!(stats.streams_imported, 1);
    assert_eq!(stats.messages_imported, 2);

    let read = target.read("events", &Offset::start()).unwrap();
    assert_eq!(read.messages, vec![Bytes::from("one"), Bytes::from("two")]);
    assert!(read.closed);
}
