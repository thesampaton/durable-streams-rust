//! Import validates documents before mutation and reports per-stream progress.
#![allow(
    clippy::unwrap_used,
    reason = "test setup and assertions fail the test on error"
)]
mod common;
use bytes::Bytes;
use common::create_test_storage_with_limits;
use durable_streams_server::transfer::{
    TransferError,
    export::{ExportOptions, export_streams},
    import::{ConflictPolicy, ImportOptions, import_streams},
};
use durable_streams_server::{Storage, protocol::offset::Offset, storage::StreamOptions};

storage_backend_tests! {
    #[test]
    fn malformed_later_payload_preserves_every_original() {
        let handle = create_test_storage_with_limits(BACKEND, 1024, 128);
        let storage = &handle.storage;
        for name in ["a", "b"] {
            storage
                .create_stream_with_data(
                    name,
                    StreamOptions::new("text/plain"),
                    vec![Bytes::from_static(b"old")],
                    false,
                )
                .unwrap();
        }
        let mut exported = Vec::new();
        export_streams(
            storage,
            &ExportOptions {
                stream_names: vec![],
            },
            &mut exported,
        )
        .unwrap();
        let mut document: serde_json::Value = serde_json::from_slice(&exported).unwrap();
        document["streams"][0]["messages"][0]["data_base64"] = "bmV3".into();
        document["streams"][1]["messages"][0]["data_base64"] = "!invalid!".into();
        let data = serde_json::to_vec(&document).unwrap();
        assert!(matches!(
            import_streams(
                storage,
                data.as_slice(),
                &ImportOptions {
                    conflict_policy: ConflictPolicy::Replace
                }
            ),
            Err(TransferError::Base64(_))
        ));
        for name in ["a", "b"] {
            assert_eq!(
                storage.read(name, &Offset::start()).unwrap().messages,
                vec![Bytes::from_static(b"old")]
            );
        }
    }

    #[test]
    fn later_capacity_failure_reports_progress_and_preserves_failing_stream() {
        let handle = create_test_storage_with_limits(BACKEND, 1024, 4);
        let storage = &handle.storage;
        for name in ["a", "b"] {
            storage
                .create_stream_with_data(
                    name,
                    StreamOptions::new("text/plain"),
                    vec![Bytes::from_static(b"old")],
                    false,
                )
                .unwrap();
        }
        let mut exported = Vec::new();
        export_streams(
            storage,
            &ExportOptions {
                stream_names: vec![],
            },
            &mut exported,
        )
        .unwrap();
        let mut document: serde_json::Value = serde_json::from_slice(&exported).unwrap();
        document["streams"][0]["messages"][0]["data_base64"] = "bmV3".into();
        document["streams"][1]["messages"][0]["data_base64"] = "bG9uZ2Vy".into();
        let data = serde_json::to_vec(&document).unwrap();
        match import_streams(
            storage,
            data.as_slice(),
            &ImportOptions {
                conflict_policy: ConflictPolicy::Replace,
            },
        ) {
            Err(TransferError::PartialImport {
                stream, completed, ..
            }) => {
                assert_eq!(stream, "b");
                assert_eq!(completed.streams_imported, 1);
                assert_eq!(completed.messages_imported, 1);
            }
            _ => panic!("expected partial import failure"),
        }
        assert_eq!(
            storage.read("a", &Offset::start()).unwrap().messages,
            vec![Bytes::from_static(b"new")]
        );
        assert_eq!(
            storage.read("b", &Offset::start()).unwrap().messages,
            vec![Bytes::from_static(b"old")]
        );
    }

    #[test]
    fn replacement_does_not_change_inherited_bytes() {
        let handle = create_test_storage_with_limits(BACKEND, 1024, 128);
        let storage = &handle.storage;
        storage
            .create_stream_with_data(
                "source",
                StreamOptions::new("text/plain"),
                vec![Bytes::from_static(b"old")],
                false,
            )
            .unwrap();
        storage
            .create_fork("fork", "source", None, StreamOptions::new("text/plain"))
            .unwrap();
        assert!(
            storage
                .replace_stream(
                    "source",
                    StreamOptions::new("text/plain"),
                    vec![Bytes::from_static(b"new")],
                    false
                )
                .is_err()
        );
        assert_eq!(
            storage.read("fork", &Offset::start()).unwrap().messages,
            vec![Bytes::from_static(b"old")]
        );
    }
}
