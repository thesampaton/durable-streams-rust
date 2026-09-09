//! Undo journal for log/metadata commits. Recovery runs before indexing logs.

use super::{FileStorage, StreamEntry, StreamMeta};
use crate::protocol::error::{Error, Result};
use crate::storage::shared::release_bytes;
use serde::{Deserialize, Serialize};
use std::fs::{self, File};
use std::path::Path;

#[derive(Serialize, Deserialize)]
struct Undo {
    log_len: u64,
    metadata: Vec<u8>,
    #[serde(default)]
    restore_log: bool,
}

#[allow(
    clippy::needless_pass_by_value,
    reason = "map_err transfers ownership of the I/O error"
)]
fn io_error(error: std::io::Error) -> Error {
    Error::classify_io_failure("file", "append transaction", error.to_string(), &error)
}

impl FileStorage {
    /// The journal is synced before either file changes; its removal commits both.
    pub(super) fn append_transaction<T>(
        &self,
        stream: &mut StreamEntry,
        restore_log: bool,
        operation: impl FnOnce(&mut StreamEntry) -> Result<T>,
    ) -> Result<T> {
        self.validate_stream_dir(&stream.dir)?;
        if stream.unavailable {
            return Err(Error::storage_unavailable(
                "file",
                "append",
                "previous rollback failed; reopen storage to recover",
            ));
        }
        let journal = stream.dir.join("append.undo");
        let undo = Undo {
            log_len: stream.file_len,
            restore_log,
            metadata: fs::read(Self::meta_path(&stream.dir)).map_err(io_error)?,
        };
        let backup = stream.dir.join("replace.backup");
        if restore_log {
            fs::copy(Self::data_log_path(&stream.dir), &backup).map_err(io_error)?;
            File::open(&backup)
                .and_then(|file| file.sync_all())
                .map_err(io_error)?;
        }
        let bytes = serde_json::to_vec(&undo).map_err(|e| Error::Storage(e.to_string()))?;
        let temporary = stream.dir.join("append.undo.tmp");
        fs::write(&temporary, bytes).map_err(io_error)?;
        File::open(&temporary)
            .and_then(|file| file.sync_all())
            .map_err(io_error)?;
        fs::rename(&temporary, &journal).map_err(io_error)?;
        File::open(&stream.dir)
            .and_then(|dir| dir.sync_all())
            .map_err(io_error)?;

        let old_index_len = stream.index.len();
        let old_index = restore_log.then(|| stream.index.clone());
        let old_total = stream.total_bytes;
        let old_read_seq = stream.next_read_seq;
        let old_byte_offset = stream.next_byte_offset;
        let result = operation(stream).and_then(|value| {
            stream.file.sync_data().map_err(io_error)?;
            File::open(Self::meta_path(&stream.dir))
                .and_then(|file| file.sync_all())
                .map_err(io_error)?;
            File::open(&stream.dir)
                .and_then(|dir| dir.sync_all())
                .map_err(io_error)?;
            fs::remove_file(&journal).map_err(io_error)?;
            // Once removal succeeds, the operation has committed. A directory
            // sync failure cannot safely be converted into an undo request.
            File::open(&stream.dir)
                .and_then(|dir| dir.sync_all())
                .map_err(io_error)?;
            Ok(value)
        });
        if let Err(error) = result {
            if !journal.exists() {
                stream.unavailable = true;
                return Err(error);
            }
            if let Err(recovery) = self.recover_append(&stream.dir) {
                stream.unavailable = true;
                return Err(Error::storage_unavailable(
                    "file",
                    "rollback append",
                    format!("{error}; rollback failed: {recovery}"),
                ));
            }
            let meta: StreamMeta = serde_json::from_slice(&undo.metadata)
                .map_err(|e| Error::Storage(e.to_string()))?;
            if restore_log {
                release_bytes(&self.total_bytes, stream.total_bytes);
            } else {
                release_bytes(
                    &self.total_bytes,
                    stream.total_bytes.saturating_sub(old_total),
                );
            }
            stream.config = meta.config;
            stream.closed = meta.closed;
            stream.updated_at = meta.updated_at;
            stream.last_seq = meta.last_seq;
            stream.producers = meta.producers;
            if let Some(index) = old_index {
                stream.index = index;
            } else {
                stream.index.truncate(old_index_len);
            }
            stream.total_bytes = old_total;
            stream.next_read_seq = old_read_seq;
            stream.next_byte_offset = old_byte_offset;
            stream.file_len = undo.log_len;
            return Err(error);
        }
        if restore_log {
            let _ = fs::remove_file(backup);
        }
        result
    }

    pub(super) fn recover_append(&self, dir: &Path) -> Result<()> {
        self.validate_stream_dir(dir)?;
        let journal = dir.join("append.undo");
        let bytes = match fs::read(&journal) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(io_error(error)),
        };
        let undo: Undo =
            serde_json::from_slice(&bytes).map_err(|e| Error::Storage(e.to_string()))?;
        // Validate the complete journal before changing either file.
        let _: StreamMeta =
            serde_json::from_slice(&undo.metadata).map_err(|e| Error::Storage(e.to_string()))?;
        if undo.restore_log {
            fs::copy(dir.join("replace.backup"), Self::data_log_path(dir)).map_err(io_error)?;
        }
        let file = self.open_stream_file(dir)?;
        if file.metadata().map_err(io_error)?.len() < undo.log_len {
            return Err(Error::Storage(
                "undo journal refers beyond the stream log".into(),
            ));
        }
        file.set_len(undo.log_len).map_err(io_error)?;
        file.sync_all().map_err(io_error)?;
        let temporary = dir.join("meta.restore.tmp");
        fs::write(&temporary, &undo.metadata).map_err(io_error)?;
        File::open(&temporary)
            .and_then(|file| file.sync_all())
            .map_err(io_error)?;
        fs::rename(&temporary, Self::meta_path(dir)).map_err(io_error)?;
        File::open(dir)
            .and_then(|dir| dir.sync_all())
            .map_err(io_error)?;
        fs::remove_file(journal).map_err(io_error)?;
        File::open(dir)
            .and_then(|dir| dir.sync_all())
            .map_err(io_error)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::offset::Offset;
    use crate::storage::{Storage, StreamOptions};
    use bytes::Bytes;
    use std::io::Write;

    #[test]
    fn ordinary_append_preserves_update_timestamp_on_reopen() {
        let root = tempfile::tempdir().unwrap();
        let storage = FileStorage::new(root.path().to_owned(), 1024, 1024).unwrap();
        storage
            .create_stream("s", StreamOptions::new("text/plain"))
            .unwrap();
        storage
            .append("s", Bytes::from_static(b"message"), "text/plain")
            .unwrap();
        let timestamp = storage.head("s").unwrap().updated_at;
        assert!(timestamp.is_some());
        drop(storage);
        let reopened = FileStorage::new(root.path().to_owned(), 1024, 1024).unwrap();
        assert_eq!(reopened.head("s").unwrap().updated_at, timestamp);
    }

    #[test]
    fn failed_initial_metadata_write_releases_payload_capacity() {
        let root = tempfile::tempdir().unwrap();
        let storage = FileStorage::new(root.path().to_owned(), 8, 8).unwrap();
        let dir = storage.stream_dir_for_name("s").unwrap();
        fs::create_dir_all(dir.join("meta.json.tmp")).unwrap();
        assert!(
            storage
                .create_stream_with_data(
                    "s",
                    StreamOptions::new("text/plain"),
                    vec![Bytes::from_static(b"12345678")],
                    false
                )
                .is_err()
        );
        assert_eq!(storage.total_bytes(), 0);
        assert!(!storage.exists("s").unwrap());
        storage
            .create_stream_with_data(
                "s",
                StreamOptions::new("text/plain"),
                vec![Bytes::from_static(b"12345678")],
                false,
            )
            .unwrap();
    }

    #[test]
    fn metadata_failure_rolls_back_final_body_and_sequence_on_disk() {
        let root = tempfile::tempdir().unwrap();
        let storage = FileStorage::new(root.path().to_owned(), 1024, 1024).unwrap();
        storage
            .create_stream("s", StreamOptions::new("text/plain"))
            .unwrap();
        storage
            .append_batch(
                "s",
                vec![Bytes::from_static(b"old")],
                "text/plain",
                Some("1"),
                false,
            )
            .unwrap();
        let dir = storage.stream_dir_for_name("s").unwrap();
        fs::create_dir(dir.join("meta.json.tmp")).unwrap();
        assert!(
            storage
                .append_batch(
                    "s",
                    vec![Bytes::from_static(b"new")],
                    "text/plain",
                    Some("2"),
                    true
                )
                .is_err()
        );
        let before = storage.head("s").unwrap();
        assert!(!before.closed);
        assert_eq!(before.total_bytes, 3);
        fs::remove_dir(dir.join("meta.json.tmp")).unwrap();
        drop(storage);
        let storage = FileStorage::new(root.path().to_owned(), 1024, 1024).unwrap();
        assert_eq!(
            storage.read("s", &Offset::start()).unwrap().messages,
            vec![Bytes::from_static(b"old")]
        );
        storage
            .append_batch(
                "s",
                vec![Bytes::from_static(b"new")],
                "text/plain",
                Some("2"),
                true,
            )
            .unwrap();
    }

    #[test]
    fn reopen_undoes_a_crash_between_log_and_metadata_commit() {
        let root = tempfile::tempdir().unwrap();
        let storage = FileStorage::new(root.path().to_owned(), 1024, 1024).unwrap();
        storage
            .create_stream("s", StreamOptions::new("text/plain"))
            .unwrap();
        storage
            .append("s", Bytes::from_static(b"old"), "text/plain")
            .map(|result| result.start_offset)
            .unwrap();
        let dir = storage.stream_dir_for_name("s").unwrap();
        let undo = Undo {
            log_len: 7,
            metadata: fs::read(FileStorage::meta_path(&dir)).unwrap(),
            restore_log: false,
        };
        fs::write(dir.join("append.undo"), serde_json::to_vec(&undo).unwrap()).unwrap();
        let mut file = storage.open_stream_file(&dir).unwrap();
        file.write_all(&[3, 0, 0, 0, b'n', b'e', b'w']).unwrap();
        let mut meta: StreamMeta = serde_json::from_slice(&undo.metadata).unwrap();
        meta.closed = true;
        fs::write(
            FileStorage::meta_path(&dir),
            serde_json::to_vec(&meta).unwrap(),
        )
        .unwrap();
        drop(storage);
        let storage = FileStorage::new(root.path().to_owned(), 1024, 1024).unwrap();
        let read = storage.read("s", &Offset::start()).unwrap();
        assert_eq!(read.messages, vec![Bytes::from_static(b"old")]);
        assert!(!read.closed);
    }

    #[test]
    fn failed_replacement_restores_original_log_and_accounting() {
        let root = tempfile::tempdir().unwrap();
        let storage = FileStorage::new(root.path().to_owned(), 1024, 1024).unwrap();
        storage
            .create_stream("s", StreamOptions::new("text/plain"))
            .unwrap();
        storage
            .append("s", Bytes::from_static(b"original"), "text/plain")
            .map(|result| result.start_offset)
            .unwrap();
        let dir = storage.stream_dir_for_name("s").unwrap();
        fs::create_dir(dir.join("meta.json.tmp")).unwrap();
        assert!(
            storage
                .replace_stream(
                    "s",
                    StreamOptions::new("application/octet-stream"),
                    vec![Bytes::from_static(b"new")],
                    true
                )
                .is_err()
        );
        assert_eq!(storage.total_bytes(), 8);
        let read = storage.read("s", &Offset::start()).unwrap();
        assert_eq!(read.messages, vec![Bytes::from_static(b"original")]);
        assert!(!read.closed);
        drop(storage);
        let restored = FileStorage::new(root.path().to_owned(), 1024, 1024).unwrap();
        assert_eq!(
            restored.read("s", &Offset::start()).unwrap().messages,
            read.messages
        );
    }
}
