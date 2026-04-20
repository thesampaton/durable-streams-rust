use super::{FileStorage, StreamEntry, StreamMeta};
use crate::protocol::error::{Error, Result};
use base64::Engine;
use std::fs::{self, File, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};

/// Re-issue a syscall when interrupted by a signal (`EINTR`).
///
/// Rust's standard library does not retry `fsync`, `rename`, or `mkdir` on
/// `EINTR`, so this wrapper handles the standard POSIX retry contract.
/// Unlike the previous `retry_on_eintr`, this never sleeps - `EINTR`
/// retries are immediate by convention. Other transient errors (`WouldBlock`,
/// `TimedOut`) propagate immediately and become 503 via error classification.
pub(super) fn retry_on_eintr<T>(
    mut op: impl FnMut() -> std::result::Result<T, io::Error>,
) -> std::result::Result<T, io::Error> {
    loop {
        match op() {
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            result => return result,
        }
    }
}

impl FileStorage {
    fn stream_dir_component(name: &str) -> Result<String> {
        let encoded = base64::prelude::BASE64_URL_SAFE_NO_PAD.encode(name.as_bytes());
        if !is_valid_stream_dir_component(&encoded) {
            return Err(Error::Storage(
                "encoded stream directory contains invalid characters".to_string(),
            ));
        }
        Ok(encoded)
    }

    /// Map a stream name to a directory path inside `root_dir`.
    ///
    /// Stream directories are always the base64url-encoded stream name as a
    /// single direct child under the configured storage root.
    pub(super) fn stream_dir_for_name(&self, name: &str) -> Result<PathBuf> {
        Ok(self.root_dir.join(Self::stream_dir_component(name)?))
    }

    pub(super) fn validate_stream_dir(&self, dir: &Path) -> Result<()> {
        if dir.parent() != Some(self.root_dir.as_path()) {
            return Err(Error::Storage(format!(
                "stream directory must be a direct child of the storage root: {}",
                dir.display()
            )));
        }

        let Some(component) = dir.file_name().and_then(|name| name.to_str()) else {
            return Err(Error::Storage(format!(
                "stream directory is not valid UTF-8: {}",
                dir.display()
            )));
        };
        if !is_valid_stream_dir_component(component) {
            return Err(Error::Storage(format!(
                "stream directory contains invalid characters: {}",
                dir.display()
            )));
        }

        match fs::symlink_metadata(dir) {
            Ok(metadata) => {
                if metadata.file_type().is_symlink() {
                    return Err(Error::Storage(format!(
                        "stream directory cannot be a symlink: {}",
                        dir.display()
                    )));
                }
                if !metadata.is_dir() {
                    return Err(Error::Storage(format!(
                        "stream path is not a directory: {}",
                        dir.display()
                    )));
                }

                let canonical = fs::canonicalize(dir).map_err(|e| {
                    Error::Storage(format!(
                        "failed to canonicalize stream directory {}: {e}",
                        dir.display()
                    ))
                })?;
                if !canonical.starts_with(&self.root_dir_canonical) {
                    return Err(Error::Storage(format!(
                        "stream directory resolves outside storage root: {}",
                        dir.display()
                    )));
                }
            }
            Err(err) if err.kind() == io::ErrorKind::NotFound => {}
            Err(err) => {
                return Err(Error::Storage(format!(
                    "failed to stat stream directory {}: {err}",
                    dir.display()
                )));
            }
        }

        Ok(())
    }

    pub(super) fn data_log_path(dir: &Path) -> PathBuf {
        dir.join("data.log")
    }

    pub(super) fn meta_path(dir: &Path) -> PathBuf {
        dir.join("meta.json")
    }

    pub(super) fn write_metadata_for(&self, name: &str, entry: &StreamEntry) -> Result<()> {
        self.validate_stream_dir(&entry.dir)?;
        let meta = StreamMeta {
            name: name.to_string(),
            config: entry.config.clone(),
            closed: entry.closed,
            created_at: entry.created_at,
            updated_at: entry.updated_at,
            last_seq: entry.last_seq.clone(),
            producers: entry.producers.clone(),
            fork_info: entry.fork_info.clone(),
            ref_count: entry.ref_count,
            state: entry.state,
        };

        let meta_path = Self::meta_path(&entry.dir);
        let tmp_path = entry.dir.join("meta.json.tmp");
        let payload = serde_json::to_vec(&meta)
            .map_err(|e| Error::Storage(format!("failed to serialize stream metadata: {e}")))?;

        retry_on_eintr(|| fs::write(&tmp_path, payload.as_slice())).map_err(|e| {
            Error::classify_io_failure(
                "file",
                "write stream metadata temp file",
                format!(
                    "failed to write metadata temp file {}: {e}",
                    tmp_path.display()
                ),
                &e,
            )
        })?;

        retry_on_eintr(|| fs::rename(&tmp_path, &meta_path)).map_err(|e| {
            Error::classify_io_failure(
                "file",
                "replace stream metadata",
                format!(
                    "failed to atomically replace metadata {}: {e}",
                    meta_path.display()
                ),
                &e,
            )
        })?;

        Ok(())
    }

    pub(super) fn open_stream_file(&self, dir: &Path) -> Result<File> {
        self.validate_stream_dir(dir)?;
        let path = Self::data_log_path(dir);
        retry_on_eintr(|| {
            OpenOptions::new()
                .create(true)
                .append(true)
                .read(true)
                .open(&path)
        })
        .map_err(|e| {
            Error::classify_io_failure(
                "file",
                "open stream log",
                format!("failed to open stream log {}: {e}", path.display()),
                &e,
            )
        })
    }

    pub(super) fn remove_stream_dir(&self, dir: &Path) -> Result<()> {
        self.validate_stream_dir(dir)?;
        retry_on_eintr(|| fs::remove_dir_all(dir)).map_err(|e| {
            Error::classify_io_failure(
                "file",
                "remove stream directory",
                format!("failed to remove stream directory {}: {e}", dir.display()),
                &e,
            )
        })
    }
}

fn is_valid_stream_dir_component(component: &str) -> bool {
    !component.is_empty()
        && component
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
}
