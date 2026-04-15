use super::{
    AcidShard, AcidStorage, Database, DatabaseError, Duration, Error, HASH_POLICY, InMemoryBackend,
    LAYOUT_FORMAT_VERSION, LayoutManifest, MESSAGES, Path, PathBuf, RedbStorageError, Result,
    STARTUP_RETRY_BACKOFF_MS, STREAMS, StoredStreamMeta, StreamState, fs,
};
use crate::storage::{fork, is_stream_expired};
use redb::{ReadableDatabase, ReadableTable};

impl AcidStorage {
    pub(super) fn create_file_shards(
        root_dir: &Path,
        shard_count: usize,
    ) -> Result<Vec<AcidShard>> {
        let acid_dir = Self::acid_dir(root_dir);
        fs::create_dir_all(&acid_dir).map_err(|e| {
            Self::storage_err(
                format!(
                    "failed to create acid storage directory {}",
                    acid_dir.display()
                ),
                e,
            )
        })?;

        Self::load_or_create_layout(&acid_dir, shard_count)?;

        let mut shards = Vec::with_capacity(shard_count);
        for idx in 0..shard_count {
            let shard_path = acid_dir.join(format!("shard_{idx:02x}.redb"));
            let db = Self::open_shard_database(&shard_path)?;
            Self::ensure_schema(&db)?;
            shards.push(AcidShard { db });
        }

        Ok(shards)
    }

    pub(super) fn create_in_memory_shards(shard_count: usize) -> Result<Vec<AcidShard>> {
        let mut shards = Vec::with_capacity(shard_count);
        for _ in 0..shard_count {
            let db = Database::builder()
                .create_with_backend(InMemoryBackend::new())
                .map_err(|e| Self::storage_err("failed to create in-memory shard database", e))?;
            Self::ensure_schema(&db)?;
            shards.push(AcidShard { db });
        }
        Ok(shards)
    }

    /// Open a shard database with retry. This is startup-only code — the
    /// `thread::sleep` backoff is acceptable here since it runs once per shard
    /// during initialization, never on the request path.
    fn open_shard_database(shard_path: &Path) -> Result<Database> {
        let context = format!("failed to open shard database {}", shard_path.display());
        let mut delays = STARTUP_RETRY_BACKOFF_MS.into_iter();

        loop {
            match Database::builder().create(shard_path) {
                Ok(db) => return Ok(db),
                Err(err) if Self::is_retryable_database_open(&err) => {
                    if let Some(delay_ms) = delays.next() {
                        std::thread::sleep(Duration::from_millis(delay_ms));
                        continue;
                    }
                    return Err(Self::storage_err(context, err));
                }
                Err(err) => return Err(Self::storage_err(context, err)),
            }
        }
    }

    fn is_retryable_database_open(err: &DatabaseError) -> bool {
        match err {
            DatabaseError::DatabaseAlreadyOpen => true,
            DatabaseError::Storage(RedbStorageError::Io(io_err)) => {
                Error::is_retryable_io_error(io_err)
            }
            _ => false,
        }
    }

    fn acid_dir(root_dir: &Path) -> PathBuf {
        root_dir.join("acid")
    }

    fn layout_path(acid_dir: &Path) -> PathBuf {
        acid_dir.join("layout.json")
    }

    fn load_or_create_layout(acid_dir: &Path, shard_count: usize) -> Result<()> {
        let layout_path = Self::layout_path(acid_dir);
        if layout_path.exists() {
            let payload = fs::read(&layout_path).map_err(|e| {
                Self::storage_err(
                    format!("failed to read acid layout file {}", layout_path.display()),
                    e,
                )
            })?;
            let manifest: LayoutManifest = serde_json::from_slice(&payload).map_err(|e| {
                Self::storage_err(
                    format!("failed to parse acid layout file {}", layout_path.display()),
                    e,
                )
            })?;

            if manifest.format_version != LAYOUT_FORMAT_VERSION {
                return Err(Error::Storage(format!(
                    "acid layout mismatch: format_version={}, expected={}",
                    manifest.format_version, LAYOUT_FORMAT_VERSION
                )));
            }
            if manifest.shard_count != shard_count {
                return Err(Error::Storage(format!(
                    "acid layout mismatch: shard_count={}, expected={shard_count}",
                    manifest.shard_count
                )));
            }
            if manifest.hash_policy != HASH_POLICY {
                return Err(Error::Storage(format!(
                    "acid layout mismatch: hash_policy='{}', expected='{}'",
                    manifest.hash_policy, HASH_POLICY
                )));
            }
            return Ok(());
        }

        let manifest = LayoutManifest {
            format_version: LAYOUT_FORMAT_VERSION,
            shard_count,
            hash_policy: HASH_POLICY.to_string(),
        };
        let payload = serde_json::to_vec_pretty(&manifest)
            .map_err(|e| Self::storage_err("failed to serialize acid layout manifest", e))?;

        let tmp_path = acid_dir.join("layout.json.tmp");
        fs::write(&tmp_path, payload).map_err(|e| {
            Self::storage_err(
                format!("failed to write temp layout file {}", tmp_path.display()),
                e,
            )
        })?;
        fs::rename(&tmp_path, &layout_path).map_err(|e| {
            Self::storage_err(
                format!("failed to write layout file {}", layout_path.display()),
                e,
            )
        })?;

        Ok(())
    }

    pub(super) fn ensure_schema(db: &Database) -> Result<()> {
        let txn = Self::begin_write_txn(db)?;
        let streams = txn
            .open_table(STREAMS)
            .map_err(|e| Self::storage_err("failed to initialize streams table", e))?;
        let messages = txn
            .open_table(MESSAGES)
            .map_err(|e| Self::storage_err("failed to initialize messages table", e))?;
        drop(messages);
        drop(streams);
        txn.commit()
            .map_err(|e| Self::storage_err("failed to commit schema initialization", e))?;
        Ok(())
    }

    pub(super) fn rebuild_state_from_disk(&self) -> Result<u64> {
        let mut total = 0_u64;
        for shard in &self.shards {
            total = total.saturating_add(self.rebuild_shard(shard)?);
        }
        Ok(total)
    }

    fn rebuild_shard(&self, shard: &AcidShard) -> Result<u64> {
        let read_txn = shard
            .db
            .begin_read()
            .map_err(|e| Self::storage_err("failed to begin read transaction", e))?;
        let streams = read_txn
            .open_table(STREAMS)
            .map_err(|e| Self::storage_err("failed to open streams table", e))?;

        let mut live_bytes = 0_u64;
        let mut expired: Vec<(String, StoredStreamMeta)> = Vec::new();
        let mut entries: Vec<(String, StoredStreamMeta)> = Vec::new();

        {
            let iter = streams
                .iter()
                .map_err(|e| Self::storage_err("failed to iterate stream metadata", e))?;
            for item in iter {
                let (key, value) =
                    item.map_err(|e| Self::storage_err("failed to read stream metadata", e))?;
                let stream_name = key.value().to_string();
                let meta: StoredStreamMeta = serde_json::from_slice(value.value())
                    .map_err(|e| Self::storage_err("failed to parse stream metadata", e))?;
                entries.push((stream_name, meta));
            }

            let shard_names = entries
                .iter()
                .map(|(stream_name, _)| stream_name.as_str())
                .collect::<std::collections::HashSet<_>>();

            for (stream_name, meta) in &entries {
                if let Some(fork_info) = &meta.fork_info
                    && !shard_names.contains(fork_info.source_name.as_str())
                {
                    return Err(Error::Storage(format!(
                        "legacy cross-shard fork lineage requires migration: {stream_name} -> {}",
                        fork_info.source_name
                    )));
                }
            }

            for (stream_name, meta) in entries {
                if is_stream_expired(&meta.config) {
                    expired.push((stream_name, meta));
                } else {
                    live_bytes = live_bytes.saturating_add(meta.total_bytes);
                }
            }
        }

        drop(streams);
        drop(read_txn);

        if expired.is_empty() {
            return Ok(live_bytes);
        }

        let txn = Self::begin_write_txn(&shard.db)?;
        let mut streams = txn
            .open_table(STREAMS)
            .map_err(|e| Self::storage_err("failed to open streams table", e))?;
        let mut messages = txn
            .open_table(MESSAGES)
            .map_err(|e| Self::storage_err("failed to open messages table", e))?;

        let mut parents_to_cascade: Vec<String> = Vec::new();
        for (name, mut meta) in expired {
            match fork::evaluate_expired_cleanup(meta.ref_count) {
                fork::DeleteDisposition::Tombstone => {
                    meta.state = StreamState::Tombstone;
                    Self::write_stream_meta(&mut streams, &name, &meta)?;
                    live_bytes = live_bytes.saturating_add(meta.total_bytes);
                }
                fork::DeleteDisposition::HardDelete => {
                    Self::delete_stream_messages(&mut messages, &name)?;
                    streams
                        .remove(name.as_str())
                        .map_err(|e| Self::storage_err("failed to remove expired stream", e))?;
                    if let Some(fork_info) = meta.fork_info {
                        parents_to_cascade.push(fork_info.source_name);
                    }
                    self.drop_notifier(&name);
                }
            }
        }

        drop(messages);
        drop(streams);
        txn.commit()
            .map_err(|e| Self::storage_err("failed to commit startup cleanup", e))?;

        for parent in parents_to_cascade {
            self.cascade_delete_acid(&parent)?;
        }

        Ok(live_bytes)
    }
}
