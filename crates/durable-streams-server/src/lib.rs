#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        reason = "test setup and assertions fail the test on error"
    )
)]
#![doc = include_str!("../CRATE_DOCS.md")]

pub mod config;
mod handlers;
mod middleware;
pub mod protocol;
pub mod router;
pub mod startup;
pub mod storage;
pub mod streams;
mod subscriptions;
pub mod transfer;

pub use config::{Config, ConfigLoadOptions, DeploymentProfile, StorageMode};
pub use router::{DEFAULT_STREAM_BASE_PATH, RouterOptions, RunningServer, Server, ServerError};
pub use storage::{Storage, acid::AcidStorage, file::FileStorage, memory::InMemoryStorage};

pub use streams::StreamService;
