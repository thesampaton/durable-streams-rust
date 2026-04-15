#![doc = include_str!("../CRATE_DOCS.md")]

pub mod config;
mod handlers;
mod middleware;
pub mod protocol;
pub mod router;
pub mod startup;
pub mod storage;
pub mod transfer;

pub use config::{Config, ConfigLoadOptions, DeploymentProfile, StorageMode};
pub use router::{DEFAULT_STREAM_BASE_PATH, ShutdownToken, build_router, build_router_with_ready};
pub use storage::{Storage, acid::AcidStorage, file::FileStorage, memory::InMemoryStorage};
