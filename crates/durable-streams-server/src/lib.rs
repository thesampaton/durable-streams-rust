#![doc = include_str!("../README.md")]
//!
//! # Library Entry Points
//!
//! Most embedders only need:
//!
//! - [`Config`] and [`ConfigLoadOptions`] to load server configuration
//! - [`build_router`] to mount the Durable Streams HTTP API into an axum app
//! - [`Storage`] plus one of [`InMemoryStorage`], [`FileStorage`], or [`AcidStorage`]
//!   to choose a persistence backend
//!
//! The lower-level [`protocol`] module exposes types that are useful in tests,
//! storage implementations, and conformance-oriented integrations.

pub mod config;
mod handlers;
mod middleware;
pub mod protocol;
pub mod router;
pub mod storage;

pub use config::{Config, ConfigLoadOptions, StorageMode};
pub use router::{ShutdownToken, build_router, build_router_with_ready};
pub use storage::{
    Storage,
    acid::AcidStorage,
    file::FileStorage,
    memory::InMemoryStorage,
};
