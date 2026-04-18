#![allow(dead_code)]
// Shared across many independent integration-test crates; each crate only uses
// a subset of helpers, so items appear unused when compiled per-test target.

mod http;
mod storage;

#[allow(unused_imports)]
pub use http::{
    HttpTestBackend, read_problem, spawn_test_server, spawn_test_server_acid,
    spawn_test_server_for_backend, spawn_test_server_with_config, spawn_test_server_with_limits,
    spawn_test_server_with_readyz, spawn_test_server_with_shutdown, spawn_test_server_with_storage,
    spawn_test_server_with_timeout, test_client, test_client_with_timeout, unique_stream_name,
};
#[allow(unused_imports)]
pub use storage::{
    StorageTestBackend, TestStorage, TestStorageHandle, create_test_storage,
    create_test_storage_with_limits,
};

/// Expand a block of tests into one `mod` per backend mapping.
///
/// This supports small backend subsets that do not use `StorageTestBackend`
/// directly but still benefit from per-backend test enumeration.
#[macro_export]
macro_rules! backend_tests {
    (
        type $backend_ty:ty;
        $($name:ident => $backend:path),+ $(,)?;
        $($body:tt)*
    ) => {
        $crate::backend_tests! {
            @expand
            [$backend_ty]
            [$($body)*]
            $($name => $backend),+
        }
    };
    (
        @expand
        [$backend_ty:ty]
        [$($body:tt)*]
        $name:ident => $backend:path $(, $rest_name:ident => $rest_backend:path)*
    ) => {
        mod $name {
            #[allow(unused_imports)]
            use super::*;
            #[allow(dead_code)]
            const BACKEND: $backend_ty = $backend;
            $($body)*
        }
        $crate::backend_tests! {
            @expand
            [$backend_ty]
            [$($body)*]
            $($rest_name => $rest_backend),*
        }
    };
    (
        @expand
        [$backend_ty:ty]
        [$($body:tt)*]
    ) => {
    };
}

/// Expand a block of `#[test]` functions into one `mod` per storage backend.
///
/// Each generated module defines a `const BACKEND: StorageTestBackend = ...`
/// identifying the backend under test, so the body can call
/// `create_test_storage(BACKEND)` etc. without a closure parameter.
///
/// `cargo test` then reports each `(test, backend)` pair as its own case
/// (e.g. `memory::my_test`, `acid::my_test`), which isolates failures and
/// drops the manual `"backend={}"` assertion suffixes.
#[macro_export]
macro_rules! storage_backend_tests {
    ($($body:tt)*) => {
        mod memory {
            #[allow(unused_imports)]
            use super::*;
            #[allow(dead_code)]
            const BACKEND: $crate::common::StorageTestBackend =
                $crate::common::StorageTestBackend::Memory;
            $($body)*
        }
        mod file_durable {
            #[allow(unused_imports)]
            use super::*;
            #[allow(dead_code)]
            const BACKEND: $crate::common::StorageTestBackend =
                $crate::common::StorageTestBackend::FileDurable;
            $($body)*
        }
        mod acid {
            #[allow(unused_imports)]
            use super::*;
            #[allow(dead_code)]
            const BACKEND: $crate::common::StorageTestBackend =
                $crate::common::StorageTestBackend::Acid;
            $($body)*
        }
        mod acid_in_memory {
            #[allow(unused_imports)]
            use super::*;
            #[allow(dead_code)]
            const BACKEND: $crate::common::StorageTestBackend =
                $crate::common::StorageTestBackend::AcidInMemory;
            $($body)*
        }
    };
}

/// Expand a block of `#[tokio::test]` functions into one `mod` per HTTP
/// backend (the subset of storage backends that participate in HTTP parity).
///
/// Each generated module defines a `const BACKEND: HttpTestBackend = ...`
/// that the body can feed to `spawn_test_server_for_backend(BACKEND)`.
#[macro_export]
macro_rules! http_backend_tests {
    ($($body:tt)*) => {
        mod memory {
            #[allow(unused_imports)]
            use super::*;
            #[allow(dead_code)]
            const BACKEND: $crate::common::HttpTestBackend =
                $crate::common::HttpTestBackend::Memory;
            $($body)*
        }
        mod acid {
            #[allow(unused_imports)]
            use super::*;
            #[allow(dead_code)]
            const BACKEND: $crate::common::HttpTestBackend =
                $crate::common::HttpTestBackend::Acid;
            $($body)*
        }
    };
}
