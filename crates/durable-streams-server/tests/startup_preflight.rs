//! Integration coverage for startup preflight.

#![allow(
    clippy::unwrap_used,
    reason = "test setup and assertions fail the test on error"
)]

use durable_streams_server::config::Config;
use durable_streams_server::startup::{
    StartupError, StartupErrorKind, StartupPhase, TlsFileStatus, check_tls_file,
    preflight_tls_files,
};
use std::fs;
use tempfile::TempDir;

// ── Missing file ───────────────────────────────────────────────────

#[test]
fn preflight_fails_for_missing_cert_file() {
    let mut config = Config::default();
    config.transport.tls.cert_path = Some("/tmp/ds-startup-test-nonexistent-cert.pem".to_string());
    config.transport.tls.key_path = Some("/tmp/ds-startup-test-nonexistent-key.pem".to_string());

    let err = preflight_tls_files(&config).unwrap_err();
    assert_eq!(err.phase, StartupPhase::CheckTlsFiles);
    assert!(
        matches!(&err.kind, StartupErrorKind::TlsFileNotFound { path } if path.contains("cert")),
        "expected TlsFileNotFound for cert, got: {err}"
    );
}

#[test]
fn preflight_fails_for_missing_key_file() {
    let dir = TempDir::new().unwrap();
    let cert = dir.path().join("cert.pem");
    fs::write(&cert, b"placeholder").unwrap();

    let mut config = Config::default();
    config.transport.tls.cert_path = Some(cert.to_str().unwrap().to_string());
    config.transport.tls.key_path = Some("/tmp/ds-startup-test-nonexistent-key.pem".to_string());

    let err = preflight_tls_files(&config).unwrap_err();
    assert_eq!(err.phase, StartupPhase::CheckTlsFiles);
    assert!(
        matches!(&err.kind, StartupErrorKind::TlsFileNotFound { path } if path.contains("key")),
        "expected TlsFileNotFound for key, got: {err}"
    );
}

#[test]
fn preflight_fails_for_missing_client_ca_file() {
    let dir = TempDir::new().unwrap();
    let cert = dir.path().join("cert.pem");
    let key = dir.path().join("key.pem");
    fs::write(&cert, b"placeholder").unwrap();
    fs::write(&key, b"placeholder").unwrap();

    let mut config = Config::default();
    config.transport.tls.cert_path = Some(cert.to_str().unwrap().to_string());
    config.transport.tls.key_path = Some(key.to_str().unwrap().to_string());
    config.transport.tls.client_ca_path =
        Some("/tmp/ds-startup-test-nonexistent-ca.pem".to_string());

    let err = preflight_tls_files(&config).unwrap_err();
    assert_eq!(err.phase, StartupPhase::CheckTlsFiles);
    assert!(
        matches!(&err.kind, StartupErrorKind::TlsFileNotFound { path } if path.contains("ca")),
        "expected TlsFileNotFound for ca, got: {err}"
    );
}

// ── Not a regular file ─────────────────────────────────────────────

#[test]
fn preflight_fails_when_cert_path_is_a_directory() {
    let dir = TempDir::new().unwrap();
    let mut config = Config::default();
    config.transport.tls.cert_path = Some(dir.path().to_str().unwrap().to_string());

    let err = preflight_tls_files(&config).unwrap_err();
    assert_eq!(err.phase, StartupPhase::CheckTlsFiles);
    assert!(
        matches!(&err.kind, StartupErrorKind::TlsFileNotRegular { .. }),
        "expected TlsFileNotRegular, got: {err}"
    );
}

// ── Happy path ─────────────────────────────────────────────────────

#[test]
fn preflight_passes_when_no_tls_paths_configured() {
    let config = Config::default();
    assert!(preflight_tls_files(&config).is_ok());
}

#[test]
fn preflight_passes_with_valid_files() {
    let dir = TempDir::new().unwrap();
    let cert = dir.path().join("cert.pem");
    let key = dir.path().join("key.pem");
    fs::write(&cert, b"placeholder-cert").unwrap();
    fs::write(&key, b"placeholder-key").unwrap();

    let mut config = Config::default();
    config.transport.tls.cert_path = Some(cert.to_str().unwrap().to_string());
    config.transport.tls.key_path = Some(key.to_str().unwrap().to_string());

    assert!(preflight_tls_files(&config).is_ok());
}

// ── check_tls_file unit-level coverage ─────────────────────────────

#[test]
fn check_tls_file_returns_ok_for_regular_file() {
    let dir = TempDir::new().unwrap();
    let file = dir.path().join("test.pem");
    fs::write(&file, b"data").unwrap();
    assert_eq!(check_tls_file(file.to_str().unwrap()), TlsFileStatus::Ok);
}

#[test]
fn check_tls_file_returns_not_found_for_absent_path() {
    assert_eq!(
        check_tls_file("/tmp/ds-startup-test-absent-file.pem"),
        TlsFileStatus::NotFound
    );
}

#[test]
fn check_tls_file_returns_not_regular_for_directory() {
    let dir = TempDir::new().unwrap();
    assert_eq!(
        check_tls_file(dir.path().to_str().unwrap()),
        TlsFileStatus::NotRegular
    );
}

// ── Error display ──────────────────────────────────────────────────

#[test]
fn startup_error_display_includes_phase_and_detail() {
    let err = StartupError::new(
        StartupPhase::CheckTlsFiles,
        StartupErrorKind::TlsFileNotFound {
            path: "/etc/ssl/missing.pem".to_string(),
        },
    );
    let msg = err.to_string();
    assert!(msg.contains("check_tls_files"), "phase missing: {msg}");
    assert!(msg.contains("missing.pem"), "path missing: {msg}");
}

#[test]
fn startup_error_display_for_tls_not_readable() {
    let err = StartupError::new(
        StartupPhase::CheckTlsFiles,
        StartupErrorKind::TlsFileNotReadable {
            path: "/etc/ssl/locked.pem".to_string(),
            reason: "permission denied".to_string(),
        },
    );
    let msg = err.to_string();
    assert!(msg.contains("check_tls_files"), "phase missing: {msg}");
    assert!(msg.contains("not readable"), "kind missing: {msg}");
    assert!(msg.contains("permission denied"), "reason missing: {msg}");
}

#[test]
fn startup_error_display_for_config_validation() {
    let err = StartupError::config_validation(
        durable_streams_server::config::ConfigValidationError::MaxMemoryBytesTooSmall,
    );
    let msg = err.to_string();
    assert!(msg.contains("validate_config"), "phase missing: {msg}");
    assert!(msg.contains("max_memory_bytes"), "detail missing: {msg}");
}

#[test]
fn startup_error_display_for_tls_context() {
    let err = StartupError::tls_context("invalid PEM data");
    let msg = err.to_string();
    assert!(msg.contains("build_tls_context"), "phase missing: {msg}");
    assert!(msg.contains("invalid PEM data"), "detail missing: {msg}");
}

#[test]
fn startup_error_display_for_runtime() {
    let err = StartupError::runtime("address already in use");
    let msg = err.to_string();
    assert!(msg.contains("start_server"), "phase missing: {msg}");
    assert!(
        msg.contains("address already in use"),
        "detail missing: {msg}"
    );
}
