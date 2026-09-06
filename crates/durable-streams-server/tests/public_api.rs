//! Integration coverage for public api.

use public_api::Builder as PublicApiBuilder;
use rustdoc_json::Builder as RustdocJsonBuilder;
use std::error::Error;
use std::path::{Path, PathBuf};
use std::process::Command;

const SNAPSHOT_PATH: &str = "tests/snapshots/public-api.txt";

#[test]
#[ignore = "requires nightly rustdoc JSON; run before release"]
fn server_public_api_matches_snapshot() -> Result<(), Box<dyn Error>> {
    assert_running_on_nightly()?;

    let manifest_path = Path::new(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml");
    let target_dir = tempfile::tempdir()?;
    let rustdoc_json = RustdocJsonBuilder::default()
        .manifest_path(&manifest_path)
        .target_dir(target_dir.path())
        .build()?;

    let public_api = PublicApiBuilder::from_rustdoc_json(rustdoc_json)
        .omit_blanket_impls(true)
        .omit_auto_trait_impls(true)
        .omit_auto_derived_impls(true)
        .build()?;

    public_api.assert_eq_or_update(snapshot_path());

    Ok(())
}

fn assert_running_on_nightly() -> Result<(), Box<dyn Error>> {
    let output = Command::new("rustc").arg("--version").output()?;
    let version = String::from_utf8(output.stdout)?;
    if version.contains("nightly") {
        return Ok(());
    }

    Err(
        "public API snapshot test requires a nightly toolchain; run `cargo +nightly test -p durable-streams-server --test public_api -- --ignored`"
            .to_string()
            .into(),
    )
}

fn snapshot_path() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join(SNAPSHOT_PATH)
}
