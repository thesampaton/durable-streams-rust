//! Integration coverage for public api.

use public_api::Builder as PublicApiBuilder;
use rustdoc_json::Builder as RustdocJsonBuilder;
use std::error::Error;
use std::path::{Path, PathBuf};

const TOOLCHAIN: &str = include_str!("public-api-toolchain.txt");
const SNAPSHOT_PATH: &str = "tests/snapshots/public-api.txt";

#[test]
#[ignore = "requires nightly rustdoc JSON; run via scripts/check-server-public-api.sh"]
fn server_public_api_matches_snapshot() -> Result<(), Box<dyn Error>> {
    let manifest_path = Path::new(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml");
    let target_dir = tempfile::tempdir()?;
    // Rustdoc type paths can change between compiler versions without API changes.
    let rustdoc_json = RustdocJsonBuilder::default()
        .toolchain(TOOLCHAIN.trim())
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

fn snapshot_path() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join(SNAPSHOT_PATH)
}
