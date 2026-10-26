//! Runners of `conformance/scenarios/` (against a live broker) and
//! `conformance/client-scenarios/` (straum-client against the scripted
//! fake server). The formats are specified in `conformance/README.md`.

pub mod broker;
pub mod client;
pub mod fake;
pub mod matching;

use std::path::{Path, PathBuf};

use straum_proto::json::{self, Value};

/// Every `*.json` file of `dir`, sorted, parsed.
pub fn load_dir(dir: &Path) -> Result<Vec<(PathBuf, Value)>, String> {
    let mut paths: Vec<PathBuf> = std::fs::read_dir(dir)
        .map_err(|e| format!("{}: {e}", dir.display()))?
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().is_some_and(|x| x == "json"))
        .collect();
    paths.sort();
    paths
        .into_iter()
        .map(|p| {
            let text = std::fs::read_to_string(&p).map_err(|e| format!("{}: {e}", p.display()))?;
            let v = json::parse(&text).map_err(|e| format!("{}: {e}", p.display()))?;
            Ok((p, v))
        })
        .collect()
}

/// `conformance/` of this repository.
pub fn conformance_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../conformance")
}
