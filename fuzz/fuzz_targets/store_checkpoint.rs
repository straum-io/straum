//! Checkpoint recovery: the base store without its checkpoint files, the
//! manifests, tables, runs and quarantine directories from the input, then
//! open.
#![no_main]

use std::sync::OnceLock;

fn base() -> &'static [(String, Vec<u8>)] {
    static BASE: OnceLock<Vec<(String, Vec<u8>)>> = OnceLock::new();
    BASE.get_or_init(|| straum_fuzz::checkpoint_base(straum_fuzz::base_store()))
}

libfuzzer_sys::fuzz_target!(|data: &[u8]| straum_fuzz::store_open(base(), data));
