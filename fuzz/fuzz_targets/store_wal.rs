//! WAL recovery: the base store's data segments, the WAL files from the
//! input (and any other file it holds), then open.
#![no_main]

use std::sync::OnceLock;

fn base() -> &'static [(String, Vec<u8>)] {
    static BASE: OnceLock<Vec<(String, Vec<u8>)>> = OnceLock::new();
    BASE.get_or_init(|| straum_fuzz::wal_base(straum_fuzz::base_store()))
}

libfuzzer_sys::fuzz_target!(|data: &[u8]| straum_fuzz::store_open(base(), data));
