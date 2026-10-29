#![no_main]

libfuzzer_sys::fuzz_target!(|data: &[u8]| straum_fuzz::proto_json(data));
