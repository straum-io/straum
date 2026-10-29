//! Writes the seed corpora (`straum_fuzz::seeds`) to `fuzz/corpus/<target>/`.
//!
//! cargo run --release --manifest-path fuzz/Cargo.toml --bin gen_corpus [<dir>]

use std::path::{Path, PathBuf};

fn main() {
    let out = std::env::args_os().nth(1).map_or_else(
        || Path::new(env!("CARGO_MANIFEST_DIR")).join("corpus"),
        PathBuf::from,
    );
    for (target, name, bytes) in straum_fuzz::seeds() {
        let dir = out.join(target);
        std::fs::create_dir_all(&dir).expect("create corpus dir");
        std::fs::write(dir.join(name), bytes).expect("write seed");
    }
    println!("seed corpora in {}", out.display());
}
