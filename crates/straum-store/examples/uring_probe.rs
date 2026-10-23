//! Environment check: open a file, positional write, fdatasync, read
//! back, through compio. Prints the driver compio selected. On Linux only the
//! io-uring driver is compiled in, so a blocked io_uring fails `Runtime::new`.

use compio::BufResult;
use compio::io::{AsyncReadAtExt, AsyncWriteAtExt};
use compio::runtime::Runtime;

fn main() -> std::io::Result<()> {
    let dir = std::env::args()
        .nth(1)
        .map_or_else(std::env::temp_dir, std::path::PathBuf::from);
    let path = dir.join("straum-store-uring-probe.bin");
    let rt = Runtime::new()?;
    println!("driver: {:?}", rt.driver_type());
    rt.block_on(async {
        let mut file = compio::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .await?;
        let payload = b"straum-store probe".to_vec();
        let BufResult(res, payload) = file.write_all_at(payload, 0).await;
        res?;
        file.sync_data().await?;
        let BufResult(res, back) = file
            .read_exact_at(Vec::with_capacity(payload.len()), 0)
            .await;
        res?;
        assert_eq!(back, payload, "read-back mismatch");
        println!("write + fdatasync + read-back: ok ({} bytes)", back.len());
        compio::fs::remove_file(&path).await
    })
}
