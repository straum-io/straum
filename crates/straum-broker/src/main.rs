//! `straum-broker --config <file> [--<key> <value> ...]`: settings in
//! crates/straum-broker/README.md. SIGTERM or SIGINT stops it: no new
//! connections or requests, queued groups committed, waiting fetches
//! answered, the store closed.

use std::process::ExitCode;

use straum_broker::{Config, serve};

/// SIGTERM on Linux and macOS.
const SIGTERM: i32 = 15;

fn main() -> ExitCode {
    let config = match Config::from_args(std::env::args().skip(1)) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("straum-broker: {e}");
            return ExitCode::from(2);
        }
    };
    let rt = match compio::runtime::Runtime::new() {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("straum-broker: runtime: {e}");
            return ExitCode::FAILURE;
        }
    };
    let res = rt.block_on(serve(config, |_, stop| {
        for signal in [Signal::Term, Signal::Int] {
            let stop = stop.clone();
            compio::runtime::spawn(async move {
                let got = match signal {
                    Signal::Term => compio::signal::unix::signal(SIGTERM).await,
                    Signal::Int => compio::signal::ctrl_c().await,
                };
                if got.is_ok() {
                    // The stop handle connects to the listener to wake the
                    // accept loop: off this thread, which runs that loop.
                    std::thread::spawn(move || stop.stop());
                }
            })
            .detach();
        }
    }));
    match res {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("straum-broker: {e}");
            ExitCode::FAILURE
        }
    }
}

#[derive(Clone, Copy)]
enum Signal {
    Term,
    Int,
}
