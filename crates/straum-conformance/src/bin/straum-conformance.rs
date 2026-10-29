//! `straum-conformance broker <straum-server binary> [scenario dir]` runs the
//! broker scenarios against fresh broker processes;
//! `straum-conformance client [scenario dir]` runs the client scenarios with
//! straum-client against the fake server. Exits 0 if every scenario passes.

use std::path::PathBuf;
use std::process::ExitCode;

use straum_conformance::{broker, client, conformance_dir, load_dir};

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let (kind, bin, dir) = match args.as_slice() {
        [k, b] if k == "broker" => (
            "broker",
            Some(PathBuf::from(b)),
            conformance_dir().join("scenarios"),
        ),
        [k, b, d] if k == "broker" => ("broker", Some(PathBuf::from(b)), PathBuf::from(d)),
        [k] if k == "client" => ("client", None, conformance_dir().join("client-scenarios")),
        [k, d] if k == "client" => ("client", None, PathBuf::from(d)),
        _ => {
            eprintln!("usage: straum-conformance broker <straum-server> [dir] | client [dir]");
            return ExitCode::from(2);
        }
    };
    let scenarios = match load_dir(&dir) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("straum-conformance: {e}");
            return ExitCode::FAILURE;
        }
    };
    let mut failed = 0;
    for (path, s) in &scenarios {
        let res = match &bin {
            Some(b) if kind == "broker" => broker::run(b, s),
            _ => client::run(s),
        };
        let name = path
            .file_name()
            .map_or_else(String::new, |n| n.to_string_lossy().into_owned());
        match res {
            Ok(()) => println!("ok    {name}"),
            Err(e) => {
                failed += 1;
                println!("FAIL  {name}: {e}");
            }
        }
    }
    println!("{} scenarios, {failed} failed", scenarios.len());
    if failed == 0 {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}
