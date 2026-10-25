//! `straum-fake-server <client-scenario.json>`: plays the scenario's
//! `server` steps for a client under test. Prints
//! `straum-fake-server: listening on <host:port>` first; exits 0 if the
//! client did exactly what the script expects, 1 otherwise.

use std::io::Write;
use std::process::ExitCode;

use straum_conformance::fake::FakeServer;
use straum_proto::json::{self, Value};

fn main() -> ExitCode {
    let Some(path) = std::env::args().nth(1) else {
        eprintln!("usage: straum-fake-server <client-scenario.json>");
        return ExitCode::from(2);
    };
    let run = || -> Result<(), String> {
        let text = std::fs::read_to_string(&path).map_err(|e| format!("{path}: {e}"))?;
        let scenario = json::parse(&text)?;
        let steps = scenario
            .get("server")
            .and_then(Value::as_array)
            .ok_or("no server steps")?;
        let server = FakeServer::bind()?;
        println!("straum-fake-server: listening on {}", server.addr());
        let _ = std::io::stdout().flush();
        server.play(steps)
    };
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("straum-fake-server: {e}");
            ExitCode::FAILURE
        }
    }
}
