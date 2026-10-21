//! Composition root: read the environment, start the node, run until a
//! signal says otherwise. One path since step 8 (D-111): a node of the
//! cluster path, experimental and not production-ready (D-102 A).

mod config;

use std::process::ExitCode;

use config::{Settings, Source};
use tracing_subscriber::EnvFilter;

fn main() -> ExitCode {
    if std::env::args().nth(1).as_deref() == Some("hash-password") {
        return hash_password();
    }
    let settings = match Settings::parse(std::env::vars()) {
        Ok(settings) => settings,
        Err(error) => {
            // Before tracing exists, and a misconfigured node must not start.
            eprintln!("configuration error: {error}");
            return ExitCode::FAILURE;
        }
    };

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new(settings.log_level.clone())),
        )
        .init();
    tracing::warn!(
        "EXPERIMENTAL cluster path (ADR-0008): not production-ready; flow control off; \
         no TLS, credentials only with STRAUM_AUTH_PASSWORD_FILE or STRAUM_AUTH_ACL_FILE; wills, persistent sessions and session expiry are refused; \
         timing defaults are placeholders, nothing is measured on Linux"
    );
    for (name, value, source) in &settings.effective {
        let source = match source {
            Source::Env => "env",
            Source::Default => "default",
        };
        tracing::info!(%name, %value, %source, "setting");
    }

    let tokio = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(settings.worker_threads)
        .build()
    {
        Ok(tokio) => tokio,
        Err(error) => {
            tracing::error!(%error, "could not start the async runtime");
            return ExitCode::FAILURE;
        }
    };
    tokio.block_on(run(settings.node))
}

/// `straum-server hash-password`: the `<hash>` of a `user:<hash>` line of
/// `STRAUM_AUTH_PASSWORD_FILE`, for the password read from stdin (D-158 A), never from the
/// arguments, where a process listing or a shell history would keep it.
fn hash_password() -> ExitCode {
    let mut password = String::new();
    if std::io::stdin().read_line(&mut password).is_err() {
        eprintln!("hash-password: stdin could not be read");
        return ExitCode::FAILURE;
    }
    let password = password.trim_end_matches(['\n', '\r']);
    if password.is_empty() {
        eprintln!("hash-password: give the password on stdin, one line");
        return ExitCode::FAILURE;
    }
    match auth::Authority::hash_password(password.as_bytes()) {
        Ok(hash) => {
            println!("{hash}");
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("hash-password: {error}");
            ExitCode::FAILURE
        }
    }
}

async fn run(config: node::NodeConfig) -> ExitCode {
    let node = match node::Node::start(config).await {
        Ok(node) => node,
        Err(error) => {
            tracing::error!(%error, "could not start the node");
            return ExitCode::FAILURE;
        }
    };
    tracing::info!(listen = %node.listen, mqtt = ?node.mqtt, metrics = ?node.metrics_listen, "node started");
    // A node whose loop has ended serves nothing: the process exits, so a
    // supervisor sees it and restarts it (D-152 A).
    let stopped = tokio::select! {
        () = serve_hand_offs(&node) => false,
        () = node.stopped() => true,
    };
    if stopped {
        tracing::error!("the node loop ended (the error above says why); the process exits");
        node.shutdown().await;
        return ExitCode::FAILURE;
    }
    tracing::info!("stopping");
    node.shutdown().await;
    ExitCode::SUCCESS
}

/// Until Ctrl-C or TERM: each SIGUSR1 asks the node to hand its slot over —
/// a protocol handoff, never an exit or a forced change of leader (D-105 A).
async fn serve_hand_offs(node: &node::Node) {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut user1 = match signal(SignalKind::user_defined1()) {
            Ok(user1) => user1,
            Err(error) => {
                tracing::warn!(%error, "no SIGUSR1 handler; no operator handoff");
                crate::signal().await;
                return;
            }
        };
        let stop = crate::signal();
        tokio::pin!(stop);
        loop {
            tokio::select! {
                () = &mut stop => return,
                _ = user1.recv() => match node.hand_off().await {
                    Ok(()) => tracing::info!("SIGUSR1: handing the slot over to the next owner"),
                    Err(error) => tracing::warn!(%error, "SIGUSR1: no handoff"),
                },
            }
        }
    }
    #[cfg(not(unix))]
    {
        let _ = node;
        crate::signal().await;
    }
}

/// Ctrl-C, or the TERM a supervisor sends.
async fn signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut terminate = match signal(SignalKind::terminate()) {
            Ok(terminate) => terminate,
            Err(error) => {
                tracing::warn!(%error, "no SIGTERM handler; Ctrl-C only");
                let _ = tokio::signal::ctrl_c().await;
                return;
            }
        };
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = terminate.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}
