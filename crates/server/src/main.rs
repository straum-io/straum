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
         TLS and credentials only when configured (STRAUM_TLS_*, STRAUM_AUTH_*, STRAUM_CLUSTER_TLS_*); wills, persistent sessions and session expiry are refused; \
         timing defaults are placeholders, nothing is measured on Linux"
    );
    for (name, value, source) in &settings.effective {
        let source = match source {
            Source::Env => "env",
            Source::Default => "default",
        };
        tracing::info!(%name, %value, %source, "setting");
    }
    for warning in &settings.warnings {
        tracing::warn!("{warning}");
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
    let tls = settings.node.mqtt.as_ref().and_then(|m| m.tls.clone());
    let node_tls = settings.node.tls.clone();
    tokio.block_on(run(settings.node, settings.access, tls, node_tls))
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

async fn run(
    config: node::NodeConfig,
    access: Option<config::Access>,
    tls: Option<transport::Tls>,
    node_tls: Option<transport::NodeTls>,
) -> ExitCode {
    // Before the start, which can take a while: until a handler is in
    // place SIGHUP keeps its default action and would end the process, and
    // `systemctl reload` may send it at any time (D-166 A).
    let hangup = hangup();
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
        () = serve_signals(&node, hangup, access.as_ref(), tls.as_ref(), node_tls.as_ref()) => false,
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

#[cfg(unix)]
type Hangup = Option<tokio::signal::unix::Signal>;
#[cfg(not(unix))]
type Hangup = ();

/// The SIGHUP stream, or `None` with the real outcome logged: a SIGHUP
/// then ends the process.
fn hangup() -> Hangup {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        match signal(SignalKind::hangup()) {
            Ok(hangup) => Some(hangup),
            Err(error) => {
                tracing::warn!(%error, "no SIGHUP handler: no reload, and a SIGHUP ends the process");
                None
            }
        }
    }
}

/// Until Ctrl-C or TERM: each SIGUSR1 asks the node to hand its slot over,
/// a protocol handoff, never an exit or a forced change of leader (D-105 A);
/// each SIGHUP reloads the TLS and credential files (D-166 A).
async fn serve_signals(
    node: &node::Node,
    hangup: Hangup,
    access: Option<&config::Access>,
    tls: Option<&transport::Tls>,
    node_tls: Option<&transport::NodeTls>,
) {
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
        let mut hangup = hangup;
        let stop = crate::signal();
        tokio::pin!(stop);
        loop {
            tokio::select! {
                () = &mut stop => return,
                Some(()) = async {
                    match hangup.as_mut() {
                        Some(hangup) => hangup.recv().await,
                        None => std::future::pending().await,
                    }
                } => reload(node, access, tls, node_tls).await,
                _ = user1.recv() => match node.hand_off().await {
                    Ok(()) => tracing::info!("SIGUSR1: handing the slot over to the next owner"),
                    Err(error) => tracing::warn!(%error, "SIGUSR1: no handoff"),
                },
            }
        }
    }
    #[cfg(not(unix))]
    {
        let _ = (node, hangup, access, tls, node_tls);
        crate::signal().await;
    }
}

/// D-166 A: every file read and checked again, then all of it in use, or
/// none of it: a file that fails leaves the ones in use as they are, never a
/// looser policy. New connections use what is installed; open client
/// connections keep what they were admitted with. Open links are checked
/// again under the new link files, and the ones whose peer no longer proves
/// its node id are closed (D-172 C).
#[cfg(unix)]
async fn reload(
    node: &node::Node,
    access: Option<&config::Access>,
    tls: Option<&transport::Tls>,
    node_tls: Option<&transport::NodeTls>,
) {
    let Some(access) = access else {
        tracing::info!("SIGHUP: no TLS or credential files are configured; nothing to reload");
        return;
    };
    match access.reload() {
        Ok(reloaded) => {
            if let (Some(tls), Some(loaded)) = (tls, reloaded.tls) {
                tls.install(loaded);
            }
            if let (Some(node_tls), Some(loaded)) = (node_tls, reloaded.node) {
                node_tls.install(loaded);
            }
            if let Some(authority) = reloaded.authority {
                if !node.reload_authority(std::sync::Arc::new(authority)).await {
                    tracing::warn!("SIGHUP: the node has stopped; the credentials were not taken");
                    return;
                }
            }
            for warning in &reloaded.warnings {
                tracing::warn!("{warning}");
            }
            tracing::info!("SIGHUP: TLS and credential files reloaded");
        }
        Err(error) => {
            tracing::warn!(%error, "SIGHUP: reload refused; the files in use stay in use");
        }
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
