//! Composition root: read the environment, start the node, run until a
//! signal says otherwise. One path since step 8 (D-111): a node of the
//! cluster path, experimental and not production-ready (D-102 A).

mod config;

use std::process::ExitCode;

use config::{Settings, Source};
use tracing_subscriber::EnvFilter;

fn main() -> ExitCode {
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
        "EXPERIMENTAL cluster path (ADR-0008): not production-ready; no client protocol yet \
         (D-188 A); flow control off; timing defaults are placeholders; failover and \
         performance are not confirmed by Linux measurements"
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
    let node_tls = settings.node.tls.clone();
    tokio.block_on(run(settings.node, settings.access, node_tls))
}

async fn run(
    config: node::NodeConfig,
    access: Option<config::Access>,
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
    tracing::info!(listen = %node.listen, metrics = ?node.metrics_listen, "node started");
    // A node whose loop has ended serves nothing: the process exits, so a
    // supervisor sees it and restarts it (D-152 A).
    let stopped = tokio::select! {
        () = serve_signals(&node, hangup, access.as_ref(), node_tls.as_ref()) => false,
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
/// each SIGHUP reloads the node TLS files (D-166 A).
async fn serve_signals(
    node: &node::Node,
    hangup: Hangup,
    access: Option<&config::Access>,
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
                } => reload(access, node_tls),
                _ = user1.recv() => match node.hand_off().await {
                    Ok(()) => tracing::info!("SIGUSR1: handing the slot over to the next owner"),
                    Err(error) => tracing::warn!(%error, "SIGUSR1: no handoff"),
                },
            }
        }
    }
    #[cfg(not(unix))]
    {
        let _ = (node, hangup, access, node_tls);
        crate::signal().await;
    }
}

/// D-166 A: every file read and checked again, then all of it in use, or
/// none of it: a file that fails leaves the ones in use as they are. Open
/// links are checked again under the new files, and the ones whose peer no
/// longer proves its node id are closed (D-172 C).
#[cfg(unix)]
fn reload(access: Option<&config::Access>, node_tls: Option<&transport::NodeTls>) {
    let (Some(access), Some(node_tls)) = (access, node_tls) else {
        tracing::info!("SIGHUP: no node TLS files are configured; nothing to reload");
        return;
    };
    match access.reload() {
        Ok(loaded) => {
            node_tls.install(loaded);
            tracing::info!("SIGHUP: node TLS files reloaded");
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
