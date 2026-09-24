//! Composition root: read the environment, look at the kernel, wire it up,
//! run until a signal says otherwise.

mod config;

use std::process::ExitCode;

use config::{Settings, Source};
use runtime::{Durability, KernelLimits, Runtime};
use tracing_subscriber::EnvFilter;

fn main() -> ExitCode {
    let kernel = KernelLimits::probe();
    let settings = match Settings::parse(std::env::vars(), kernel) {
        Ok(settings) => settings,
        Err(error) => {
            // Before tracing exists, and a misconfigured broker must not start.
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

    report(&settings, &kernel);

    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder.enable_all();
    if let Some(threads) = settings.worker_threads {
        builder.worker_threads(threads);
    }
    let tokio = match builder.build() {
        Ok(tokio) => tokio,
        Err(error) => {
            tracing::error!(%error, "could not start the async runtime");
            return ExitCode::FAILURE;
        }
    };

    tokio.block_on(run(settings))
}

async fn run(settings: Settings) -> ExitCode {
    let broker = match Runtime::start(settings.runtime) {
        Ok(broker) => broker,
        Err(error) => {
            tracing::error!(%error, "could not start");
            return ExitCode::FAILURE;
        }
    };

    signal().await;
    tracing::info!("stopping");
    broker.shutdown().await;
    ExitCode::SUCCESS
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

/// One startup report: what the kernel says, what we resolved, and where the
/// two disagree (docs/linux-tuning.md).
fn report(settings: &Settings, kernel: &KernelLimits) {
    match kernel.somaxconn {
        Some(_) => tracing::info!(
            somaxconn = ?kernel.somaxconn,
            tcp_max_syn_backlog = ?kernel.max_syn_backlog,
            rmem_max = ?kernel.rmem_max,
            wmem_max = ?kernel.wmem_max,
            open_files = ?kernel.open_files,
            "kernel parameters"
        ),
        None => {
            tracing::info!("kernel parameters unavailable on this platform; `auto` uses defaults")
        }
    }

    for (name, value, source) in &settings.effective {
        let source = match source {
            Source::Env => "env",
            Source::Default => "default",
            Source::Kernel => "kernel",
        };
        tracing::info!(%name, %value, %source, "setting");
    }

    let transport = &settings.runtime.transport;
    if let Some(somaxconn) = kernel.somaxconn {
        if transport.backlog > somaxconn {
            tracing::warn!(
                backlog = transport.backlog,
                somaxconn,
                "the kernel will clamp the accept backlog"
            );
        }
    }
    if let Some(syn) = kernel.max_syn_backlog {
        if transport.backlog > syn {
            tracing::info!(
                backlog = transport.backlog,
                tcp_max_syn_backlog = syn,
                "the SYN queue may overflow before the accept queue does"
            );
        }
    }
    if let Some(open_files) = kernel.open_files {
        let needed = (transport.max_connections as u64).saturating_add(64);
        if needed > open_files {
            tracing::warn!(
                max_connections = transport.max_connections,
                open_files,
                "accepts will fail with EMFILE before reaching the configured maximum"
            );
        }
    }

    // Accepted, documented, and not acted on yet: saying so is cheaper than
    // letting somebody discover it in production.
    for name in &settings.ignored {
        tracing::warn!(
            %name,
            "set but not applicable on this platform; ignored so one env file works on both"
        );
    }
    if settings.runtime.auth.passwords.is_empty() && settings.runtime.auth.allow_anonymous {
        tracing::warn!(
            "no credentials configured and anonymous access allowed: anybody who can \
             reach this port may publish and subscribe to anything"
        );
    }
    if settings.runtime.tls.is_none() {
        tracing::warn!("no TLS: credentials and messages travel in the clear");
    }
    if settings.runtime.transport.listeners > 1 {
        tracing::info!(
            listeners = settings.runtime.transport.listeners,
            "listening on several sockets; the kernel balances accepts across them"
        );
    }
    if settings.runtime.durability != Durability::Memory {
        tracing::warn!(
            durability = ?settings.runtime.durability,
            path = %settings.runtime.wal.path.display(),
            queue = settings.runtime.wal.queue,
            max_batch_bytes = settings.runtime.wal.limits.max_batch_bytes,
            "the WAL is not wired into the runtime yet; messages are held in memory only"
        );
    }
}
