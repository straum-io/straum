//! The Straum broker, version 0: a single-shard, single-node broker of the
//! protocol in docs/protocol.md over the straum-store engine. One compio
//! runtime thread owns the store and serves every connection
//! (`server.rs`).

pub mod catalog;
pub mod config;
pub mod identity;
mod server;
mod sync;

use std::net::SocketAddr;

pub use config::Config;
pub use server::{BrokerError, StopHandle, serve};

/// A broker running on a thread of its own, for tests and embedding.
pub struct Broker {
    pub addr: SocketAddr,
    stop: StopHandle,
    thread: Option<std::thread::JoinHandle<Result<(), String>>>,
}

impl Broker {
    /// Starts a broker and waits until it accepts connections.
    pub fn start(config: Config) -> Result<Self, String> {
        config.validate()?;
        let (tx, rx) = std::sync::mpsc::channel();
        let errors = tx.clone();
        let thread = std::thread::spawn(move || {
            let rt = compio::runtime::Runtime::new().map_err(|e| e.to_string())?;
            let res = rt.block_on(serve(config, move |addr, stop| {
                let _ = tx.send(Ok((addr, stop)));
            }));
            if let Err(e) = &res {
                let _ = errors.send(Err(e.to_string()));
            }
            res.map_err(|e| e.to_string())
        });
        match rx.recv() {
            Ok(Ok((addr, stop))) => Ok(Self {
                addr,
                stop,
                thread: Some(thread),
            }),
            Ok(Err(e)) => {
                let _ = thread.join();
                Err(e)
            }
            Err(_) => Err(thread
                .join()
                .map_err(|_| "broker thread panicked".to_owned())
                .and_then(|r| r)
                .err()
                .unwrap_or_else(|| "broker stopped before listening".into())),
        }
    }

    /// Stops the broker (the shutdown sequence of `serve`) and waits for it.
    pub fn stop(mut self) -> Result<(), String> {
        self.stop.stop();
        self.thread.take().map_or(Ok(()), |t| {
            t.join().map_err(|_| "broker thread panicked".to_owned())?
        })
    }
}

impl Drop for Broker {
    fn drop(&mut self) {
        if let Some(t) = self.thread.take() {
            self.stop.stop();
            let _ = t.join();
        }
    }
}
