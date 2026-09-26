//! What one idle connection costs the broker, in resident memory.
//!
//! The broker runs as its own process and this tool as another, so the
//! number is the broker's and not the harness's — measuring both in one
//! process would count the client's sockets and buffers as the server's.
//!
//! Connections are opened in steps and held open. At each step the server's
//! RSS is sampled, so the output is a curve rather than one number: the
//! slope is the per-connection cost and the intercept is what the broker
//! costs before anybody arrives.
//!
//!   straum-server &
//!   connhold --address 127.0.0.1:1883 --pid $! --connections 15000 --step 2500
//!
//! Each connection sends a CONNECT and waits for its CONNACK, so the socket
//! is a session the broker has accounted for and not just an accepted fd.

use std::io::{Read, Write};
use std::net::TcpStream;

struct Settings {
    address: String,
    pid: u32,
    connections: usize,
    step: usize,
}

fn settings() -> Settings {
    let mut held = Settings {
        address: "127.0.0.1:1883".to_owned(),
        pid: 0,
        connections: 10_000,
        step: 2_500,
    };
    let mut args = std::env::args().skip(1);
    while let Some(flag) = args.next() {
        let mut value = || args.next().unwrap_or_default();
        match flag.as_str() {
            "--address" => held.address = value(),
            "--pid" => held.pid = value().parse().unwrap_or(0),
            "--connections" => held.connections = value().parse().unwrap_or(10_000),
            "--step" => held.step = value().parse().unwrap_or(2_500),
            _ => {}
        }
    }
    held
}

/// A CONNECT with a unique client id, clean session, 60 s keep-alive.
fn connect_packet(index: usize) -> Vec<u8> {
    let id = format!("hold-{index}");
    let id = id.as_bytes();
    let remaining = 10 + 2 + id.len();

    let mut out = Vec::with_capacity(2 + remaining);
    out.push(0x10);
    out.push(u8::try_from(remaining).unwrap_or(u8::MAX));
    out.extend_from_slice(&[0x00, 0x04, b'M', b'Q', b'T', b'T']);
    out.push(0x04);
    out.push(0x02);
    out.extend_from_slice(&60u16.to_be_bytes());
    out.extend_from_slice(&u16::try_from(id.len()).unwrap_or(0).to_be_bytes());
    out.extend_from_slice(id);
    out
}

/// Resident set size of a process, in KiB, as the system reports it.
fn rss(pid: u32) -> u64 {
    let out = std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &pid.to_string()])
        .output();
    out.ok()
        .and_then(|out| String::from_utf8(out.stdout).ok())
        .and_then(|text| text.trim().parse::<u64>().ok())
        .unwrap_or(0)
}

fn main() {
    let settings = settings();
    if settings.pid == 0 {
        eprintln!("connhold: --pid is required; it is the broker to measure");
        return;
    }

    let idle = rss(settings.pid);
    println!(
        "{:>12} {:>12} {:>14} {:>16}",
        "connections", "RSS KiB", "over idle KiB", "B/connection"
    );
    println!("{:>12} {:>12} {:>14} {:>16}", 0, idle, 0, "-");

    // Held for the life of the run: dropping one would close it.
    let mut held: Vec<TcpStream> = Vec::with_capacity(settings.connections);
    let mut connack = [0u8; 4];

    for index in 0..settings.connections {
        let mut stream = match TcpStream::connect(&settings.address) {
            Ok(stream) => stream,
            Err(error) => {
                eprintln!("connhold: stopped at {index}: {error}");
                break;
            }
        };
        if stream.set_nodelay(true).is_err()
            || stream.write_all(&connect_packet(index)).is_err()
            || stream.read_exact(&mut connack).is_err()
        {
            eprintln!("connhold: the broker refused connection {index}");
            break;
        }
        if connack != [0x20, 0x02, 0x00, 0x00] {
            eprintln!("connhold: connection {index} was not accepted: {connack:?}");
            break;
        }
        held.push(stream);

        let count = held.len();
        if count % settings.step == 0 {
            let now = rss(settings.pid);
            println!(
                "{:>12} {:>12} {:>14} {:>16.0}",
                count,
                now,
                now.saturating_sub(idle),
                now.saturating_sub(idle) as f64 * 1024.0 / count as f64,
            );
        }
    }

    let count = held.len();
    if count % settings.step != 0 {
        let now = rss(settings.pid);
        println!(
            "{:>12} {:>12} {:>14} {:>16.0}",
            count,
            now,
            now.saturating_sub(idle),
            now.saturating_sub(idle) as f64 * 1024.0 / count.max(1) as f64,
        );
    }
    // Sampled from outside from here on. Spawning `ps` out of a process
    // holding tens of thousands of descriptors is its own experiment, and
    // it is the one that failed first.
    println!("ready {count}");
    let _ = std::io::stdout().flush();
    let mut line = String::new();
    let _ = std::io::stdin().read_line(&mut line);
    drop(held);
}
