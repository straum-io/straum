//! Runs a broker scenario (`conformance/scenarios/*.json`) against a fresh
//! broker process: the runner starts the broker binary with a new data
//! directory and the scenario's settings, reads the bound address from its
//! `listening on` line, plays the steps and stops the broker.

use std::collections::HashMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use straum_proto::json::{self, Value};
use straum_proto::{FrameHeader, Response};

use crate::matching::matches;

/// A broker process with its own data directory, killed and removed on drop.
pub struct BrokerProcess {
    child: Child,
    pub addr: String,
    dir: PathBuf,
}

impl BrokerProcess {
    pub fn start(bin: &Path, settings: &[(String, String)]) -> Result<Self, String> {
        let dir = std::env::temp_dir().join(format!(
            "straum-conformance-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| d.as_nanos())
        ));
        let mut cmd = Command::new(bin);
        cmd.args(["--data-dir", &dir.to_string_lossy()]).args([
            "--listen",
            "127.0.0.1:0",
            "--cluster-id",
            "conformance",
            "--node-id",
            "1",
        ]);
        for (k, v) in settings {
            cmd.arg(format!("--{k}")).arg(v);
        }
        let mut child = cmd
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| format!("{}: {e}", bin.display()))?;
        let stderr = child.stderr.take().ok_or("no stderr")?;
        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            for line in BufReader::new(stderr).lines().map_while(Result::ok) {
                if let Some(addr) = line.strip_prefix("straum-server: listening on ") {
                    let _ = tx.send(addr.to_owned());
                }
            }
        });
        match rx.recv_timeout(Duration::from_secs(30)) {
            Ok(addr) => Ok(Self { child, addr, dir }),
            Err(_) => {
                let _ = child.kill();
                let status = child.wait().ok();
                Err(format!(
                    "the broker did not start listening (exit {status:?})"
                ))
            }
        }
    }
}

impl Drop for BrokerProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// One connection of a scenario: writes go through a thread, so a send
/// never blocks the script even when the broker stops reading.
struct Conn {
    stream: TcpStream,
    tx: Option<mpsc::Sender<Vec<u8>>>,
    next_id: u64,
    /// Received and not yet expected: correlation id → (response, time).
    got: HashMap<u64, (Response, Instant)>,
    buf: Vec<u8>,
    closed: bool,
}

impl Conn {
    fn open(addr: &str) -> Result<Self, String> {
        let stream = TcpStream::connect(addr).map_err(|e| format!("connect {addr}: {e}"))?;
        stream.set_nodelay(true).map_err(|e| e.to_string())?;
        let mut w = stream.try_clone().map_err(|e| e.to_string())?;
        let (tx, rx) = mpsc::channel::<Vec<u8>>();
        std::thread::spawn(move || {
            for frame in rx {
                if w.write_all(&frame).is_err() {
                    break;
                }
            }
        });
        Ok(Self {
            stream,
            tx: Some(tx),
            next_id: 1,
            got: HashMap::new(),
            buf: Vec::new(),
            closed: false,
        })
    }

    fn send(&mut self, bytes: Vec<u8>) -> Result<(), String> {
        self.tx
            .as_ref()
            .ok_or("connection closed by the scenario")?
            .send(bytes)
            .map_err(|_| "writer stopped".to_owned())
    }

    /// Reads until one more frame is complete, the connection closes, or
    /// `deadline`. Returns false on the deadline.
    fn read_some(&mut self, deadline: Instant) -> Result<bool, String> {
        let left = deadline.saturating_duration_since(Instant::now());
        if left.is_zero() {
            return Ok(false);
        }
        self.stream
            .set_read_timeout(Some(left))
            .map_err(|e| e.to_string())?;
        let mut chunk = [0; 64 * 1024];
        match self.stream.read(&mut chunk) {
            Ok(0) => {
                self.closed = true;
            }
            Ok(n) => self.buf.extend_from_slice(&chunk[..n]),
            Err(e)
                if matches!(
                    e.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) =>
            {
                return Ok(false);
            }
            Err(_) => self.closed = true,
        }
        while self.buf.len() >= straum_proto::FRAME_HEADER {
            let h = FrameHeader::parse(
                self.buf[..straum_proto::FRAME_HEADER]
                    .try_into()
                    .map_err(|_| "header")?,
            );
            let len = h.body_len().ok_or("response frame length below 10")?;
            if self.buf.len() < straum_proto::FRAME_HEADER + len {
                break;
            }
            let body: Vec<u8> = self
                .buf
                .drain(..straum_proto::FRAME_HEADER + len)
                .skip(straum_proto::FRAME_HEADER)
                .collect();
            let resp = Response::decode(h.msg_type, &body)
                .map_err(|e| format!("response does not decode: {e}"))?;
            self.got.insert(h.correlation_id, (resp, Instant::now()));
        }
        Ok(true)
    }
}

fn get_str<'a>(v: &'a Value, k: &str) -> Option<&'a str> {
    v.get(k).and_then(Value::as_str)
}

fn get_ms(v: &Value, k: &str, default: u64) -> u64 {
    v.get(k)
        .and_then(Value::as_i64)
        .map_or(default, |n| n.max(0) as u64)
}

/// The broker settings of a scenario's `broker` object.
pub fn settings(scenario: &Value) -> Vec<(String, String)> {
    match scenario.get("broker") {
        Some(Value::Obj(fields)) => fields
            .iter()
            .map(|(k, v)| {
                let v = match v {
                    Value::Str(s) => s.clone(),
                    Value::Num(n) => n.to_string(),
                    other => format!("{other:?}"),
                };
                (k.replace('_', "-"), v)
            })
            .collect(),
        _ => Vec::new(),
    }
}

/// Plays `scenario` against a new broker started from `bin`.
pub fn run(bin: &Path, scenario: &Value) -> Result<(), String> {
    let broker = BrokerProcess::start(bin, &settings(scenario))?;
    let mut s = State {
        addr: broker.addr.clone(),
        conns: HashMap::new(),
        sent: HashMap::new(),
    };
    let steps = scenario
        .get("steps")
        .and_then(Value::as_array)
        .ok_or("scenario has no steps")?;
    let res = s.steps(steps, None);
    for c in s.conns.values() {
        let _ = c.stream.shutdown(std::net::Shutdown::Both);
    }
    drop(broker);
    res
}

struct State {
    addr: String,
    conns: HashMap<String, Conn>,
    /// Step id → (connection, correlation id, send time).
    sent: HashMap<String, (String, u64, Instant)>,
}

impl State {
    fn conn(&mut self, name: &str) -> Result<&mut Conn, String> {
        self.conns
            .get_mut(name)
            .ok_or_else(|| format!("connection {name} not open"))
    }

    fn steps(&mut self, steps: &[Value], index: Option<usize>) -> Result<(), String> {
        for (n, step) in steps.iter().enumerate() {
            self.step(step, index)
                .map_err(|e| format!("step {n} {}: {e}", compact(step)))?;
        }
        Ok(())
    }

    fn step(&mut self, step: &Value, index: Option<usize>) -> Result<(), String> {
        let id_of = |v: &Value| {
            get_str(v, "id").map(|id| match index {
                Some(i) => format!("{id}#{i}"),
                None => id.to_owned(),
            })
        };
        if let Some(name) = get_str(step, "connect") {
            let c = Conn::open(&self.addr)?;
            self.conns.insert(name.to_owned(), c);
        } else if let Some(name) = get_str(step, "send") {
            let req = json::request_from_json(step.get("request").ok_or("send without request")?)?;
            let c = self.conn(name)?;
            let corr = match step.get("correlation_id") {
                Some(v) => v
                    .as_str()
                    .and_then(|s| s.parse().ok())
                    .ok_or("correlation_id is a decimal string")?,
                None => {
                    c.next_id += 1;
                    c.next_id - 1
                }
            };
            let mut frame = Vec::new();
            straum_proto::encode_request(&mut frame, corr, &req);
            c.send(frame)?;
            if let Some(id) = id_of(step) {
                self.sent
                    .insert(id, (name.to_owned(), corr, Instant::now()));
            }
        } else if let Some(name) = get_str(step, "send_raw") {
            let bytes = get_str(step, "hex")
                .and_then(json::unhex)
                .ok_or("send_raw needs hex")?;
            self.conn(name)?.send(bytes)?;
            if let Some(id) = id_of(step) {
                let corr = step
                    .get("correlation_id")
                    .and_then(Value::as_str)
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0);
                self.sent
                    .insert(id, (name.to_owned(), corr, Instant::now()));
            }
        } else if let Some(name) = get_str(step, "expect") {
            let id = id_of(step).ok_or("expect needs the id of a send")?;
            let (conn, corr, sent_at) = self
                .sent
                .get(&id)
                .cloned()
                .ok_or_else(|| format!("no send with id {id}"))?;
            if conn != name {
                return Err(format!("{id} was sent on {conn}, not {name}"));
            }
            let deadline = Instant::now() + Duration::from_millis(get_ms(step, "within_ms", 5000));
            let c = self.conn(name)?;
            let (resp, at) = loop {
                if let Some(r) = c.got.remove(&corr) {
                    break r;
                }
                if c.closed {
                    return Err(format!("connection closed before the response to {id}"));
                }
                if !c.read_some(deadline)? {
                    return Err(format!("no response to {id} in time"));
                }
            };
            let waited = at.duration_since(sent_at).as_millis() as u64;
            let not_before = get_ms(step, "not_before_ms", 0);
            if waited < not_before {
                return Err(format!(
                    "response to {id} after {waited} ms, expected at least {not_before} ms"
                ));
            }
            let want = step.get("response").ok_or("expect without response")?;
            matches(want, &json::response_to_json(&resp), "response")?;
        } else if let Some(name) = get_str(step, "expect_closed") {
            let deadline = Instant::now() + Duration::from_millis(get_ms(step, "within_ms", 5000));
            let c = self.conn(name)?;
            while !c.closed {
                if !c.read_some(deadline)? {
                    return Err("the connection is still open".into());
                }
            }
        } else if let Some(name) = get_str(step, "close") {
            if let Some(mut c) = self.conns.remove(name) {
                c.tx.take();
                let _ = c.stream.shutdown(std::net::Shutdown::Both);
            }
        } else if step.get("sleep_ms").is_some() {
            std::thread::sleep(Duration::from_millis(get_ms(step, "sleep_ms", 0)));
        } else if let Some(n) = step.get("repeat").and_then(Value::as_i64) {
            let inner = step
                .get("steps")
                .and_then(Value::as_array)
                .ok_or("repeat needs steps")?;
            for i in 0..n.max(0) as usize {
                self.steps(inner, Some(i))?;
            }
        } else {
            return Err("unknown step".into());
        }
        Ok(())
    }
}

fn compact(v: &Value) -> String {
    let s = v
        .to_pretty()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    if s.len() > 120 {
        format!(
            "{}…",
            &s[..s.char_indices().nth(117).map_or(s.len(), |(i, _)| i)]
        )
    } else {
        s
    }
}
