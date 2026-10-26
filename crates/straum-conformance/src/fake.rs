//! The scripted fake server of `conformance/client-scenarios/`: it plays a
//! scenario's `server` steps in order, answering what a v0 broker never
//! sends (leader changes, fencing, overload, a reset mid-request). It
//! succeeds when every step ran and the client sent nothing more.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::time::{Duration, Instant};

use straum_proto::json::{self, Value};
use straum_proto::{FrameHeader, Request};

use crate::matching::matches;

const STEP_TIMEOUT: Duration = Duration::from_secs(10);
/// After the last step, a request arriving within this time fails the run.
const QUIET: Duration = Duration::from_millis(300);

pub struct FakeServer {
    pub listener: TcpListener,
    pub host: String,
    pub port: u16,
}

impl FakeServer {
    pub fn bind() -> Result<Self, String> {
        let listener = TcpListener::bind("127.0.0.1:0").map_err(|e| e.to_string())?;
        let addr = listener.local_addr().map_err(|e| e.to_string())?;
        Ok(Self {
            listener,
            host: addr.ip().to_string(),
            port: addr.port(),
        })
    }

    pub fn addr(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }

    /// Replaces `"{{host}}"` and `"{{port}}"` in a reply with this server's
    /// address, so Metadata can list nodes at it.
    fn fill(&self, v: &Value) -> Value {
        match v {
            Value::Str(s) if s == "{{host}}" => Value::Str(self.host.clone()),
            Value::Str(s) if s == "{{port}}" => Value::Num(self.port.into()),
            Value::Arr(a) => Value::Arr(a.iter().map(|x| self.fill(x)).collect()),
            Value::Obj(f) => Value::Obj(f.iter().map(|(k, x)| (k.clone(), self.fill(x))).collect()),
            other => other.clone(),
        }
    }

    /// Plays `steps` (a scenario's `server` array).
    pub fn play(&self, steps: &[Value]) -> Result<(), String> {
        let mut conn: Option<TcpStream> = None;
        let mut last_reply = Instant::now();
        for (n, step) in steps.iter().enumerate() {
            let fail = |e: String| format!("server step {n}: {e}");
            if step.get("accept").and_then(Value::as_bool) == Some(true) {
                self.listener
                    .set_nonblocking(true)
                    .map_err(|e| fail(e.to_string()))?;
                let deadline = Instant::now() + STEP_TIMEOUT;
                let s = loop {
                    match self.listener.accept() {
                        Ok((s, _)) => break s,
                        Err(e)
                            if e.kind() == std::io::ErrorKind::WouldBlock
                                && Instant::now() < deadline =>
                        {
                            std::thread::sleep(Duration::from_millis(5));
                        }
                        Err(e) => return Err(fail(format!("no connection: {e}"))),
                    }
                };
                s.set_nonblocking(false).map_err(|e| fail(e.to_string()))?;
                conn = Some(s);
                continue;
            }
            let s = conn
                .as_mut()
                .ok_or_else(|| fail("no connection accepted".into()))?;
            if let Some(want) = step.get("expect") {
                let (corr, req) = read_request(s, STEP_TIMEOUT).map_err(fail)?;
                let got = json::request_to_json(&req);
                matches(want, &got, "request").map_err(fail)?;
                let since = last_reply.elapsed().as_millis() as u64;
                if let Some(min) = step.get("not_before_ms").and_then(Value::as_i64)
                    && since < min as u64
                {
                    return Err(fail(format!(
                        "request {since} ms after the last reply, expected at least {min} ms"
                    )));
                }
                if let Some(reply) = step.get("reply") {
                    let resp = json::response_from_json(&self.fill(reply)).map_err(fail)?;
                    let mut frame = Vec::new();
                    straum_proto::encode_response(&mut frame, corr, &resp);
                    s.write_all(&frame).map_err(|e| fail(e.to_string()))?;
                    last_reply = Instant::now();
                }
            }
            if step.get("close").and_then(Value::as_bool) == Some(true)
                && let Some(s) = conn.take()
            {
                let _ = s.shutdown(std::net::Shutdown::Both);
            }
        }
        if let Some(s) = conn.as_mut()
            && let Ok((_, req)) = read_request(s, QUIET)
        {
            return Err(format!(
                "unexpected request after the script: {}",
                req.name()
            ));
        }
        Ok(())
    }
}

fn read_request(s: &mut TcpStream, timeout: Duration) -> Result<(u64, Request), String> {
    s.set_read_timeout(Some(timeout))
        .map_err(|e| e.to_string())?;
    let mut head = [0; straum_proto::FRAME_HEADER];
    s.read_exact(&mut head)
        .map_err(|e| format!("no request: {e}"))?;
    let h = FrameHeader::parse(&head);
    let mut body = vec![0; h.body_len().ok_or("frame length below 10")?];
    s.read_exact(&mut body)
        .map_err(|e| format!("request body: {e}"))?;
    let req =
        Request::decode(h.msg_type, &body).map_err(|e| format!("request does not decode: {e}"))?;
    Ok((h.correlation_id, req))
}
