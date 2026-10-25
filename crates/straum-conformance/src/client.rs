//! Runs a client scenario (`conformance/client-scenarios/*.json`) with
//! straum-client against the fake server.

use std::time::Duration;

use straum_client::{Client, ClientConfig, ClientError, ProduceOptions};
use straum_proto::json::{self, Value};
use straum_proto::{Response, json::message_from_json};

use crate::fake::FakeServer;
use crate::matching::matches;

fn field<'a>(v: &'a Value, k: &str) -> Result<&'a Value, String> {
    v.get(k).ok_or_else(|| format!("client.{k} missing"))
}

fn num(v: &Value, k: &str) -> Result<u64, String> {
    match field(v, k)? {
        Value::Num(n) => Ok(*n as u64),
        Value::Str(s) => s.parse().map_err(|_| format!("client.{k}: not a number")),
        _ => Err(format!("client.{k}: not a number")),
    }
}

fn text<'a>(v: &'a Value, k: &str) -> Result<&'a str, String> {
    field(v, k)?
        .as_str()
        .ok_or_else(|| format!("client.{k}: not a string"))
}

/// The operation's outcome as JSON: `{"ok": <response body>}` or
/// `{"error": <code name or number>}`.
fn outcome(r: Result<Response, ClientError>) -> Value {
    match r {
        Ok(resp) => {
            let body = json::response_to_json(&resp)
                .get("body")
                .cloned()
                .unwrap_or(Value::Null);
            Value::Obj(vec![("ok".into(), body)])
        }
        Err(ClientError::Broker(e)) => Value::Obj(vec![(
            "error".into(),
            match e.code() {
                Some(c) => Value::Str(c.name().into()),
                None => Value::Num(e.code.into()),
            },
        )]),
        Err(e) => Value::Obj(vec![("error".into(), Value::Str(format!("client: {e}")))]),
    }
}

fn perform(addr: &str, op: &Value) -> Result<Value, String> {
    let mut config = ClientConfig::new(addr);
    if let Some(n) = op.get("max_attempts").and_then(Value::as_i64) {
        config.max_attempts = n as u32;
    }
    config.request_timeout = Some(Duration::from_secs(10));
    let mut client = match Client::connect(config) {
        Ok(c) => c,
        Err(e) => return Ok(outcome(Err(e))),
    };
    let r = match text(op, "op")? {
        "produce" => {
            let messages = field(op, "messages")?
                .as_array()
                .ok_or("client.messages: not an array")?
                .iter()
                .map(message_from_json)
                .collect::<Result<_, _>>()?;
            let opts = ProduceOptions {
                producer_id: num(op, "producer_id")?,
                producer_seq: num(op, "producer_seq")?,
                ..ProduceOptions::default()
            };
            client
                .produce(
                    text(op, "topic")?,
                    num(op, "partition")? as u32,
                    messages,
                    opts,
                )
                .map(Response::ProduceOk)
        }
        "fetch" => client
            .fetch(
                text(op, "topic")?,
                num(op, "partition")? as u32,
                num(op, "from_seq")?,
                num(op, "max_bytes")? as u32,
                num(op, "max_wait_ms")? as u32,
            )
            .map(Response::FetchOk),
        other => return Err(format!("unknown client op {other}")),
    };
    Ok(outcome(r))
}

/// Plays one client scenario: the fake server's script and the client's
/// operation, then compares the outcome with `result`.
pub fn run(scenario: &Value) -> Result<(), String> {
    let server = FakeServer::bind()?;
    let addr = server.addr();
    let steps = scenario
        .get("server")
        .and_then(Value::as_array)
        .ok_or("scenario has no server steps")?
        .to_vec();
    let script = std::thread::spawn(move || server.play(&steps));
    let op = scenario.get("client").ok_or("scenario has no client")?;
    let got = perform(&addr, op);
    let played = script
        .join()
        .map_err(|_| "fake server panicked".to_owned())?;
    let got = got?;
    played?;
    let want = scenario.get("result").ok_or("scenario has no result")?;
    matches(want, &got, "result")
}
