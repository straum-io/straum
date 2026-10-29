# straum-client

The reference client of the Straum protocol ([docs/protocol.md](../../docs/protocol.md)):
blocking, over `std::net`, no runtime. `Client` makes one request at a time
and follows the normative client behavior of section 6: on `NotLeader` or
`FencedLeaderEpoch` it refreshes Metadata, connects to the leader listed
there and retries with the same `producer_id` and `producer_seq` and the
leader's epoch; on `Overloaded` it backs off (at least `retry_after_ms`,
otherwise 10 ms doubling to 1 s); on a lost connection it reconnects and
retries; every other error, and any unknown code, goes to the caller.
`ClientConfig::max_attempts` (default 5) bounds the attempts of one
request. `Client::partition_for` is the default partitioner (crc32c of the
key modulo the partition count) or round robin without a key.
`Connection` is one TCP connection without retries, with pipelining
(`send`, then `recv` in any order).

```rust
let mut c = Client::connect(ClientConfig::new("127.0.0.1:7171"))?;
c.create_topic("orders", 1, 0)?;
let ok = c.produce("orders", 0, vec![Message::body(&b"hello"[..])], ProduceOptions::default())?;
let fetched = c.fetch("orders", 0, ok.base_seq, 1 << 20, 1000)?;
```

Tested by the client scenarios of `conformance/client-scenarios/` against
the fake server (`crates/straum-conformance`) and by the broker's tests.
Dependencies: straum-proto only.
