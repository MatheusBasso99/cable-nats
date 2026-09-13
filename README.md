# cable-nats

This is a [Cable.cr](https://github.com/cable-cr/cable) backend extension for [jgaskins/nats](https://github.com/jgaskins/nats).

It drives Cable's ActionCable-style WebSocket pub/sub over core [NATS](https://nats.io)
publish/subscribe — the same at-most-once, fire-and-forget delivery semantics as Redis
pub/sub, so it is a drop-in alternative to
[cable-redis](https://github.com/cable-cr/cable-redis). No JetStream required.

> [!WARNING]
> **Experimental — not production tested.** This adapter is an experimental proof of
> concept generated with Fable 5 and has not yet been exercised in a real production
> workload. For that reason, no pull request has been opened against the upstream
> repository yet. Use it at your own risk and validate it thoroughly before relying on
> it in production.

## Installation

1. Add the dependency to your `shard.yml`:

   ```yaml
   dependencies:
     cable:
       github: cable-cr/cable
     cable-nats:
       github: cable-cr/cable-nats
   ```

2. Run `shards install`

## Usage

```crystal
require "cable"
require "cable-nats"

Cable.configure do |settings|
  settings.url = ENV.fetch("CABLE_BACKEND_URL", "nats://127.0.0.1:4222")
  settings.backend_class = Cable::NATSBackend
  # ... all other Cable config settings
end
```

Both the `nats://` and `tls://` URI schemes are registered, so a URL like
`tls://nats.example.com:4222` selects this backend and connects over TLS.

## NATS

NATS removes most of the connection babysitting a Redis pub/sub backend needs.
A few things worth knowing about how this backend maps Cable onto it:

### One connection, not two

Redis cannot issue commands on a connection that is subscribed, so `cable-redis`
maintains separate subscribe and publish connections. NATS multiplexes publishes
and subscriptions over a single socket, so this backend shares one `NATS::Client`
between `subscribe_connection` and `publish_connection`.

### Keepalive and reconnection are built in

The NATS client exchanges protocol PING/PONG on its own timer, and when the
connection drops it reconnects automatically, re-establishes every subscription,
and replays writes buffered during the outage. You do not need to configure
anything to get this.

Cable's own backend pinger still runs and is still useful: this backend answers
`ping_subscribe_connection`/`ping_publish_connection` with a round-trip flush, so
a dead connection surfaces as an error and feeds Cable's restart logic.

```crystal
Cable.configure do |settings|
  settings.backend_ping_interval = 15.seconds # default is 15.
  settings.restart_error_allowance = 20       # default is 20. Use 0 to disable restarts
end
```

> NOTE: An error log `Cable.restart` will be invoked whenever a restart happens.
> We highly advise you to monitor these logs.

### Subscription confirm means listening

The NATS client does not write each command to the socket: it buffers them and
flushes every 10 ms (`NATS_FLUSH_INTERVAL_MS`). Cable sends
`confirm_subscription` to the WebSocket client as soon as the backend's
`subscribe` returns, so without help a broadcast from another process (a
background worker, say) could reach the NATS server in that window, before the
`SUB`, and be dropped for this node while the client believed it was listening.

So for every new stream identifier, `subscribe` waits for a PING/PONG round trip
before it returns. The server handles a connection's commands in order, so the
`PONG` proves it has registered the subscription: by the time the client sees
the confirmation, a broadcast from any process reaches it.

- **Cost:** one round trip per new stream identifier per process, not per
  viewer. Identifiers the node already streams return immediately. That
  includes the `cable_internal/<identifier>` stream every connection opens, so
  an identified user's first connection to a node pays one too. Concurrent
  subscriptions share round trips: one is in flight at a time, and its `PONG`
  covers every `SUB` written before its `PING`.
- **If the server does not answer** within `Cable::NATSBackend::FLUSH_TIMEOUT`
  (2 seconds), `subscribe` logs a warning through `Cable::Logger` and returns.
  The subscription itself is not lost: the `SUB` is already buffered and goes
  out on the next outbound flush (or is replayed on reconnect). Only the
  guarantee is lost, for messages published before the server registers it.
- **A stalled connection never blocks.** Once `max_pings_out` (2) PINGs are
  unanswered, the backend sends no more: `subscribe` warns and returns at once,
  and `ping_subscribe_connection`/`ping_publish_connection` raise. One more PING
  could fill the client's bounded queue of pending PONGs, and the client waits
  for room while holding the lock every write (and its own reconnect) needs.
- **Scope:** the guarantee holds for the NATS server this node is connected to.
  In a cluster, interest propagates between servers asynchronously, so a
  broadcast published through another server in that interval can still miss.

### Stream identifiers are sanitized into NATS subjects

NATS subjects cannot contain whitespace or the reserved characters `.` (token
separator), `*` and `>` (wildcards). Cable stream identifiers created with
`stream_from` can contain any of these, so the backend percent-encodes reserved
characters (and `%` itself) when building the subject, keeping the mapping
reversible:

```crystal
Cable::NATSBackend.subject_for("room 1.two")            # => "room%201%2Etwo"
Cable::NATSBackend.stream_identifier_for("room%201%2Etwo") # => "room 1.two"
```

This is transparent — you keep using your identifiers as-is on both the
`stream_from` and `broadcast` sides. It only matters if you point other NATS
tooling at the subjects Cable uses.

### Delivery semantics

Core NATS pub/sub is at-most-once: subscribers only receive messages published
while they are connected, and nothing is persisted. This matches Redis pub/sub,
so anything built on `cable-redis` behaves the same here.

## Development

`crystal spec` is fully self-contained — no NATS server, Docker, or any other
external service is required. The suite boots an in-process fake NATS server
(`spec/support/fake_nats_server.cr`) that speaks enough of the NATS wire
protocol for the real `NATS::Client` to connect, subscribe, publish, and
reconnect against it:

```sh
crystal spec
```

To run the same suite against a real NATS server instead (integration mode),
point `CABLE_BACKEND_URL` at one:

```sh
docker run --rm -d -p 4222:4222 nats:latest
CABLE_BACKEND_URL=nats://localhost:4222 crystal spec
```

In integration mode the reconnection spec restarts a real `nats-server` inside
a Docker container; it is marked pending when Docker is unavailable.

Streaming specs publish as soon as the subscription is confirmed, with no
settling, because that is the guarantee described above. Widening the client's
outbound interval makes a regression fail reliably instead of occasionally:

```sh
NATS_FLUSH_INTERVAL_MS=500 crystal spec
```

The spec helper `settle_subscriptions` is only for writes nothing confirms on
its own: the internal channel (subscribed from a fiber spawned at boot), UNSUB,
and a subscribe whose round trip failed.

1. Make the update
2. Add a spec and run `crystal spec` (and `NATS_FLUSH_INTERVAL_MS=500 crystal spec`)
3. Format it `crystal tool format spec/ src/`
4. Ameba `./bin/ameba` (build it once after `shards install`: `mkdir -p bin && crystal build -o bin/ameba lib/ameba/bin/ameba.cr`)
5. Commit it
6. GO TO 1

## Contributing

1. Fork it (<https://github.com/cable-cr/cable-nats/fork>)
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create a new Pull Request

## Contributors

- [Jeremy Woertink](https://github.com/jwoertink) - creator and maintainer
