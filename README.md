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

Specs need a running NATS server (the reconnection spec also needs Docker):

```sh
docker run --rm -p 4222:4222 nats:latest
```

Override the URL with `CABLE_BACKEND_URL` (default `nats://localhost:4222`).

1. Make the update
2. Add a spec and run `crystal spec`
3. Format it `crystal tool format spec/ src/`
4. Ameba `./bin/ameba`
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
