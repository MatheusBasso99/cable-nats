require "spec"
require "log/spec"
require "cable"
require "../src/cable-nats"
require "./support/*"

# By default the suite is fully self-contained: it boots an in-process
# `FakeNATSServer` and points the real `NATS::Client` at it, so `crystal spec`
# needs no external services. Point `CABLE_BACKEND_URL` at a real server to
# run the same suite as integration tests instead:
#
#     docker run --rm -p 4222:4222 nats:latest
#     CABLE_BACKEND_URL=nats://localhost:4222 crystal spec
module BackendEnvironment
  extend self

  @@fake_server : FakeNATSServer?

  # True when `CABLE_BACKEND_URL` selects a real NATS server.
  def integration? : Bool
    !ENV["CABLE_BACKEND_URL"]?.presence.nil?
  end

  def url : String
    ENV["CABLE_BACKEND_URL"]?.presence || "nats://127.0.0.1:#{fake_server.port}"
  end

  # The shared fake server behind every spec except the reconnection ones,
  # which boot their own restartable instance.
  def fake_server : FakeNATSServer
    @@fake_server ||= FakeNATSServer.new
  end
end

Cable.configure do |settings|
  settings.route = "/updates"
  settings.token = "test_token"
  settings.url = BackendEnvironment.url
  settings.backend_class = Cable::NATSBackend
  # Also the NATS client's keepalive interval. Specs count PINGs exactly and
  # hold PONGs back for seconds, so keep the client's own PINGs out of them;
  # the keepalive spec lowers it for itself.
  settings.backend_ping_interval = 10.minutes
  settings.restart_error_allowance = 2
end

Spec.before_each do
  Cable.restart
end
