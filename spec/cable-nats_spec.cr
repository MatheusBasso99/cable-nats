require "./spec_helper"

include RequestHelpers

describe Cable::NATSBackend do
  describe "registration" do
    it "registers for the nats and tls URI schemes" do
      Cable::BackendRegistry::REGISTERED_BACKENDS["nats"].should eq(Cable::NATSBackend)
      Cable::BackendRegistry::REGISTERED_BACKENDS["tls"].should eq(Cable::NATSBackend)
    end
  end

  describe ".subject_for" do
    it "leaves NATS-safe identifiers untouched" do
      Cable::NATSBackend.subject_for("chat_1").should eq("chat_1")
      Cable::NATSBackend.subject_for("cable_internal/abc-123").should eq("cable_internal/abc-123")
    end

    it "escapes characters NATS subjects cannot contain" do
      Cable::NATSBackend.subject_for("chat room").should eq("chat%20room")
      Cable::NATSBackend.subject_for("a.b").should eq("a%2Eb")
      Cable::NATSBackend.subject_for("a*b").should eq("a%2Ab")
      Cable::NATSBackend.subject_for("a>b").should eq("a%3Eb")
      Cable::NATSBackend.subject_for("100%").should eq("100%25")
      Cable::NATSBackend.subject_for("a\t\r\n\0b").should eq("a%09%0D%0A%00b")
    end

    it "produces subjects the NATS client accepts for publishing" do
      client = Cable.server.backend_publish
      identifier = "chat room.one*two>three%four\tfive"
      subject = Cable::NATSBackend.subject_for(identifier)

      client.valid_publish_subject?(subject).should be_true
      client.valid_subscribe_subject?(subject).should be_true
    end
  end

  describe ".stream_identifier_for" do
    it "reverses .subject_for" do
      [
        "chat_1",
        "chat room",
        "a.b.c",
        "wild*card>subject",
        "100%25 already encoded",
        "tabs\tand\nnewlines",
        "unicode äöü snowman ☃",
      ].each do |identifier|
        subject = Cable::NATSBackend.subject_for(identifier)
        Cable::NATSBackend.stream_identifier_for(subject).should eq(identifier)
      end
    end
  end

  describe "connection management" do
    it "shares a single client between the subscribe and publish connections" do
      backend = Cable::NATSBackend.new
      backend.subscribe_connection.should be_a(NATS::Client)
      backend.subscribe_connection.should be(backend.publish_connection)
      backend.close_subscribe_connection
    end

    it "guards against double-close and ignores operations after closing" do
      backend = Cable::NATSBackend.new
      backend.subscribe_connection

      backend.close_subscribe_connection
      backend.close_publish_connection

      backend.publish_message("chat_1", "hello")
      backend.subscribe("chat_1")
      backend.unsubscribe("chat_1")
      backend.ping_subscribe_connection
      backend.ping_publish_connection
      backend.open_subscribe_connection(Cable::INTERNAL[:channel])
    end

    it "shuts down the server cleanly" do
      Cable.server.backend_subscribe.should be_a(NATS::Client)
      Cable.server.shutdown
    end
  end

  describe "ping/pong" do
    it "answers the BackendPinger keepalive hooks with a flush" do
      Cable.server.backend.ping_subscribe_connection
      Cable.server.backend.ping_publish_connection
    end
  end

  describe "internal channel" do
    it "answers internal ping broadcasts with a PONG log" do
      Log.capture("cable", :debug) do |logs|
        settle_subscriptions
        Cable.server.publish(Cable::INTERNAL[:channel], "ping")
        sleep 200.milliseconds
        logs.check(:debug, /PONG/)
      end
    end

    it "answers internal debug broadcasts with a server debug dump" do
      Log.capture("cable", :debug) do |logs|
        settle_subscriptions
        Cable.server.publish(Cable::INTERNAL[:channel], "debug")
        sleep 200.milliseconds
        logs.check(:debug, /Some Good Information/)
      end
    end

    it "forwards other internal messages to the fiber channel without crashing" do
      settle_subscriptions
      Cable.server.publish(Cable::INTERNAL[:channel], "anything else")
      sleep 200.milliseconds
      Cable.server.backend.ping_publish_connection
    end
  end

  describe "streaming" do
    it "connects and publishes through NATS" do
      connect do |connection, socket|
        identifier = {channel: "ChatChannel", room: "1"}.to_json
        connection.receive({"command" => "subscribe", "identifier" => identifier}.to_json)
        wait_for { socket.messages.includes?(confirmation(identifier)) }

        settle_subscriptions
        json_message = %({"foo": "bar"})
        Cable.server.publish(channel: "chat_1", message: json_message)
        wait_for { socket.messages.includes?(stream_message(identifier, json_message)) }

        socket.messages.should contain(confirmation(identifier))
        socket.messages.should contain(stream_message(identifier, json_message))
      end
    end

    it "streams multiple channels independently" do
      connect do |connection, socket|
        identifier_one = {channel: "ChatChannel", room: "1"}.to_json
        identifier_two = {channel: "ChatChannel", room: "2"}.to_json
        connection.receive({"command" => "subscribe", "identifier" => identifier_one}.to_json)
        connection.receive({"command" => "subscribe", "identifier" => identifier_two}.to_json)
        wait_for { socket.messages.includes?(confirmation(identifier_two)) }

        settle_subscriptions
        Cable.server.publish(channel: "chat_1", message: %({"n": 1}))
        Cable.server.publish(channel: "chat_2", message: %({"n": 2}))

        wait_for { socket.messages.includes?(stream_message(identifier_one, %({"n": 1}))) }
        wait_for { socket.messages.includes?(stream_message(identifier_two, %({"n": 2}))) }
      end
    end

    it "stops streaming after unsubscribe" do
      connect do |connection, socket|
        identifier = {channel: "ChatChannel", room: "1"}.to_json
        connection.receive({"command" => "subscribe", "identifier" => identifier}.to_json)
        wait_for { socket.messages.includes?(confirmation(identifier)) }

        connection.receive({"command" => "unsubscribe", "identifier" => identifier}.to_json)
        wait_for { socket.messages.any?(&.includes?(Cable.message(:unsubscribe))) }

        settle_subscriptions
        Cable.server.publish(channel: "chat_1", message: %({"foo": "bar"}))
        sleep 200.milliseconds

        socket.messages.should_not contain(stream_message(identifier, %({"foo": "bar"})))
      end
    end

    it "does not double-subscribe when two channels stream the same identifier" do
      socket_one = DummySocket.new(IO::Memory.new)
      connection_one = ConnectionTest.new(builds_request(token: "test-token"), socket_one)
      socket_two = DummySocket.new(IO::Memory.new)
      connection_two = ConnectionTest.new(builds_request(token: "test-token"), socket_two)

      begin
        identifier = {channel: "ChatChannel", room: "1"}.to_json
        connection_one.receive({"command" => "subscribe", "identifier" => identifier}.to_json)
        connection_two.receive({"command" => "subscribe", "identifier" => identifier}.to_json)
        wait_for { socket_two.messages.includes?(confirmation(identifier)) }

        settle_subscriptions
        json_message = %({"n": 1})
        Cable.server.publish(channel: "chat_1", message: json_message)
        wait_for { socket_one.messages.includes?(stream_message(identifier, json_message)) }
        wait_for { socket_two.messages.includes?(stream_message(identifier, json_message)) }
        sleep 200.milliseconds

        socket_one.messages.count(stream_message(identifier, json_message)).should eq(1)
        socket_two.messages.count(stream_message(identifier, json_message)).should eq(1)
      ensure
        connection_one.close
        connection_two.close
      end
    end

    it "ignores unsubscribes for identifiers that were never subscribed" do
      Cable.server.backend.unsubscribe("never_subscribed")
    end

    it "streams identifiers that need subject sanitization" do
      connect do |connection, socket|
        room = "lounge one.two*three>four"
        identifier = {channel: "ChatChannel", room: room}.to_json
        connection.receive({"command" => "subscribe", "identifier" => identifier}.to_json)
        wait_for { socket.messages.includes?(confirmation(identifier)) }

        settle_subscriptions
        json_message = %({"foo": "bar"})
        Cable.server.publish(channel: "chat_#{room}", message: json_message)
        wait_for { socket.messages.includes?(stream_message(identifier, json_message)) }
      end
    end
  end

  describe "reconnection" do
    it "keeps streaming after the NATS server restarts", tags: "reconnect" do
      with_restartable_nats do |url, restart_server|
        original_url = Cable.settings.url

        begin
          Cable.settings.url = url
          Cable.restart

          connect do |connection, socket|
            identifier = {channel: "ChatChannel", room: "1"}.to_json
            connection.receive({"command" => "subscribe", "identifier" => identifier}.to_json)
            wait_for { socket.messages.includes?(confirmation(identifier)) }

            settle_subscriptions
            Cable.server.publish(channel: "chat_1", message: %({"n": 1}))
            wait_for { socket.messages.includes?(stream_message(identifier, %({"n": 1}))) }

            restart_server.call

            # The client needs a beat to notice the drop, reconnect, and
            # resubscribe; publishes are fire-and-forget, so retry until one
            # lands on the resubscribed stream.
            wait_for(timeout: 30.seconds) do
              Cable.server.publish(channel: "chat_1", message: %({"n": 2}))
              sleep 100.milliseconds
              socket.messages.includes?(stream_message(identifier, %({"n": 2})))
            end
          end
        ensure
          # Restore and restart while the server is still reachable so the
          # old client can shut down cleanly.
          Cable.settings.url = original_url
          Cable.restart
        end
      end
    end
  end
end

private class ChatChannel < Cable::Channel
  def subscribed
    stream_from "chat_#{params["room"]}"
  end

  def receive(message)
  end

  def perform(action, action_params)
  end

  def unsubscribed
  end
end

private class ConnectionTest < Cable::Connection
  identified_by :identifier

  def connect
    if tk = token
      self.identifier = tk
    end
  end

  def broadcast_to(channel, message)
  end
end

private def connect(&)
  socket = DummySocket.new(IO::Memory.new)
  connection = ConnectionTest.new(builds_request(token: "test-token"), socket)

  yield connection, socket

  connection.close
  socket.close
end

private def confirmation(identifier : String) : String
  {"type" => Cable.message(:confirmation), "identifier" => identifier}.to_json
end

private def stream_message(identifier : String, json_message : String) : String
  {"identifier" => identifier, "message" => JSON.parse(json_message)}.to_json
end

# Round-trips the shared client so every SUB written so far is guaranteed to
# have been processed by the NATS server before we publish to it.
private def settle_subscriptions
  client = Cable.server.backend_publish
  Fiber.yield
  client.flush
end

# Yields a NATS URL plus a proc that restarts the server behind it, dropping
# every connection. Against the default in-process backend this restarts a
# dedicated `FakeNATSServer`; in integration mode (`CABLE_BACKEND_URL` set) it
# restarts a Docker container running the real `nats-server`.
private def with_restartable_nats(&)
  if BackendEnvironment.integration?
    pending!("docker is required for the reconnection spec in integration mode") unless docker_available?

    container = "cable-nats-spec-reconnect"
    docker("rm", "-f", container)
    docker!("run", "-d", "--rm", "--name", container, "-p", "14222:4222", "nats:latest")

    begin
      wait_for_port(14222)
      yield "nats://localhost:14222", -> do
        docker!("restart", container)
        wait_for_port(14222)
        # Give the client a moment to notice the drop before writing again.
        sleep 2.seconds
      end
    ensure
      docker("rm", "-f", container)
    end
  else
    server = FakeNATSServer.new

    begin
      yield "nats://127.0.0.1:#{server.port}", -> { server.restart }
    ensure
      server.stop
    end
  end
end

private def wait_for(timeout : Time::Span = 5.seconds, &)
  deadline = Time.instant + timeout
  until yield
    fail "timed out after #{timeout} waiting for condition" if Time.instant > deadline
    sleep 20.milliseconds
  end
end

private def wait_for_port(port : Int32)
  wait_for(timeout: 15.seconds) do
    TCPSocket.new("localhost", port, connect_timeout: 500.milliseconds).close
    true
  rescue Socket::Error | IO::TimeoutError
    false
  end
end

private def docker_available? : Bool
  docker("info")
end

private def docker(*args : String) : Bool
  Process.run("docker", args.to_a, output: Process::Redirect::Close, error: Process::Redirect::Close).success?
rescue IO::Error
  false
end

private def docker!(*args : String)
  fail "docker #{args.join(' ')} failed" unless docker(*args)
end
