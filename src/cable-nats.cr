require "nats"

module Cable
  # A `Cable::BackendCore` implementation that drives Cable's pub/sub over
  # core NATS (at-most-once, fire-and-forget — the same delivery semantics as
  # Redis pub/sub).
  #
  # Unlike Redis, NATS multiplexes publishing and subscribing over a single
  # socket, so one shared `NATS::Client` serves as both the subscribe and the
  # publish connection. Keepalive (protocol PING/PONG) and reconnection with
  # automatic resubscription are built into the client, so the `ping_*` hooks
  # only need to flush the connection to prove liveness to `Cable::BackendPinger`.
  #
  # Enable it by pointing Cable at a NATS server:
  #
  # ```
  # Cable.configure do |settings|
  #   settings.url = ENV.fetch("CABLE_BACKEND_URL", "nats://localhost:4222")
  #   settings.backend_class = Cable::NATSBackend
  # end
  # ```
  class NATSBackend < Cable::BackendCore
    VERSION = "0.1.0"

    register "nats" # nats://
    register "tls"  # tls:// (NATS over TLS)

    # NATS subjects cannot contain whitespace/control characters or the
    # reserved tokens `.` (token separator), `*` and `>` (wildcards), all of
    # which are legal in Cable stream identifiers. `%` is reserved here as the
    # escape character so the encoding stays reversible.
    private RESERVED_SUBJECT_CHARS = {'.', '*', '>', '%'}

    private getter client : NATS::Client = NATS::Client.new(URI.parse(Cable.settings.url))

    @subscriptions = {} of String => NATS::Subscription
    @subscriptions_mutex = Mutex.new
    @closed = false
    @shutdown_signal = ::Channel(Nil).new

    # Encodes a Cable stream identifier into a valid NATS subject by
    # percent-encoding reserved subject characters (`.`, `*`, `>`, `%`) and
    # whitespace/control characters. The mapping is reversible via
    # `.stream_identifier_for`.
    #
    # ```
    # Cable::NATSBackend.subject_for("chat_1")     # => "chat_1"
    # Cable::NATSBackend.subject_for("room 1.two") # => "room%201%2Etwo"
    # ```
    def self.subject_for(stream_identifier : String) : String
      String.build(stream_identifier.bytesize) do |str|
        stream_identifier.each_char do |char|
          if char.ord < 33 || char.in?(RESERVED_SUBJECT_CHARS)
            str << '%' << char.ord.to_s(16, upcase: true).rjust(2, '0')
          else
            str << char
          end
        end
      end
    end

    # Decodes a subject produced by `.subject_for` back into the original
    # Cable stream identifier.
    #
    # ```
    # Cable::NATSBackend.stream_identifier_for("room%201%2Etwo") # => "room 1.two"
    # ```
    def self.stream_identifier_for(subject : String) : String
      subject.gsub(/%([0-9A-F]{2})/) { $1.to_i(16).chr }
    end

    # Returns the shared `NATS::Client`. NATS multiplexes subscriptions and
    # publishes over one socket, so this is the same client returned by
    # `#publish_connection`.
    def subscribe_connection : NATS::Client
      client
    end

    # Returns the shared `NATS::Client` (see `#subscribe_connection`).
    def publish_connection : NATS::Client
      client
    end

    # Flushes, drains, and closes the shared client. Safe to call more than
    # once — the server closes the subscribe and publish connections
    # separately, which both resolve to the same client here.
    def close_subscribe_connection
      close_client
    end

    # :ditto:
    def close_publish_connection
      close_client
    end

    # Subscribes to Cable's internal channel, answering `ping` and `debug`
    # broadcasts and forwarding everything else to the server's fiber channel.
    # The calling fiber is parked until the backend is closed, mirroring the
    # blocking subscribe loop the server expects from the Redis backend.
    def open_subscribe_connection(channel)
      return if @closed

      client.subscribe(self.class.subject_for(channel)) do |message, _subscription|
        body = String.new(message.body)
        if channel == Cable::INTERNAL[:channel] && body == "ping"
          Cable::Logger.debug { "Cable::NATSBackend#open_subscribe_connection -> PONG" }
        elsif channel == Cable::INTERNAL[:channel] && body == "debug"
          Cable.server.debug
        else
          Cable.server.fiber_channel.send({channel, body})
          Cable::Logger.debug { "Cable::NATSBackend#open_subscribe_connection channel:#{channel} message:#{body}" }
        end
      end

      @shutdown_signal.receive?
    end

    # Broadcasts `message` to every subscriber of `stream_identifier`.
    def publish_message(stream_identifier : String, message : String)
      return if @closed

      client.publish(self.class.subject_for(stream_identifier), message)
    end

    # Starts streaming `stream_identifier`, forwarding each message to the
    # server's fiber channel as a `{stream_identifier, body}` tuple. The
    # `NATS::Subscription` is tracked so `#unsubscribe` can cancel it later.
    def subscribe(stream_identifier : String)
      return if @closed

      @subscriptions_mutex.synchronize do
        next if @subscriptions.has_key?(stream_identifier)

        @subscriptions[stream_identifier] = client.subscribe(self.class.subject_for(stream_identifier)) do |message, _subscription|
          body = String.new(message.body)
          Cable.server.fiber_channel.send({stream_identifier, body})
          Cable::Logger.debug { "Cable::NATSBackend#subscribe channel:#{stream_identifier} message:#{body}" }
        end
      end
    end

    # Stops streaming `stream_identifier`. Unknown identifiers are ignored —
    # the server unsubscribes defensively even for streams it never tracked.
    def unsubscribe(stream_identifier : String)
      subscription = @subscriptions_mutex.synchronize do
        @subscriptions.delete(stream_identifier)
      end
      return if subscription.nil? || @closed

      client.unsubscribe(subscription)
    end

    # Keepalive hook called by `Cable::BackendPinger`. The NATS client already
    # exchanges protocol PING/PONG on its own timer and reconnects
    # automatically, so a round-trip flush is enough to surface a dead
    # connection to the pinger's restart-on-error logic.
    def ping_subscribe_connection
      return if @closed

      client.flush
    end

    # :ditto:
    def ping_publish_connection
      return if @closed

      client.flush
    end

    # Closing flushes and drains the connection, which can block indefinitely
    # when the server is unreachable and the client is stuck in its reconnect
    # loop. Bound it so `Cable::Server#shutdown` (and therefore `Cable.restart`)
    # can never hang on a dead backend.
    private CLOSE_TIMEOUT = 5.seconds

    private def close_client
      return if @closed

      @closed = true
      @shutdown_signal.close
      @subscriptions_mutex.synchronize { @subscriptions.clear }

      done = ::Channel(Nil).new(1)
      spawn do
        client.close
      rescue e
        Cable::Logger.debug { "Cable::NATSBackend#close_client error while closing: #{e.message}" }
      ensure
        done.close
      end

      select
      when done.receive?
      when timeout(CLOSE_TIMEOUT)
        Cable::Logger.warn { "Cable::NATSBackend#close_client timed out after #{CLOSE_TIMEOUT}; abandoning connection" }
      end
    end
  end

  class Server
    # `Cable::Server`'s memoized `backend_publish`/`backend_subscribe` getters
    # carry no type annotation, so the compiler cannot infer their instance
    # variables through `Cable::BackendRegistry`'s untyped delegation. Pin them
    # to the client type this backend hands out.
    @backend_publish : NATS::Client?
    @backend_subscribe : NATS::Client?
  end
end
