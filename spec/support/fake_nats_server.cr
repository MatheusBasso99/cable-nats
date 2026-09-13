require "json"
require "socket"

# A minimal in-process NATS server implementing just enough of the wire
# protocol (INFO, CONNECT, PING/PONG, SUB, UNSUB, PUB and MSG delivery with
# `*`/`>` wildcard matching) for the real `NATS::Client` to connect, publish,
# subscribe, flush, and reconnect against it.
#
# It exists so `crystal spec` runs fully self-contained: the suite exercises
# the genuine client code paths without an external `nats-server` process.
# Core pub/sub only — no headers (HPUB/HMSG), auth, TLS, or JetStream.
class FakeNATSServer
  # One accepted client socket plus the subscriptions it has registered,
  # keyed by sid. Writes are serialized so MSG deliveries and PONG replies
  # coming from different fibers cannot interleave on the wire.
  private class Connection
    getter subscriptions = {} of Int64 => String

    def initialize(@socket : TCPSocket)
      @write_mutex = Mutex.new
    end

    def deliver(subject : String, sid : Int64, payload : Bytes) : Nil
      write do |socket|
        socket << "MSG " << subject << ' ' << sid << ' ' << payload.size << "\r\n"
        socket.write payload
        socket << "\r\n"
      end
    end

    def send_line(line : String) : Nil
      write do |socket|
        socket << line << "\r\n"
      end
    end

    def close : Nil
      @socket.close rescue nil
    end

    private def write(& : TCPSocket ->) : Nil
      @write_mutex.synchronize do
        yield @socket
        @socket.flush
      end
    rescue IO::Error
      # The client went away mid-write; its reader fiber handles cleanup.
    end
  end

  getter port : Int32

  @listener : TCPServer
  @connections = [] of Connection
  @mutex = Mutex.new
  @ping_count = Atomic(Int32).new(0)
  @mute_pongs = false
  @held_pongs = [] of Connection

  # Binds to 127.0.0.1 on `port` (0 picks a free ephemeral port) and starts
  # accepting connections immediately.
  def initialize(port : Int32 = 0)
    @listener = TCPServer.new("127.0.0.1", port)
    @port = @listener.local_address.port
    listen
  end

  # Number of `PING` commands received from any client since the server
  # started. Each `NATS::Client#flush` sends exactly one.
  def ping_count : Int32
    @ping_count.get
  end

  # While muted, `PING` is still read and counted but its `PONG` is held back,
  # the way a stalled server would behave, so client flushes time out. Unmuting
  # sends every held `PONG`: dropping them instead would leave the client's
  # queue of pending flushes one reply behind for the rest of the connection.
  def mute_pongs=(mute : Bool) : Nil
    held = @mutex.synchronize do
      @mute_pongs = mute
      next [] of Connection if mute

      @held_pongs.dup.tap { @held_pongs.clear }
    end
    held.each(&.send_line("PONG"))
  end

  # Closes the listener and drops every open client connection.
  def stop : Nil
    @listener.close rescue nil
    @mutex.synchronize do
      @connections.each(&.close)
      @connections.clear
      @held_pongs.clear
    end
  end

  # Simulates a server restart for reconnection specs: drops everything, then
  # boots again on the same port so clients can reconnect.
  def restart : Nil
    stop
    @listener = TCPServer.new("127.0.0.1", @port)
    listen
  end

  private def listen : Nil
    listener = @listener
    spawn do
      while socket = listener.accept?
        handle socket
      end
    end
  end

  private def handle(socket : TCPSocket) : Nil
    socket.tcp_nodelay = true
    socket.sync = false
    connection = Connection.new(socket)
    @mutex.synchronize { @connections << connection }

    spawn do
      connection.send_line "INFO #{info_json}"
      loop do
        handle_command socket.read_line, connection, socket
      end
    rescue IO::Error
      # Client disconnected (or the server stopped); fall through to cleanup.
    ensure
      @mutex.synchronize { @connections.delete(connection) }
      connection.close
    end
  end

  private def handle_command(line : String, connection : Connection, socket : TCPSocket) : Nil
    case line
    when "PING"
      @ping_count.add(1)
      held = @mutex.synchronize do
        @held_pongs << connection if @mute_pongs
        @mute_pongs
      end
      connection.send_line "PONG" unless held
    when .starts_with?("SUB ")
      # SUB <subject> [queue group] <sid>
      tokens = line.split(' ')
      @mutex.synchronize { connection.subscriptions[tokens.last.to_i64] = tokens[1] }
    when .starts_with?("UNSUB ")
      # UNSUB <sid> [max messages] — max is ignored, the sid is dropped now
      @mutex.synchronize { connection.subscriptions.delete(line.split(' ')[1].to_i64) }
    when .starts_with?("PUB ")
      # PUB <subject> [reply-to] <bytes>, then <bytes> of payload plus CRLF
      tokens = line.split(' ')
      payload = Bytes.new(tokens.last.to_i)
      socket.read_fully(payload)
      socket.skip(2)
      publish tokens[1], payload
    else
      # CONNECT, PONG, etc. need no reply for these specs.
    end
  end

  private def publish(subject : String, payload : Bytes) : Nil
    targets = @mutex.synchronize do
      @connections.flat_map do |connection|
        connection.subscriptions.compact_map do |sid, pattern|
          {connection, sid} if subject_matches?(subject, pattern)
        end
      end
    end

    targets.each { |connection, sid| connection.deliver(subject, sid, payload) }
  end

  # NATS subject matching: `*` matches exactly one dot-separated token and a
  # trailing `>` matches one or more remaining tokens.
  private def subject_matches?(subject : String, pattern : String) : Bool
    subject_tokens = subject.split('.')
    pattern_tokens = pattern.split('.')

    pattern_tokens.each_with_index do |token, index|
      return true if token == ">"
      return false unless subject_token = subject_tokens[index]?
      return false unless token == "*" || token == subject_token
    end

    pattern_tokens.size == subject_tokens.size
  end

  private def info_json : String
    {
      server_id:   "FAKE",
      server_name: "fake-nats-spec-server",
      version:     "2.10.0",
      go:          "crystal",
      host:        "127.0.0.1",
      port:        port,
      headers:     true,
      max_payload: 1_048_576_i64,
      proto:       1,
    }.to_json
  end
end
