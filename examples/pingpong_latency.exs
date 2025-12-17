# PingPong Latency Benchmark Example
#
# Inspired by syndicate-rs pingpong.rs, demonstrating:
# - Server (pong mode): Relay broker hosting a dataspace with Observe pattern subscription
# - Client (ping mode): Wire-level ping burst sender (one-way)
# - Statistics collection with percentile reporting (Stats module)
# - Periodic timer for throughput reporting
#
# CURRENT LIMITATIONS:
# - Client mode sends pings but doesn't receive pongs (missing bidirectional relay)
# - Full RTT measurement requires a client-side relay implementation
# - The server-side pong responder and stats collection work correctly
# - See cross_node_ping_pong.exs for assertion-based cross-node example
#
# For full bidirectional benchmarking, consider using the box_and_client example
# pattern with a local dataspace, or implementing a client-side relay connection.
#
# Usage:
#   # Start a server (pong mode) - fully functional
#   MIX_ENV=test mix run examples/pingpong_latency.exs -- pong
#
#   # Start a client (ping mode) - sends pings only, no RTT measurement
#   MIX_ENV=test mix run examples/pingpong_latency.exs -- ping
#
#   # Client with custom settings
#   MIX_ENV=test mix run examples/pingpong_latency.exs -- ping -t 10 -a 1 -l 1000 -b 64
#
# Options for ping mode:
#   -t, --turns      Number of turns to send (default: 1)
#   -a, --actions    Number of actions per turn (default: 1)
#   -l, --latency    Report latency every N samples (default: 0 = don't report percentiles)
#   -b, --bytes      Bytes of padding to add (default: 0)
#
# Run with: MIX_ENV=test mix run examples/pingpong_latency.exs -- <mode> [options]
# (Protocol consolidation must be disabled for runtime implementations)

defmodule PingPongLatency.Config do
  @moduledoc """
  Configuration for PingPong latency benchmark.
  """
  defstruct mode: :pong,
            turn_count: 1,
            action_count: 1,
            report_latency_every: 0,
            bytes_padding: 0,
            socket_path: "/tmp/absynthe_pingpong_latency.sock"

  @doc """
  Parse command line arguments into config.
  """
  def from_args(args) do
    {opts, positional, _} =
      OptionParser.parse(args,
        aliases: [t: :turns, a: :actions, l: :latency, b: :bytes, s: :socket],
        strict: [
          turns: :integer,
          actions: :integer,
          latency: :integer,
          bytes: :integer,
          socket: :string
        ]
      )

    mode =
      case positional do
        ["ping" | _] -> :ping
        ["pong" | _] -> :pong
        _ -> :pong
      end

    %__MODULE__{
      mode: mode,
      turn_count: Keyword.get(opts, :turns, 1),
      action_count: Keyword.get(opts, :actions, 1),
      report_latency_every: Keyword.get(opts, :latency, 0),
      bytes_padding: Keyword.get(opts, :bytes, 0),
      socket_path: Keyword.get(opts, :socket, "/tmp/absynthe_pingpong_latency.sock")
    }
  end
end

defmodule PingPongLatency.Stats do
  @moduledoc """
  Statistics collector for RTT samples.
  """
  defstruct samples: [],
            turn_counter: 0,
            event_counter: 0,
            report_every: 0,
            batch_count: 0

  def new(report_every) do
    %__MODULE__{
      samples: if(report_every > 0, do: :array.new(report_every, default: 0), else: nil),
      report_every: report_every
    }
  end

  def add_sample(stats, rtt_ns) when stats.report_every > 0 do
    samples = :array.set(stats.batch_count, rtt_ns, stats.samples)
    batch_count = stats.batch_count + 1

    if batch_count >= stats.report_every do
      report_latencies(samples, stats.report_every)
      %{stats | samples: :array.new(stats.report_every, default: 0), batch_count: 0}
    else
      %{stats | samples: samples, batch_count: batch_count}
    end
  end

  def add_sample(stats, _rtt_ns), do: stats

  def increment_turn(stats) do
    %{stats | turn_counter: stats.turn_counter + 1}
  end

  def increment_event(stats) do
    %{stats | event_counter: stats.event_counter + 1}
  end

  def report_periodic(stats) do
    IO.puts(
      "[Stats] #{stats.turn_counter} turns, #{stats.event_counter} events in the last second"
    )

    %{stats | turn_counter: 0, event_counter: 0}
  end

  defp report_latencies(samples_array, n) do
    # Convert array to sorted list
    samples =
      0..(n - 1)
      |> Enum.map(fn i -> :array.get(i, samples_array) end)
      |> Enum.sort()

    rtt_0 = Enum.at(samples, 0)
    rtt_50 = Enum.at(samples, div(n, 2))
    rtt_90 = Enum.at(samples, div(n * 90, 100))
    rtt_95 = Enum.at(samples, div(n * 95, 100))
    rtt_99 = Enum.at(samples, div(n * 99, 100))
    rtt_99_9 = Enum.at(samples, div(n * 999, 1000))
    rtt_99_99 = Enum.at(samples, div(n * 9999, 10000))
    rtt_max = Enum.at(samples, n - 1)

    # Convert nanoseconds to milliseconds for display
    to_ms = fn ns -> ns / 1_000_000.0 end

    IO.puts(
      "rtt: 0% #{:io_lib.format("~.5f", [to_ms.(rtt_0)])}ms, " <>
        "50% #{:io_lib.format("~.5f", [to_ms.(rtt_50)])}ms, " <>
        "90% #{:io_lib.format("~.5f", [to_ms.(rtt_90)])}ms, " <>
        "95% #{:io_lib.format("~.5f", [to_ms.(rtt_95)])}ms, " <>
        "99% #{:io_lib.format("~.5f", [to_ms.(rtt_99)])}ms, " <>
        "99.9% #{:io_lib.format("~.5f", [to_ms.(rtt_99_9)])}ms, " <>
        "99.99% #{:io_lib.format("~.5f", [to_ms.(rtt_99_99)])}ms, " <>
        "max #{:io_lib.format("~.5f", [to_ms.(rtt_max)])}ms"
    )

    IO.puts(
      "msg: 0% #{:io_lib.format("~.5f", [to_ms.(rtt_0) / 2])}ms, " <>
        "50% #{:io_lib.format("~.5f", [to_ms.(rtt_50) / 2])}ms, " <>
        "90% #{:io_lib.format("~.5f", [to_ms.(rtt_90) / 2])}ms, " <>
        "95% #{:io_lib.format("~.5f", [to_ms.(rtt_95) / 2])}ms, " <>
        "99% #{:io_lib.format("~.5f", [to_ms.(rtt_99) / 2])}ms, " <>
        "99.9% #{:io_lib.format("~.5f", [to_ms.(rtt_99_9) / 2])}ms, " <>
        "99.99% #{:io_lib.format("~.5f", [to_ms.(rtt_99_99) / 2])}ms, " <>
        "max #{:io_lib.format("~.5f", [to_ms.(rtt_max) / 2])}ms"
    )
  end
end

defmodule PingPongLatency.Consumer do
  @moduledoc """
  Entity that handles incoming Ping/Pong messages and tracks statistics.

  In ping mode: receives Pong messages, calculates RTT, sends next Ping
  In pong mode: receives Ping messages, echoes back as Pong (with same timestamp)
  """
  defstruct [
    :ds_ref,
    :send_label,
    :stats,
    :should_echo,
    :bytes_padding,
    :current_reply,
    :notify_pid
  ]

  defimpl Absynthe.Core.Entity do
    alias Absynthe.Protocol.Event

    # Handle periodic stats report timer
    def on_message(consumer, :report_stats, turn) do
      stats = PingPongLatency.Stats.report_periodic(consumer.stats)
      {%{consumer | stats: stats}, turn}
    end

    # Handle incoming Ping or Pong assertion notifications
    # The dataspace sends captured values as a sequence [timestamp, padding]
    def on_message(consumer, {:sequence, [{:integer, timestamp}, padding]}, turn) do
      stats = PingPongLatency.Stats.increment_event(consumer.stats)

      if consumer.should_echo do
        # Pong mode: echo back with same timestamp and padding
        reply = make_record(consumer.send_label, timestamp, padding)
        action = Event.message(consumer.ds_ref, reply)
        turn = Absynthe.Core.Turn.add_action(turn, action)
        {%{consumer | stats: stats}, turn}
      else
        # Ping mode: calculate RTT and send next ping
        rtt_ns = now_ns() - timestamp

        # Only process first event per turn (like syndicate-rs)
        {stats, turn, current_reply} =
          if consumer.current_reply == nil do
            stats = PingPongLatency.Stats.increment_turn(stats)
            stats = PingPongLatency.Stats.add_sample(stats, rtt_ns)

            # Prepare next ping with fresh timestamp
            reply = make_record(consumer.send_label, now_ns(), padding)
            action = Event.message(consumer.ds_ref, reply)
            turn = Absynthe.Core.Turn.add_action(turn, action)

            {stats, turn, reply}
          else
            # Already sent reply this turn, just send cached reply
            action = Event.message(consumer.ds_ref, consumer.current_reply)
            turn = Absynthe.Core.Turn.add_action(turn, action)
            {stats, turn, consumer.current_reply}
          end

        {%{consumer | stats: stats, current_reply: current_reply}, turn}
      end
    end

    # Handle stop message - notify waiting process
    def on_message(consumer, :stop, turn) do
      if consumer.notify_pid do
        send(consumer.notify_pid, :consumer_stopped)
      end

      {consumer, turn}
    end

    def on_message(consumer, _msg, turn), do: {consumer, turn}

    # Clear current_reply at end of turn (simulate pre_commit callback)
    def on_publish(consumer, _assertion, _handle, turn) do
      {%{consumer | current_reply: nil}, turn}
    end

    def on_retract(consumer, _handle, turn), do: {consumer, turn}
    def on_sync(consumer, _peer, turn), do: {consumer, turn}

    defp make_record(label, timestamp, padding) do
      {:record, {{:symbol, label}, [{:integer, timestamp}, padding]}}
    end

    defp now_ns do
      System.monotonic_time(:nanosecond)
    end
  end
end

defmodule PingPongLatency.Runner do
  @moduledoc """
  Orchestrates the PingPong latency benchmark.
  """

  alias Absynthe.Core.Actor
  alias Absynthe.Relay.{Broker, Client, Framing}
  alias Absynthe.Relay.Packet.{Turn, TurnEvent, Assert, Message}

  def run(config) do
    IO.puts("=== PingPong Latency Benchmark ===")
    IO.puts("Mode: #{config.mode}")

    if config.mode == :ping do
      IO.puts("Turns: #{config.turn_count}, Actions: #{config.action_count}")

      IO.puts(
        "Latency reporting: #{if config.report_latency_every > 0, do: "every #{config.report_latency_every} samples", else: "disabled"}"
      )

      IO.puts("Padding: #{config.bytes_padding} bytes")
    end

    IO.puts("Socket: #{config.socket_path}\n")

    case config.mode do
      :pong -> run_pong_server(config)
      :ping -> run_ping_client(config)
    end
  end

  defp run_pong_server(config) do
    IO.puts("[Server] Starting broker on #{config.socket_path}")

    # Clean up existing socket
    File.rm(config.socket_path)

    {:ok, broker} = Broker.start_link(socket_path: config.socket_path)
    dataspace_ref = Broker.dataspace_ref(broker)
    actor_pid = Broker.actor_pid(broker)

    # Create consumer entity for Pong mode (echoes Ping back as Pong)
    consumer = %PingPongLatency.Consumer{
      ds_ref: dataspace_ref,
      send_label: "Pong",
      stats: PingPongLatency.Stats.new(0),
      should_echo: true,
      bytes_padding: 0
    }

    {:ok, consumer_ref} = Actor.spawn_entity(actor_pid, :root, consumer)

    # Subscribe to Ping pattern: <Ping $ $> (capture timestamp and padding)
    observe_pattern =
      Absynthe.observe(
        Absynthe.record(:Ping, [Absynthe.capture(), Absynthe.capture()]),
        consumer_ref
      )

    {:ok, _observe_handle} = Actor.assert(actor_pid, dataspace_ref, observe_pattern)

    # Start periodic stats timer (every 1 second)
    start_periodic_timer(actor_pid, consumer_ref, :report_stats, 1000)

    IO.puts("[Server] Pong service ready, waiting for Ping messages...")
    IO.puts("[Server] Press Ctrl+C to stop\n")

    # Keep server running
    Process.sleep(:infinity)
  end

  defp run_ping_client(config) do
    IO.puts("[Client] Connecting to server at #{config.socket_path}...")

    # Wait for socket to be available
    wait_for_socket(config.socket_path, 5000)

    {:ok, client} = Client.connect_unix(config.socket_path)
    IO.puts("[Client] Connected!")

    # Create local actor for client side
    {:ok, client_actor} = Actor.start_link(id: :ping_client)

    # Create consumer entity for Ping mode (measures RTT, sends next Ping)
    consumer = %PingPongLatency.Consumer{
      ds_ref: nil,
      send_label: "Ping",
      stats: PingPongLatency.Stats.new(config.report_latency_every),
      should_echo: false,
      bytes_padding: config.bytes_padding,
      notify_pid: self()
    }

    {:ok, consumer_ref} = Actor.spawn_entity(client_actor, :root, consumer)

    # Subscribe to Pong pattern via relay
    # We need to send the Observe assertion to the remote dataspace (OID 0)
    observe_assertion = make_observe_pattern("Pong", consumer_ref)

    turn = %Turn{
      events: [
        %TurnEvent{
          oid: 0,
          event: %Assert{
            assertion: translate_to_wire(observe_assertion, consumer_ref),
            handle: 1
          }
        }
      ]
    }

    {:ok, data} = Framing.encode(turn)
    {:ok, client} = Client.send_data(client, data)

    Process.sleep(100)

    # Start periodic stats timer
    start_periodic_timer(client_actor, consumer_ref, :report_stats, 1000)

    IO.puts("[Client] Starting ping burst...")

    # Create padding
    padding = {:binary, :binary.copy(<<0>>, config.bytes_padding)}

    # Send initial burst of pings
    for turn_idx <- 1..config.turn_count do
      timestamp = System.monotonic_time(:nanosecond)
      ping = {:record, {{:symbol, "Ping"}, [{:integer, timestamp}, padding]}}

      events =
        for _action_idx <- 1..config.action_count do
          %TurnEvent{
            oid: 0,
            event: %Message{body: ping}
          }
        end

      turn = %Turn{events: events}
      {:ok, data} = Framing.encode(turn)
      {:ok, _client} = Client.send_data(client, data)

      if rem(turn_idx, 100) == 0 do
        IO.puts("[Client] Sent #{turn_idx} turns...")
      end
    end

    IO.puts("[Client] Burst complete. Waiting for responses...")

    # Let messages flow for a while
    wait_time = max(config.turn_count * 10, 5000)
    Process.sleep(wait_time)

    IO.puts("\n=== Benchmark Complete ===")

    # Cleanup
    Client.close(client)
    GenServer.stop(client_actor)
  end

  defp make_observe_pattern(recv_label, _observer_ref) do
    # Create Observe record with pattern <recv_label $ $>
    pattern =
      Absynthe.record(String.to_atom(recv_label), [Absynthe.capture(), Absynthe.capture()])

    # The observer ref will be translated to wire format separately
    pattern
  end

  defp translate_to_wire(pattern, observer_ref) do
    # Build wire-format Observe assertion
    # <Observe pattern #observer_ref>
    {:record,
     {{:symbol, "Observe"},
      [
        pattern,
        {:embedded, {:sequence, [{:integer, 0}, {:integer, observer_ref.entity_id}]}}
      ]}}
  end

  defp start_periodic_timer(actor_pid, entity_ref, message, interval_ms) do
    spawn(fn ->
      periodic_loop(actor_pid, entity_ref, message, interval_ms)
    end)
  end

  defp periodic_loop(actor_pid, entity_ref, message, interval_ms) do
    Process.sleep(interval_ms)

    if Process.alive?(actor_pid) do
      Absynthe.send_to(actor_pid, entity_ref, message)
      periodic_loop(actor_pid, entity_ref, message, interval_ms)
    end
  end

  defp wait_for_socket(path, timeout) when timeout > 0 do
    if File.exists?(path) do
      :ok
    else
      Process.sleep(100)
      wait_for_socket(path, timeout - 100)
    end
  end

  defp wait_for_socket(path, _timeout) do
    raise "Socket not available at #{path}"
  end
end

# Main entry point - only run when executed directly as a script
# When compiled for testing (Code.compile_file), set PINGPONG_LATENCY_SKIP=1 in env
# or simply use the modules directly in tests without running the script
unless System.get_env("PINGPONG_LATENCY_SKIP") do
  config = PingPongLatency.Config.from_args(System.argv())
  PingPongLatency.Runner.run(config)
end
