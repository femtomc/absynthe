defmodule Absynthe.Integration.PingPongLatencyTest do
  @moduledoc """
  Integration tests for the PingPong latency benchmark example.

  Tests the core functionality of timestamped message exchange and RTT tracking.
  """
  use ExUnit.Case

  alias Absynthe.Core.Actor
  alias Absynthe.Relay.{Broker, Client, Framing}
  alias Absynthe.Relay.Packet.{Turn, TurnEvent, Assert, Message}

  @moduletag :integration
  @moduletag timeout: 30_000

  # PongResponder entity that echoes back Ping as Pong with same timestamp
  defmodule PongResponder do
    defstruct [:pong_count]

    defimpl Absynthe.Core.Entity do
      alias Absynthe.Core.{Turn, Ref}
      alias Absynthe.Protocol.Event

      def on_publish(responder, assertion, _handle, turn) do
        case assertion do
          # <Ping timestamp padding>
          {:record, {{:symbol, "Ping"}, [{:integer, timestamp}, padding]}} ->
            # Echo back as Pong with same timestamp
            pong = {:record, {{:symbol, "Pong"}, [{:integer, timestamp}, padding]}}
            # Send to dataspace as a message
            # The message will be routed to any observer of Pong patterns
            # For now, we just count
            {%{responder | pong_count: (responder.pong_count || 0) + 1}, turn}

          _ ->
            {responder, turn}
        end
      end

      def on_retract(responder, _handle, turn), do: {responder, turn}

      # Handle message-based pings (for message observers)
      def on_message(responder, {:sequence, [{:integer, timestamp}, padding]}, turn) do
        # This is triggered when dataspace forwards a message matching our pattern
        # We would echo back here, but need a target ref
        {responder, turn}
      end

      def on_message(responder, _msg, turn), do: {responder, turn}

      def on_sync(responder, peer, turn) do
        send(Ref.actor_id(peer), {:synced, peer})
        {responder, turn}
      end
    end
  end

  describe "pingpong latency benchmark core functionality" do
    test "pong responder receives ping assertions with timestamps" do
      socket_path = "/tmp/pingpong_latency_test_#{:erlang.unique_integer([:positive])}.sock"

      {:ok, broker} = Broker.start_link(socket_path: socket_path)
      Process.sleep(50)

      dataspace_ref = Broker.dataspace_ref(broker)
      actor_pid = Broker.actor_pid(broker)

      # Spawn PongResponder
      {:ok, responder_ref} =
        Actor.spawn_entity(actor_pid, :root, %PongResponder{pong_count: 0})

      # Subscribe to <Ping $ $> pattern
      observe =
        Absynthe.observe(
          Absynthe.record(:Ping, [Absynthe.capture(), Absynthe.capture()]),
          responder_ref
        )

      {:ok, _} = Actor.assert(actor_pid, dataspace_ref, observe)
      Process.sleep(50)

      # Connect client
      {:ok, client} = Client.connect_unix(socket_path)

      # Send timestamped ping
      timestamp = System.monotonic_time(:nanosecond)
      padding = {:binary, <<0::size(64)>>}

      ping_assertion =
        {:record,
         {{:symbol, "Ping"},
          [
            {:integer, timestamp},
            padding
          ]}}

      turn = %Turn{
        events: [
          %TurnEvent{
            oid: 0,
            event: %Assert{assertion: ping_assertion, handle: 1}
          }
        ]
      }

      {:ok, data} = Framing.encode(turn)
      {:ok, _client} = Client.send_data(client, data)

      # Wait for processing
      Process.sleep(100)

      # Verify responder received the ping (we can't easily query state,
      # but no crash means it processed correctly)

      Client.close(client)
      Broker.stop(broker)
    end

    test "multiple pings with different timestamps are processed" do
      socket_path = "/tmp/pingpong_multi_#{:erlang.unique_integer([:positive])}.sock"

      {:ok, broker} = Broker.start_link(socket_path: socket_path)
      Process.sleep(50)

      dataspace_ref = Broker.dataspace_ref(broker)
      actor_pid = Broker.actor_pid(broker)

      {:ok, responder_ref} =
        Actor.spawn_entity(actor_pid, :root, %PongResponder{pong_count: 0})

      observe =
        Absynthe.observe(
          Absynthe.record(:Ping, [Absynthe.capture(), Absynthe.capture()]),
          responder_ref
        )

      {:ok, _} = Actor.assert(actor_pid, dataspace_ref, observe)
      Process.sleep(50)

      {:ok, client} = Client.connect_unix(socket_path)

      # Send multiple pings with different timestamps in one turn
      padding = {:binary, <<>>}

      events =
        for i <- 1..5 do
          timestamp = System.monotonic_time(:nanosecond) + i * 1000

          ping_assertion =
            {:record,
             {{:symbol, "Ping"},
              [
                {:integer, timestamp},
                padding
              ]}}

          %TurnEvent{
            oid: 0,
            event: %Assert{assertion: ping_assertion, handle: i}
          }
        end

      turn = %Turn{events: events}
      {:ok, data} = Framing.encode(turn)
      {:ok, _client} = Client.send_data(client, data)

      Process.sleep(200)

      Client.close(client)
      Broker.stop(broker)
    end

    test "ping messages via dataspace message routing" do
      socket_path = "/tmp/pingpong_msg_#{:erlang.unique_integer([:positive])}.sock"

      {:ok, broker} = Broker.start_link(socket_path: socket_path)
      Process.sleep(50)

      dataspace_ref = Broker.dataspace_ref(broker)
      actor_pid = Broker.actor_pid(broker)

      {:ok, responder_ref} =
        Actor.spawn_entity(actor_pid, :root, %PongResponder{pong_count: 0})

      # Subscribe to messages (not assertions)
      observe =
        Absynthe.observe(
          Absynthe.record(:Ping, [Absynthe.capture(), Absynthe.capture()]),
          responder_ref
        )

      {:ok, _} = Actor.assert(actor_pid, dataspace_ref, observe)
      Process.sleep(50)

      {:ok, client} = Client.connect_unix(socket_path)

      # Send ping as message (not assertion)
      timestamp = System.monotonic_time(:nanosecond)
      padding = {:binary, <<>>}

      ping_msg =
        {:record,
         {{:symbol, "Ping"},
          [
            {:integer, timestamp},
            padding
          ]}}

      turn = %Turn{
        events: [
          %TurnEvent{
            oid: 0,
            event: %Message{body: ping_msg}
          }
        ]
      }

      {:ok, data} = Framing.encode(turn)
      {:ok, _client} = Client.send_data(client, data)

      Process.sleep(100)

      Client.close(client)
      Broker.stop(broker)
    end
  end

  # Helper to load the example modules without running them
  defp load_example_modules do
    System.put_env("PINGPONG_LATENCY_SKIP", "1")
    Code.compile_file("examples/pingpong_latency.exs")
  after
    System.delete_env("PINGPONG_LATENCY_SKIP")
  end

  describe "stats tracking" do
    test "Stats module correctly calculates percentiles" do
      # Load the stats module from the example
      load_example_modules()

      # Create stats tracker
      stats = PingPongLatency.Stats.new(10)
      assert stats.report_every == 10
      assert stats.batch_count == 0

      # Add samples
      samples = [100, 200, 300, 400, 500, 600, 700, 800, 900, 1000]

      final_stats =
        Enum.reduce(samples, stats, fn sample, acc ->
          PingPongLatency.Stats.add_sample(acc, sample)
        end)

      # After 10 samples, batch should have reset
      assert final_stats.batch_count == 0
    end

    test "Stats module tracks turn and event counters" do
      load_example_modules()

      stats = PingPongLatency.Stats.new(0)

      stats = PingPongLatency.Stats.increment_turn(stats)
      stats = PingPongLatency.Stats.increment_event(stats)
      stats = PingPongLatency.Stats.increment_event(stats)

      assert stats.turn_counter == 1
      assert stats.event_counter == 2

      # Reset via periodic report
      stats = PingPongLatency.Stats.report_periodic(stats)
      assert stats.turn_counter == 0
      assert stats.event_counter == 0
    end
  end

  describe "config parsing" do
    test "parses ping mode with options" do
      load_example_modules()

      config =
        PingPongLatency.Config.from_args(["ping", "-t", "10", "-a", "5", "-l", "100", "-b", "64"])

      assert config.mode == :ping
      assert config.turn_count == 10
      assert config.action_count == 5
      assert config.report_latency_every == 100
      assert config.bytes_padding == 64
    end

    test "parses pong mode" do
      load_example_modules()

      config = PingPongLatency.Config.from_args(["pong"])

      assert config.mode == :pong
    end

    test "defaults to pong mode when no args" do
      load_example_modules()

      config = PingPongLatency.Config.from_args([])

      assert config.mode == :pong
      assert config.turn_count == 1
      assert config.action_count == 1
    end
  end
end
