defmodule Absynthe.Relay.RelayIntegrationTest do
  use ExUnit.Case

  alias Absynthe.Relay.{Broker, Framing, Membrane}
  alias Absynthe.Relay.Packet.{Turn, TurnEvent, Assert, Retract, Message}

  @moduletag :integration
  @moduletag timeout: 10_000

  describe "Unix socket relay" do
    test "accepts connection and receives turn events" do
      # Start broker with Unix socket
      socket_path = "/tmp/absynthe_test_#{:erlang.unique_integer([:positive])}.sock"

      {:ok, broker} = Broker.start_link(socket_path: socket_path)

      # Give the listener time to start
      Process.sleep(100)

      # Connect as a client
      {:ok, client} = :gen_tcp.connect({:local, socket_path}, 0, [:binary, active: false])

      # Send an assertion to OID 0 (the dataspace)
      turn = %Turn{
        events: [
          %TurnEvent{
            oid: 0,
            event: %Assert{
              assertion: {:record, {{:symbol, "Ping"}, [{:integer, 1}]}},
              handle: 1
            }
          }
        ]
      }

      {:ok, data} = Framing.encode(turn)
      :ok = :gen_tcp.send(client, data)

      # Give the relay time to process
      Process.sleep(100)

      # Send a retraction
      retract_turn = %Turn{
        events: [
          %TurnEvent{
            oid: 0,
            event: %Retract{handle: 1}
          }
        ]
      }

      {:ok, retract_data} = Framing.encode(retract_turn)
      :ok = :gen_tcp.send(client, retract_data)

      # Give time to process
      Process.sleep(100)

      # Clean up
      :gen_tcp.close(client)
      Broker.stop(broker)
    end

    test "handles multiple assertions" do
      socket_path = "/tmp/absynthe_test_#{:erlang.unique_integer([:positive])}.sock"

      {:ok, broker} = Broker.start_link(socket_path: socket_path)
      Process.sleep(100)

      {:ok, client} = :gen_tcp.connect({:local, socket_path}, 0, [:binary, active: false])

      # Send multiple assertions in one turn
      turn = %Turn{
        events: [
          %TurnEvent{
            oid: 0,
            event: %Assert{assertion: {:string, "first"}, handle: 1}
          },
          %TurnEvent{
            oid: 0,
            event: %Assert{assertion: {:string, "second"}, handle: 2}
          },
          %TurnEvent{
            oid: 0,
            event: %Assert{assertion: {:string, "third"}, handle: 3}
          }
        ]
      }

      {:ok, data} = Framing.encode(turn)
      :ok = :gen_tcp.send(client, data)

      Process.sleep(100)

      # Retract middle one
      retract_turn = %Turn{
        events: [
          %TurnEvent{oid: 0, event: %Retract{handle: 2}}
        ]
      }

      {:ok, retract_data} = Framing.encode(retract_turn)
      :ok = :gen_tcp.send(client, retract_data)

      Process.sleep(100)

      :gen_tcp.close(client)
      Broker.stop(broker)
    end

    test "handles message events" do
      socket_path = "/tmp/absynthe_test_#{:erlang.unique_integer([:positive])}.sock"

      {:ok, broker} = Broker.start_link(socket_path: socket_path)
      Process.sleep(100)

      {:ok, client} = :gen_tcp.connect({:local, socket_path}, 0, [:binary, active: false])

      # Send a message
      turn = %Turn{
        events: [
          %TurnEvent{
            oid: 0,
            event: %Message{body: {:record, {{:symbol, "Ping"}, []}}}
          }
        ]
      }

      {:ok, data} = Framing.encode(turn)
      :ok = :gen_tcp.send(client, data)

      Process.sleep(100)

      :gen_tcp.close(client)
      Broker.stop(broker)
    end

    test "handles nop keepalive packets" do
      socket_path = "/tmp/absynthe_test_#{:erlang.unique_integer([:positive])}.sock"

      {:ok, broker} = Broker.start_link(socket_path: socket_path)
      Process.sleep(100)

      {:ok, client} = :gen_tcp.connect({:local, socket_path}, 0, [:binary, active: false])

      # Send nop packets
      {:ok, nop_data} = Framing.encode(:nop)
      :ok = :gen_tcp.send(client, nop_data)
      :ok = :gen_tcp.send(client, nop_data)
      :ok = :gen_tcp.send(client, nop_data)

      Process.sleep(100)

      :gen_tcp.close(client)
      Broker.stop(broker)
    end

    test "cleans up assertions when client disconnects" do
      socket_path = "/tmp/absynthe_test_#{:erlang.unique_integer([:positive])}.sock"

      {:ok, broker} = Broker.start_link(socket_path: socket_path)
      Process.sleep(100)

      {:ok, client} = :gen_tcp.connect({:local, socket_path}, 0, [:binary, active: false])

      # Send assertion
      turn = %Turn{
        events: [
          %TurnEvent{
            oid: 0,
            event: %Assert{assertion: {:string, "will be cleaned up"}, handle: 1}
          }
        ]
      }

      {:ok, data} = Framing.encode(turn)
      :ok = :gen_tcp.send(client, data)

      Process.sleep(100)

      # Disconnect without retracting
      :gen_tcp.close(client)

      # The relay should clean up the assertion
      Process.sleep(200)

      Broker.stop(broker)
    end

    test "handles multiple concurrent clients" do
      socket_path = "/tmp/absynthe_test_#{:erlang.unique_integer([:positive])}.sock"

      {:ok, broker} = Broker.start_link(socket_path: socket_path)
      Process.sleep(100)

      # Connect multiple clients
      {:ok, client1} = :gen_tcp.connect({:local, socket_path}, 0, [:binary, active: false])
      {:ok, client2} = :gen_tcp.connect({:local, socket_path}, 0, [:binary, active: false])

      Process.sleep(100)

      # Each client sends an assertion
      turn1 = %Turn{
        events: [
          %TurnEvent{
            oid: 0,
            event: %Assert{assertion: {:string, "from client 1"}, handle: 1}
          }
        ]
      }

      turn2 = %Turn{
        events: [
          %TurnEvent{
            oid: 0,
            event: %Assert{assertion: {:string, "from client 2"}, handle: 1}
          }
        ]
      }

      {:ok, data1} = Framing.encode(turn1)
      {:ok, data2} = Framing.encode(turn2)

      :ok = :gen_tcp.send(client1, data1)
      :ok = :gen_tcp.send(client2, data2)

      Process.sleep(100)

      :gen_tcp.close(client1)
      :gen_tcp.close(client2)
      Broker.stop(broker)
    end
  end

  describe "broker API" do
    test "returns dataspace ref" do
      socket_path = "/tmp/absynthe_test_#{:erlang.unique_integer([:positive])}.sock"

      {:ok, broker} = Broker.start_link(socket_path: socket_path)

      ref = Broker.dataspace_ref(broker)
      assert ref != nil
      assert ref.actor_id == {:broker, broker}

      Broker.stop(broker)
    end

    test "returns actor pid" do
      socket_path = "/tmp/absynthe_test_#{:erlang.unique_integer([:positive])}.sock"

      {:ok, broker} = Broker.start_link(socket_path: socket_path)

      pid = Broker.actor_pid(broker)
      assert is_pid(pid)
      assert Process.alive?(pid)

      Broker.stop(broker)
    end

    test "returns addresses" do
      socket_path = "/tmp/absynthe_test_#{:erlang.unique_integer([:positive])}.sock"

      {:ok, broker} = Broker.start_link(socket_path: socket_path)
      Process.sleep(100)

      addresses = Broker.addresses(broker)
      assert [{:unix, ^socket_path}] = addresses

      Broker.stop(broker)
    end
  end

  describe "idle timeout" do
    test "connection closes after idle timeout" do
      socket_path = "/tmp/absynthe_test_#{:erlang.unique_integer([:positive])}.sock"

      # Start broker with short idle timeout (100ms)
      {:ok, broker} = Broker.start_link(socket_path: socket_path, idle_timeout: 100)
      Process.sleep(50)

      # Connect as a client
      {:ok, client} = :gen_tcp.connect({:local, socket_path}, 0, [:binary, active: true])

      # Wait for idle timeout to trigger
      Process.sleep(200)

      # Should receive tcp_closed message
      assert_receive {:tcp_closed, ^client}, 500

      Broker.stop(broker)
    end

    test "activity resets idle timeout" do
      socket_path = "/tmp/absynthe_test_#{:erlang.unique_integer([:positive])}.sock"

      # Start broker with short idle timeout (200ms)
      {:ok, broker} = Broker.start_link(socket_path: socket_path, idle_timeout: 200)
      Process.sleep(50)

      # Connect as a client
      {:ok, client} = :gen_tcp.connect({:local, socket_path}, 0, [:binary, active: true])

      # Send activity every 100ms, which should reset the idle timer
      for _ <- 1..3 do
        Process.sleep(100)
        # Send a nop packet to reset idle timer
        {:ok, nop_data} = Framing.encode(:nop)
        :ok = :gen_tcp.send(client, nop_data)
      end

      # Connection should still be open after 300ms total
      refute_receive {:tcp_closed, ^client}, 100

      # Now wait for idle timeout
      Process.sleep(250)
      assert_receive {:tcp_closed, ^client}, 500

      Broker.stop(broker)
    end

    test "infinity timeout keeps connection open indefinitely" do
      socket_path = "/tmp/absynthe_test_#{:erlang.unique_integer([:positive])}.sock"

      # Start broker with no idle timeout (default)
      {:ok, broker} = Broker.start_link(socket_path: socket_path)
      Process.sleep(50)

      # Connect as a client
      {:ok, client} = :gen_tcp.connect({:local, socket_path}, 0, [:binary, active: true])

      # Wait a while - connection should stay open
      Process.sleep(300)
      refute_receive {:tcp_closed, ^client}, 0

      # Clean up
      :gen_tcp.close(client)
      Broker.stop(broker)
    end
  end
end
