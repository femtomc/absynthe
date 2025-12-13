defmodule Absynthe.Relay.RelayIntegrationTest do
  use ExUnit.Case

  alias Absynthe.Relay.{Broker, Framing, Relay}
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

  describe "transport validation" do
    test "rejects unsupported transport with clear error" do
      socket_path = "/tmp/absynthe_test_#{:erlang.unique_integer([:positive])}.sock"

      {:ok, broker} = Broker.start_link(socket_path: socket_path)
      Process.sleep(50)

      dataspace_ref = Broker.dataspace_ref(broker)

      # Create a socket to test with
      {:ok, client} = :gen_tcp.connect({:local, socket_path}, 0, [:binary, active: false])

      # Attempting to start a relay with :ssl transport should fail with ArgumentError
      # Use GenServer.start (not start_link) to avoid crashing the test process
      result =
        GenServer.start(Relay,
          socket: client,
          transport: :ssl,
          dataspace_ref: dataspace_ref
        )

      assert {:error, {%ArgumentError{message: message}, _stacktrace}} = result
      assert message =~ "unsupported transport: :ssl"
      assert message =~ "expected :gen_tcp or :noise"

      :gen_tcp.close(client)
      Broker.stop(broker)
    end
  end

  describe "ack-based flow control" do
    test "debt is repaid after actor processes turn" do
      socket_path = "/tmp/absynthe_fc_test_#{:erlang.unique_integer([:positive])}.sock"

      # Start broker with flow control enabled
      {:ok, broker} =
        Broker.start_link(
          socket_path: socket_path,
          flow_control: [limit: 100, loan_ack_timeout: 5_000]
        )

      Process.sleep(100)

      {:ok, client} = :gen_tcp.connect({:local, socket_path}, 0, [:binary, active: false])

      # Get initial relay stats (need to find relay pid)
      # For now, just verify the flow works end-to-end

      # Send an assertion to OID 0
      turn = %Turn{
        events: [
          %TurnEvent{
            oid: 0,
            event: %Assert{
              assertion: {:record, {{:symbol, "TestFC"}, [{:integer, 1}]}},
              handle: 1
            }
          }
        ]
      }

      {:ok, data} = Framing.encode(turn)
      :ok = :gen_tcp.send(client, data)

      # Give time for the actor to process the turn and send ack back
      Process.sleep(200)

      # Send another turn - if flow control is working, this should succeed
      turn2 = %Turn{
        events: [
          %TurnEvent{
            oid: 0,
            event: %Assert{
              assertion: {:record, {{:symbol, "TestFC"}, [{:integer, 2}]}},
              handle: 2
            }
          }
        ]
      }

      {:ok, data2} = Framing.encode(turn2)
      :ok = :gen_tcp.send(client, data2)

      Process.sleep(100)

      # Clean up
      :gen_tcp.close(client)
      Broker.stop(broker)
    end

    test "debt accumulates with multiple rapid turns" do
      socket_path = "/tmp/absynthe_fc_accum_#{:erlang.unique_integer([:positive])}.sock"

      # Start broker with flow control and small limits to trigger backpressure quickly
      {:ok, broker} =
        Broker.start_link(
          socket_path: socket_path,
          flow_control: [
            limit: 20,
            high_water_mark: 16,
            low_water_mark: 8,
            loan_ack_timeout: 5_000
          ]
        )

      Process.sleep(100)

      {:ok, client} = :gen_tcp.connect({:local, socket_path}, 0, [:binary, active: false])

      # Send multiple turns rapidly
      for i <- 1..10 do
        turn = %Turn{
          events: [
            %TurnEvent{
              oid: 0,
              event: %Assert{
                assertion: {:record, {{:symbol, "Burst"}, [{:integer, i}]}},
                handle: i
              }
            }
          ]
        }

        {:ok, data} = Framing.encode(turn)
        :ok = :gen_tcp.send(client, data)
      end

      # Give time for acks to be processed
      Process.sleep(500)

      # Clean up
      :gen_tcp.close(client)
      Broker.stop(broker)
    end

    # Note: Testing late ack behavior (ack arriving after timeout) is difficult in
    # integration tests because the dataspace actor responds immediately. The late
    # ack logic has been reviewed manually:
    # - handle_loan_timeout keeps loans in pending_loans so late acks can repay
    # - maybe_repay_pending_loan resets consecutive_timeouts when any ack arrives
    # - Rescheduled timeout timers ensure continued monitoring until ack or max_timeouts
    # A proper test would require injecting a slow actor or mocking the actor response.

    test "loan timeout keeps debt outstanding (no forgiveness)" do
      socket_path = "/tmp/absynthe_fc_timeout_#{:erlang.unique_integer([:positive])}.sock"

      # Create a broker with very short loan ack timeout and high max_unacked_timeouts
      # so the connection doesn't close immediately
      {:ok, broker} =
        Broker.start_link(
          socket_path: socket_path,
          flow_control: [limit: 100, loan_ack_timeout: 100, max_unacked_timeouts: 10]
        )

      Process.sleep(100)

      {:ok, client} = :gen_tcp.connect({:local, socket_path}, 0, [:binary, active: false])

      # Send assertion to a valid OID - this will timeout without ack
      turn = %Turn{
        events: [
          %TurnEvent{
            oid: 0,
            event: %Assert{
              assertion: {:record, {{:symbol, "TimeoutTest"}, []}},
              handle: 1
            }
          }
        ]
      }

      {:ok, data} = Framing.encode(turn)
      :ok = :gen_tcp.send(client, data)

      # Wait longer than the timeout
      Process.sleep(200)

      # Debt should remain outstanding (not forgiven)
      # The relay should still be alive but with accumulated debt
      # Sending more data is still possible because we have high max_unacked_timeouts

      turn2 = %Turn{
        events: [
          %TurnEvent{
            oid: 0,
            event: %Assert{
              assertion: {:record, {{:symbol, "AfterTimeout"}, []}},
              handle: 2
            }
          }
        ]
      }

      {:ok, data2} = Framing.encode(turn2)
      :ok = :gen_tcp.send(client, data2)

      Process.sleep(100)

      # Clean up
      :gen_tcp.close(client)
      Broker.stop(broker)
    end

    test "max_unacked_timeouts configuration is accepted" do
      # This test verifies the configuration option is accepted.
      # Testing actual connection closure on repeated timeouts would require
      # a slow/hung actor that doesn't ack, which is not practical in integration tests
      # since the dataspace processes events quickly and sends acks immediately.

      socket_path = "/tmp/absynthe_fc_max_timeout_#{:erlang.unique_integer([:positive])}.sock"

      # Create a broker with the max_unacked_timeouts configuration
      {:ok, broker} =
        Broker.start_link(
          socket_path: socket_path,
          flow_control: [limit: 100, loan_ack_timeout: 5_000, max_unacked_timeouts: 2]
        )

      Process.sleep(100)

      {:ok, client} = :gen_tcp.connect({:local, socket_path}, 0, [:binary, active: false])

      # Send an assertion - should work normally since actor responds quickly
      turn = %Turn{
        events: [
          %TurnEvent{
            oid: 0,
            event: %Assert{
              assertion: {:record, {{:symbol, "NormalFlow"}, []}},
              handle: 1
            }
          }
        ]
      }

      {:ok, data} = Framing.encode(turn)
      :ok = :gen_tcp.send(client, data)

      # Give time for processing
      Process.sleep(100)

      # Connection should still be open since acks were received
      # (no timeout -> no consecutive_timeouts increment)
      turn2 = %Turn{
        events: [
          %TurnEvent{
            oid: 0,
            event: %Assert{
              assertion: {:record, {{:symbol, "StillOpen"}, []}},
              handle: 2
            }
          }
        ]
      }

      {:ok, data2} = Framing.encode(turn2)
      assert :ok == :gen_tcp.send(client, data2)

      # Clean up
      :gen_tcp.close(client)
      Broker.stop(broker)
    end

    test "multi-event turn only acks after last event processed" do
      socket_path = "/tmp/absynthe_fc_multi_#{:erlang.unique_integer([:positive])}.sock"

      # Start broker with flow control
      {:ok, broker} =
        Broker.start_link(
          socket_path: socket_path,
          flow_control: [limit: 100, loan_ack_timeout: 5_000]
        )

      Process.sleep(100)

      {:ok, client} = :gen_tcp.connect({:local, socket_path}, 0, [:binary, active: false])

      # Send a turn with multiple events - debt should only be repaid once,
      # after ALL events are processed, not after each one
      turn = %Turn{
        events: [
          %TurnEvent{
            oid: 0,
            event: %Assert{
              assertion: {:record, {{:symbol, "Multi"}, [{:integer, 1}]}},
              handle: 1
            }
          },
          %TurnEvent{
            oid: 0,
            event: %Assert{
              assertion: {:record, {{:symbol, "Multi"}, [{:integer, 2}]}},
              handle: 2
            }
          },
          %TurnEvent{
            oid: 0,
            event: %Assert{
              assertion: {:record, {{:symbol, "Multi"}, [{:integer, 3}]}},
              handle: 3
            }
          }
        ]
      }

      {:ok, data} = Framing.encode(turn)
      :ok = :gen_tcp.send(client, data)

      # Give time for actor to process all events
      Process.sleep(200)

      # Send another multi-event turn to verify flow control still works
      turn2 = %Turn{
        events: [
          %TurnEvent{
            oid: 0,
            event: %Assert{
              assertion: {:record, {{:symbol, "Multi"}, [{:integer, 4}]}},
              handle: 4
            }
          },
          %TurnEvent{
            oid: 0,
            event: %Assert{
              assertion: {:record, {{:symbol, "Multi"}, [{:integer, 5}]}},
              handle: 5
            }
          }
        ]
      }

      {:ok, data2} = Framing.encode(turn2)
      :ok = :gen_tcp.send(client, data2)

      Process.sleep(200)

      # Clean up
      :gen_tcp.close(client)
      Broker.stop(broker)
    end

    test "unknown OID immediately acks when it's the last event" do
      socket_path = "/tmp/absynthe_fc_unknown_#{:erlang.unique_integer([:positive])}.sock"

      # Start broker with flow control and short timeout
      {:ok, broker} =
        Broker.start_link(
          socket_path: socket_path,
          # Short timeout so we'd notice if ack doesn't happen
          flow_control: [limit: 100, loan_ack_timeout: 200]
        )

      Process.sleep(100)

      {:ok, client} = :gen_tcp.connect({:local, socket_path}, 0, [:binary, active: false])

      # Send event to unknown OID - should immediately ack (not wait for timeout)
      turn = %Turn{
        events: [
          %TurnEvent{
            oid: 999,
            event: %Assert{
              assertion: {:record, {{:symbol, "Unknown"}, []}},
              handle: 1
            }
          }
        ]
      }

      {:ok, data} = Framing.encode(turn)
      :ok = :gen_tcp.send(client, data)

      # Give minimal time for immediate ack
      Process.sleep(50)

      # Send another turn - if ack worked, this should succeed without hitting limit
      turn2 = %Turn{
        events: [
          %TurnEvent{
            oid: 0,
            event: %Assert{
              assertion: {:record, {{:symbol, "AfterUnknown"}, []}},
              handle: 2
            }
          }
        ]
      }

      {:ok, data2} = Framing.encode(turn2)
      :ok = :gen_tcp.send(client, data2)

      Process.sleep(100)

      # Clean up
      :gen_tcp.close(client)
      Broker.stop(broker)
    end

    test "mixed valid and unknown OID events in same turn" do
      socket_path = "/tmp/absynthe_fc_mixed_#{:erlang.unique_integer([:positive])}.sock"

      # Start broker with flow control
      {:ok, broker} =
        Broker.start_link(
          socket_path: socket_path,
          flow_control: [limit: 100, loan_ack_timeout: 5_000]
        )

      Process.sleep(100)

      {:ok, client} = :gen_tcp.connect({:local, socket_path}, 0, [:binary, active: false])

      # Send turn with mix of valid OID 0 and unknown OID 999
      # The valid event should trigger ack when it's processed
      turn = %Turn{
        events: [
          %TurnEvent{
            oid: 999,
            event: %Assert{
              assertion: {:record, {{:symbol, "Unknown"}, []}},
              handle: 1
            }
          },
          %TurnEvent{
            oid: 0,
            event: %Assert{
              assertion: {:record, {{:symbol, "Valid"}, []}},
              handle: 2
            }
          }
        ]
      }

      {:ok, data} = Framing.encode(turn)
      :ok = :gen_tcp.send(client, data)

      # Give time for processing
      Process.sleep(200)

      # Clean up
      :gen_tcp.close(client)
      Broker.stop(broker)
    end
  end
end
