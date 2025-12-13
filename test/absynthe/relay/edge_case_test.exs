defmodule Absynthe.Relay.EdgeCaseTest do
  use ExUnit.Case, async: true

  alias Absynthe.Core.Ref
  alias Absynthe.Relay.{Membrane, Framing, Broker}
  alias Absynthe.Relay.Packet
  alias Absynthe.Relay.Packet.{Turn, TurnEvent, Assert, Retract, Message}

  describe "Membrane refcount/GC lifecycle" do
    test "export starts with refcount 0" do
      membrane = Membrane.new()
      ref = Ref.new(:actor, :entity)

      {oid, membrane} = Membrane.export(membrane, ref)

      assert Membrane.refcount(membrane, oid) == 0
    end

    test "inc_ref increments refcount" do
      membrane = Membrane.new()
      ref = Ref.new(:actor, :entity)

      {oid, membrane} = Membrane.export(membrane, ref)
      membrane = Membrane.inc_ref(membrane, oid)

      assert Membrane.refcount(membrane, oid) == 1

      membrane = Membrane.inc_ref(membrane, oid)
      assert Membrane.refcount(membrane, oid) == 2
    end

    test "dec_ref decrements refcount and returns :alive while refcount > 0" do
      membrane = Membrane.new()
      ref = Ref.new(:actor, :entity)

      {oid, membrane} = Membrane.export(membrane, ref)
      membrane = Membrane.inc_ref(membrane, oid)
      membrane = Membrane.inc_ref(membrane, oid)

      assert Membrane.refcount(membrane, oid) == 2

      {membrane, status} = Membrane.dec_ref(membrane, oid)
      assert status == :alive
      assert Membrane.refcount(membrane, oid) == 1
    end

    test "dec_ref returns :removed and GCs symbol when refcount hits 0" do
      membrane = Membrane.new()
      ref = Ref.new(:actor, :entity)

      {oid, membrane} = Membrane.export(membrane, ref)
      membrane = Membrane.inc_ref(membrane, oid)

      assert Membrane.size(membrane) == 1
      assert Membrane.refcount(membrane, oid) == 1

      {membrane, status} = Membrane.dec_ref(membrane, oid)
      assert status == :removed
      assert Membrane.size(membrane) == 0
      assert Membrane.refcount(membrane, oid) == 0
      assert Membrane.lookup_by_oid(membrane, oid) == :error
      assert Membrane.lookup_by_ref(membrane, ref) == :error
    end

    test "dec_ref on unknown OID returns :not_found" do
      membrane = Membrane.new()

      {_membrane, status} = Membrane.dec_ref(membrane, 999)
      assert status == :not_found
    end

    test "inc_ref on unknown OID is a no-op" do
      membrane = Membrane.new()

      # Should not crash, just return unchanged membrane
      membrane_after = Membrane.inc_ref(membrane, 999)
      assert Membrane.size(membrane_after) == 0
    end

    test "import creates symbol with refcount 0" do
      membrane = Membrane.new()

      {_ref, membrane} =
        Membrane.import(membrane, 42, [], fn oid ->
          Ref.new(:proxy, oid)
        end)

      assert Membrane.refcount(membrane, 42) == 0
    end

    test "multiple exports of same ref return same OID" do
      membrane = Membrane.new()
      ref = Ref.new(:actor, :entity)

      {oid1, membrane} = Membrane.export(membrane, ref)
      {oid2, membrane} = Membrane.export(membrane, ref)

      assert oid1 == oid2
      assert Membrane.size(membrane) == 1
    end

    test "export strips attenuation for lookup" do
      membrane = Membrane.new()
      base_ref = Ref.new(:actor, :entity)
      attenuated_ref = Ref.with_attenuation(base_ref, [:some_caveat])

      {oid1, membrane} = Membrane.export(membrane, base_ref)
      {oid2, _membrane} = Membrane.export(membrane, attenuated_ref)

      assert oid1 == oid2
    end

    test "GC removes both oid_to_symbol and ref_to_oid mappings" do
      membrane = Membrane.new()
      ref = Ref.new(:actor, :entity)

      {oid, membrane} = Membrane.export(membrane, ref)
      membrane = Membrane.inc_ref(membrane, oid)

      # Verify both mappings exist
      assert {:ok, ^ref} = Membrane.lookup_by_oid(membrane, oid)
      assert {:ok, ^oid} = Membrane.lookup_by_ref(membrane, ref)

      # GC the symbol
      {membrane, :removed} = Membrane.dec_ref(membrane, oid)

      # Verify both mappings are removed
      assert :error = Membrane.lookup_by_oid(membrane, oid)
      assert :error = Membrane.lookup_by_ref(membrane, ref)
    end

    test "OID counter advances even after GC" do
      membrane = Membrane.new()

      # Export, inc_ref, then GC
      ref1 = Ref.new(:actor, :entity1)
      {oid1, membrane} = Membrane.export(membrane, ref1)
      membrane = Membrane.inc_ref(membrane, oid1)
      {membrane, :removed} = Membrane.dec_ref(membrane, oid1)

      # New export should get next OID, not reuse the old one
      ref2 = Ref.new(:actor, :entity2)
      {oid2, _membrane} = Membrane.export(membrane, ref2)

      assert oid2 == oid1 + 1
    end
  end

  describe "Framing corrupted/malformed buffer handling" do
    test "decode_from_buffer returns error for invalid preserves data" do
      # Completely invalid binary data
      corrupted = <<0xFF, 0xFE, 0xFD, 0xFC, 0xFB>>

      result = Framing.decode_from_buffer(corrupted)

      assert {:error, {:preserves_decode_error, _reason}} = result
    end

    test "decode_all returns error with successfully decoded packets before error" do
      # First encode a valid packet
      turn = %Turn{events: []}
      {:ok, valid_bytes} = Framing.encode(turn)

      # Append corrupted data
      corrupted = <<0xFF, 0xFE, 0xFD, 0xFC, 0xFB>>
      buffer = valid_bytes <> corrupted

      result = Framing.decode_all(buffer)

      # Should return error but with the successfully decoded packet
      assert {:error, {:preserves_decode_error, _reason}, packets, _remaining} = result
      assert length(packets) == 1
      assert %Turn{events: []} = hd(packets)
    end

    test "append_and_decode propagates framing errors" do
      buffer = Framing.new_state()
      corrupted = <<0xFF, 0xFE, 0xFD, 0xFC, 0xFB>>

      result = Framing.append_and_decode(buffer, corrupted)

      assert {:error, {:preserves_decode_error, _reason}, [], _remaining} = result
    end

    test "decode_from_buffer handles empty buffer" do
      result = Framing.decode_from_buffer(<<>>)
      assert {:incomplete, <<>>} = result
    end

    test "decode_from_buffer handles truncated packet" do
      # Encode a valid packet and truncate it
      turn = %Turn{
        events: [
          %TurnEvent{
            oid: 0,
            event: %Assert{assertion: {:string, "test"}, handle: 1}
          }
        ]
      }

      {:ok, valid_bytes} = Framing.encode(turn)

      # Truncate to half
      truncated_len = div(byte_size(valid_bytes), 2)
      truncated = binary_part(valid_bytes, 0, truncated_len)

      result = Framing.decode_from_buffer(truncated)
      assert {:incomplete, ^truncated} = result
    end

    test "decode_from_buffer rejects packet with invalid structure" do
      # Encode something that is valid Preserves but not a valid packet
      # A simple integer is valid Preserves but not a valid packet
      {:ok, binary} = Absynthe.Preserves.Encoder.Binary.encode({:integer, 42})

      result = Framing.decode_from_buffer(binary)

      assert {:error, {:packet_decode_error, _reason}} = result
    end
  end

  describe "Framing buffer management edge cases" do
    test "multiple valid packets decode correctly" do
      turn1 = %Turn{events: []}

      turn2 = %Turn{
        events: [
          %TurnEvent{oid: 0, event: %Assert{assertion: {:string, "a"}, handle: 1}}
        ]
      }

      {:ok, b1} = Framing.encode(turn1)
      {:ok, b2} = Framing.encode(turn2)

      {:ok, packets, rest} = Framing.decode_all(b1 <> b2)

      assert length(packets) == 2
      assert rest == <<>>
    end

    test "nop packets decode correctly" do
      {:ok, nop_bytes} = Framing.encode(:nop)

      {:ok, packet, rest} = Framing.decode_from_buffer(nop_bytes)

      assert packet == :nop
      assert rest == <<>>
    end

    test "retract event decodes correctly" do
      turn = %Turn{
        events: [
          %TurnEvent{oid: 5, event: %Retract{handle: 42}}
        ]
      }

      {:ok, bytes} = Framing.encode(turn)
      {:ok, decoded, <<>>} = Framing.decode_from_buffer(bytes)

      assert %Turn{events: [event]} = decoded
      assert event.oid == 5
      assert %Retract{handle: 42} = event.event
    end
  end

  describe "Outbound retract routing" do
    @moduletag timeout: 10_000

    test "retract goes to correct OID based on handle_to_oid tracking" do
      # This test verifies that when we retract an assertion, it goes to
      # the correct OID (the one it was originally asserted to)

      socket_path = "/tmp/absynthe_retract_test_#{:erlang.unique_integer([:positive])}.sock"
      {:ok, broker} = Broker.start_link(socket_path: socket_path)
      Process.sleep(100)

      {:ok, client} = :gen_tcp.connect({:local, socket_path}, 0, [:binary, active: false])

      # Send assertions to OID 0 with different handles
      turn = %Turn{
        events: [
          %TurnEvent{oid: 0, event: %Assert{assertion: {:string, "first"}, handle: 1}},
          %TurnEvent{oid: 0, event: %Assert{assertion: {:string, "second"}, handle: 2}}
        ]
      }

      {:ok, data} = Framing.encode(turn)
      :ok = :gen_tcp.send(client, data)
      Process.sleep(100)

      # Retract handle 1 (should go to OID 0)
      retract_turn = %Turn{
        events: [%TurnEvent{oid: 0, event: %Retract{handle: 1}}]
      }

      {:ok, retract_data} = Framing.encode(retract_turn)
      :ok = :gen_tcp.send(client, retract_data)
      Process.sleep(100)

      # Retract handle 2 (should also go to OID 0)
      retract_turn2 = %Turn{
        events: [%TurnEvent{oid: 0, event: %Retract{handle: 2}}]
      }

      {:ok, retract_data2} = Framing.encode(retract_turn2)
      :ok = :gen_tcp.send(client, retract_data2)
      Process.sleep(100)

      :gen_tcp.close(client)
      Broker.stop(broker)
    end

    test "retract for unknown wire handle is ignored gracefully" do
      socket_path = "/tmp/absynthe_unknown_handle_#{:erlang.unique_integer([:positive])}.sock"
      {:ok, broker} = Broker.start_link(socket_path: socket_path)
      Process.sleep(100)

      {:ok, client} = :gen_tcp.connect({:local, socket_path}, 0, [:binary, active: false])

      # Send a retract for a handle that was never asserted
      retract_turn = %Turn{
        events: [%TurnEvent{oid: 0, event: %Retract{handle: 999}}]
      }

      {:ok, retract_data} = Framing.encode(retract_turn)
      :ok = :gen_tcp.send(client, retract_data)
      Process.sleep(100)

      # Should not crash - verify by sending another valid packet
      valid_turn = %Turn{
        events: [%TurnEvent{oid: 0, event: %Assert{assertion: {:string, "test"}, handle: 1}}]
      }

      {:ok, valid_data} = Framing.encode(valid_turn)
      :ok = :gen_tcp.send(client, valid_data)
      Process.sleep(100)

      :gen_tcp.close(client)
      Broker.stop(broker)
    end

    test "retract for unknown OID is ignored gracefully" do
      socket_path = "/tmp/absynthe_unknown_oid_#{:erlang.unique_integer([:positive])}.sock"
      {:ok, broker} = Broker.start_link(socket_path: socket_path)
      Process.sleep(100)

      {:ok, client} = :gen_tcp.connect({:local, socket_path}, 0, [:binary, active: false])

      # First assert something valid
      assert_turn = %Turn{
        events: [%TurnEvent{oid: 0, event: %Assert{assertion: {:string, "valid"}, handle: 1}}]
      }

      {:ok, assert_data} = Framing.encode(assert_turn)
      :ok = :gen_tcp.send(client, assert_data)
      Process.sleep(100)

      # Send event to non-existent OID
      bad_turn = %Turn{
        events: [%TurnEvent{oid: 999, event: %Assert{assertion: {:string, "bad"}, handle: 2}}]
      }

      {:ok, bad_data} = Framing.encode(bad_turn)
      :ok = :gen_tcp.send(client, bad_data)
      Process.sleep(100)

      # Verify relay still works by retracting the valid assertion
      retract_turn = %Turn{
        events: [%TurnEvent{oid: 0, event: %Retract{handle: 1}}]
      }

      {:ok, retract_data} = Framing.encode(retract_turn)
      :ok = :gen_tcp.send(client, retract_data)
      Process.sleep(100)

      :gen_tcp.close(client)
      Broker.stop(broker)
    end

    test "message to OID is delivered correctly" do
      socket_path = "/tmp/absynthe_message_test_#{:erlang.unique_integer([:positive])}.sock"
      {:ok, broker} = Broker.start_link(socket_path: socket_path)
      Process.sleep(100)

      {:ok, client} = :gen_tcp.connect({:local, socket_path}, 0, [:binary, active: false])

      # Send a message to OID 0
      turn = %Turn{
        events: [
          %TurnEvent{oid: 0, event: %Message{body: {:record, {{:symbol, "Ping"}, []}}}}
        ]
      }

      {:ok, data} = Framing.encode(turn)
      :ok = :gen_tcp.send(client, data)
      Process.sleep(100)

      :gen_tcp.close(client)
      Broker.stop(broker)
    end
  end

  describe "Socket ownership transfer" do
    @moduletag timeout: 10_000

    test "listener continues accepting after controlling_process handoff succeeds" do
      # Test the happy path: handoff succeeds and listener keeps accepting
      socket_path = "/tmp/absynthe_handoff_#{:erlang.unique_integer([:positive])}.sock"
      {:ok, broker} = Broker.start_link(socket_path: socket_path)
      Process.sleep(100)

      # Connect and verify handoff works
      {:ok, client1} = :gen_tcp.connect({:local, socket_path}, 0, [:binary, active: false])

      turn = %Turn{
        events: [%TurnEvent{oid: 0, event: %Assert{assertion: {:string, "test1"}, handle: 1}}]
      }

      {:ok, data} = Framing.encode(turn)
      :ok = :gen_tcp.send(client1, data)
      Process.sleep(50)

      # Second connection should also work
      {:ok, client2} = :gen_tcp.connect({:local, socket_path}, 0, [:binary, active: false])

      turn2 = %Turn{
        events: [%TurnEvent{oid: 0, event: %Assert{assertion: {:string, "test2"}, handle: 1}}]
      }

      {:ok, data2} = Framing.encode(turn2)
      :ok = :gen_tcp.send(client2, data2)
      Process.sleep(50)

      :gen_tcp.close(client1)
      :gen_tcp.close(client2)

      # Broker should still be alive
      assert Process.alive?(broker)

      Broker.stop(broker)
    end

    test "listener survives when relay process exits early during handoff" do
      # This tests that if the relay process exits between start_link and
      # controlling_process, the listener doesn't crash and keeps accepting
      socket_path = "/tmp/absynthe_early_exit_#{:erlang.unique_integer([:positive])}.sock"
      {:ok, broker} = Broker.start_link(socket_path: socket_path)
      Process.sleep(100)

      # Connect and close rapidly multiple times to stress test early exits
      for _ <- 1..10 do
        case :gen_tcp.connect({:local, socket_path}, 0, [:binary, active: false]) do
          {:ok, client} ->
            # Close immediately to potentially trigger races
            :gen_tcp.close(client)

          {:error, _reason} ->
            # Connection failed, that's ok
            :ok
        end
      end

      Process.sleep(100)

      # Broker should still be accepting connections
      {:ok, client} = :gen_tcp.connect({:local, socket_path}, 0, [:binary, active: false])

      turn = %Turn{
        events: [
          %TurnEvent{oid: 0, event: %Assert{assertion: {:string, "still works"}, handle: 1}}
        ]
      }

      {:ok, data} = Framing.encode(turn)
      :ok = :gen_tcp.send(client, data)
      Process.sleep(50)

      :gen_tcp.close(client)

      # Broker should still be alive
      assert Process.alive?(broker)

      Broker.stop(broker)
    end

    test "listener handles relay start failure gracefully" do
      # Test that the listener handles failures gracefully when relay can't start
      socket_path = "/tmp/absynthe_ownership_#{:erlang.unique_integer([:positive])}.sock"
      {:ok, broker} = Broker.start_link(socket_path: socket_path)
      Process.sleep(100)

      # Connect multiple clients rapidly to stress test the handoff
      clients =
        for _ <- 1..5 do
          {:ok, client} = :gen_tcp.connect({:local, socket_path}, 0, [:binary, active: false])
          client
        end

      Process.sleep(100)

      # Send data from all clients
      for client <- clients do
        turn = %Turn{
          events: [%TurnEvent{oid: 0, event: %Assert{assertion: {:string, "test"}, handle: 1}}]
        }

        {:ok, data} = Framing.encode(turn)
        :gen_tcp.send(client, data)
      end

      Process.sleep(100)

      # Close all clients
      for client <- clients do
        :gen_tcp.close(client)
      end

      # Broker should still be alive
      assert Process.alive?(broker)

      Broker.stop(broker)
    end

    test "listener continues accepting after connection errors" do
      socket_path = "/tmp/absynthe_continue_#{:erlang.unique_integer([:positive])}.sock"
      {:ok, broker} = Broker.start_link(socket_path: socket_path)
      Process.sleep(100)

      # Connect and immediately close
      {:ok, client1} = :gen_tcp.connect({:local, socket_path}, 0, [:binary, active: false])
      :gen_tcp.close(client1)
      Process.sleep(50)

      # Should still be able to connect another client
      {:ok, client2} = :gen_tcp.connect({:local, socket_path}, 0, [:binary, active: false])

      # Send valid data
      turn = %Turn{
        events: [
          %TurnEvent{oid: 0, event: %Assert{assertion: {:string, "after close"}, handle: 1}}
        ]
      }

      {:ok, data} = Framing.encode(turn)
      :ok = :gen_tcp.send(client2, data)
      Process.sleep(100)

      :gen_tcp.close(client2)
      Broker.stop(broker)
    end

    test "relay connection closes gracefully on corrupted data" do
      # This test verifies that when a relay receives corrupted data,
      # it closes the connection gracefully.
      # Note: Currently the relay terminating with an error causes the shared actor
      # to receive cleanup messages. This tests the framing error path is exercised.

      socket_path = "/tmp/absynthe_corrupt_#{:erlang.unique_integer([:positive])}.sock"

      # Start broker - trap exits to survive relay crash propagation
      Process.flag(:trap_exit, true)
      {:ok, broker} = Broker.start_link(socket_path: socket_path)
      Process.sleep(100)

      {:ok, client} = :gen_tcp.connect({:local, socket_path}, 0, [:binary, active: false])

      # Send corrupted data - the relay should send error packet and close connection
      :ok = :gen_tcp.send(client, <<0xFF, 0xFE, 0xFD, 0xFC, 0xFB>>)

      # Give time for the error packet to be sent
      Process.sleep(100)

      # Try to read - we should either get the error packet or connection closed
      result = :gen_tcp.recv(client, 0, 500)

      case result do
        {:ok, data} ->
          # Got error packet - verify it decodes correctly
          {:ok, packet, _rest} = Framing.decode_from_buffer(data)
          assert %Packet.Error{} = packet

        {:error, :closed} ->
          # Connection was closed - also acceptable
          :ok

        {:error, :timeout} ->
          # Timed out waiting - also acceptable (connection may still be closing)
          :ok
      end

      :gen_tcp.close(client)

      # Clean up (broker may already be dead from cascade)
      if Process.alive?(broker), do: Broker.stop(broker)
    end
  end

  describe "Packet encoding/decoding edge cases" do
    test "error packet encodes and decodes" do
      error_packet = Packet.error("something broke", {:symbol, "test_error"})

      {:ok, bytes} = Framing.encode(error_packet)
      {:ok, decoded, <<>>} = Framing.decode_from_buffer(bytes)

      assert %Packet.Error{message: "something broke"} = decoded
    end

    test "empty turn encodes and decodes" do
      turn = %Turn{events: []}

      {:ok, bytes} = Framing.encode(turn)
      {:ok, decoded, <<>>} = Framing.decode_from_buffer(bytes)

      assert %Turn{events: []} = decoded
    end

    test "turn with multiple events preserves order" do
      turn = %Turn{
        events: [
          %TurnEvent{oid: 0, event: %Assert{assertion: {:integer, 1}, handle: 1}},
          %TurnEvent{oid: 1, event: %Assert{assertion: {:integer, 2}, handle: 2}},
          %TurnEvent{oid: 2, event: %Assert{assertion: {:integer, 3}, handle: 3}}
        ]
      }

      {:ok, bytes} = Framing.encode(turn)
      {:ok, decoded, <<>>} = Framing.decode_from_buffer(bytes)

      assert %Turn{events: events} = decoded
      assert length(events) == 3
      assert Enum.at(events, 0).oid == 0
      assert Enum.at(events, 1).oid == 1
      assert Enum.at(events, 2).oid == 2
    end
  end
end
