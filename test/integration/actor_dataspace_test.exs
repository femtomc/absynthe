defmodule Absynthe.Integration.ActorDataspaceTest do
  use ExUnit.Case

  alias Absynthe.Core.{Actor, Turn, Entity, Ref}
  alias Absynthe.Dataspace.Dataspace
  alias Absynthe.Assertions.Handle
  alias Absynthe.Protocol.Event

  # A simple observer entity that collects received assertions and messages
  defmodule CollectorEntity do
    defstruct received: []

    defimpl Entity do
      def on_message(entity, message, turn) do
        updated = %{entity | received: [{:message, message} | entity.received]}
        {updated, turn}
      end

      def on_publish(entity, assertion, handle, turn) do
        updated = %{entity | received: [{:assert, assertion, handle} | entity.received]}
        {updated, turn}
      end

      def on_retract(entity, handle, turn) do
        updated = %{entity | received: [{:retract, handle} | entity.received]}
        {updated, turn}
      end

      def on_sync(entity, peer, turn) do
        # On sync, send a message back to the peer
        action = Event.message(peer, {:symbol, "synced"})
        turn = Turn.add_action(turn, action)
        {entity, turn}
      end
    end
  end

  # An entity that forwards messages to another entity
  defmodule ForwarderEntity do
    defstruct [:target_ref]

    defimpl Entity do
      def on_message(%{target_ref: target} = entity, message, turn) do
        # Forward the message to target
        action = Event.message(target, message)
        turn = Turn.add_action(turn, action)
        {entity, turn}
      end

      def on_publish(entity, _assertion, _handle, turn), do: {entity, turn}
      def on_retract(entity, _handle, turn), do: {entity, turn}
      def on_sync(entity, _peer, turn), do: {entity, turn}
    end
  end

  describe "actor message routing" do
    test "message forwarding between entities in same actor" do
      {:ok, actor} = Actor.start_link(id: :forward_test)

      # Create collector
      collector = %CollectorEntity{}
      {:ok, collector_ref} = Actor.spawn_entity(actor, :root, collector)

      # Create forwarder pointing to collector
      forwarder = %ForwarderEntity{target_ref: collector_ref}
      {:ok, forwarder_ref} = Actor.spawn_entity(actor, :root, forwarder)

      # Send message to forwarder
      Actor.send_message(actor, forwarder_ref, {:string, "hello"})

      # Give time for processing
      Process.sleep(50)

      # Get updated collector state
      state = :sys.get_state(actor)
      {_facet_id, updated_collector} = Map.get(state.entities, Ref.entity_id(collector_ref))

      # Collector should have received the forwarded message
      assert [{:message, {:string, "hello"}}] = updated_collector.received

      GenServer.stop(actor)
    end

    test "assertion routing between entities" do
      {:ok, actor} = Actor.start_link(id: :assert_test)

      # Create collector
      collector = %CollectorEntity{}
      {:ok, collector_ref} = Actor.spawn_entity(actor, :root, collector)

      # Assert to collector
      {:ok, handle} = Actor.assert(actor, collector_ref, {:string, "test_assertion"})

      # Give time for processing
      Process.sleep(50)

      # Get updated collector state
      state = :sys.get_state(actor)
      {_facet_id, updated_collector} = Map.get(state.entities, Ref.entity_id(collector_ref))

      # Collector should have received the assertion
      assert [{:assert, {:string, "test_assertion"}, ^handle}] = updated_collector.received

      GenServer.stop(actor)
    end

    test "retraction routing between entities" do
      {:ok, actor} = Actor.start_link(id: :retract_test)

      # Create collector
      collector = %CollectorEntity{}
      {:ok, collector_ref} = Actor.spawn_entity(actor, :root, collector)

      # Assert and then retract
      {:ok, handle} = Actor.assert(actor, collector_ref, {:string, "temp"})
      Process.sleep(20)
      :ok = Actor.retract(actor, handle)
      Process.sleep(50)

      # Get updated collector state
      state = :sys.get_state(actor)
      {_facet_id, updated_collector} = Map.get(state.entities, Ref.entity_id(collector_ref))

      # Collector should have received both assert and retract (most recent first)
      assert [{:retract, ^handle}, {:assert, {:string, "temp"}, ^handle}] =
               updated_collector.received

      GenServer.stop(actor)
    end

    test "sync triggers response" do
      {:ok, actor} = Actor.start_link(id: :sync_test)

      # Create two collectors - one to sync with, one to receive the response
      syncer = %CollectorEntity{}
      {:ok, syncer_ref} = Actor.spawn_entity(actor, :root, syncer)

      responder = %CollectorEntity{}
      {:ok, responder_ref} = Actor.spawn_entity(actor, :root, responder)

      # Send sync to responder, expecting response at syncer
      Actor.sync(actor, responder_ref, syncer_ref)

      # Give time for processing
      Process.sleep(50)

      # Get updated syncer state
      state = :sys.get_state(actor)
      {_facet_id, updated_syncer} = Map.get(state.entities, Ref.entity_id(syncer_ref))

      # Syncer should have received the sync response message
      assert [{:message, {:symbol, "synced"}}] = updated_syncer.received

      GenServer.stop(actor)
    end
  end

  describe "dataspace entity" do
    test "dataspace stores assertions" do
      # Create a dataspace entity
      dataspace = Dataspace.new()

      # Simulate assertion handling via Entity protocol
      handle = Handle.new(:test_actor, 1)
      turn = Turn.new(:test_actor, :test_facet)

      {updated_dataspace, _turn} =
        Entity.on_publish(dataspace, {:string, "hello"}, handle, turn)

      # Dataspace should contain the assertion
      assert Dataspace.has_assertion?(updated_dataspace, {:string, "hello"})
      assert [_] = Dataspace.assertions(updated_dataspace)
    end

    test "dataspace removes assertions on retract" do
      dataspace = Dataspace.new()
      handle = Handle.new(:test_actor, 1)
      turn = Turn.new(:test_actor, :test_facet)

      # Add assertion
      {dataspace, turn} =
        Entity.on_publish(dataspace, {:string, "temp"}, handle, turn)

      assert Dataspace.has_assertion?(dataspace, {:string, "temp"})

      # Retract assertion
      {dataspace, _turn} = Entity.on_retract(dataspace, handle, turn)

      refute Dataspace.has_assertion?(dataspace, {:string, "temp"})
    end

    test "dataspace sync responds with synced message" do
      # Test that dataspace.on_sync sends a message (not another sync)
      dataspace = Dataspace.new()
      turn = Turn.new(:ds_sync_test, :test_facet)
      peer_ref = %Ref{actor_id: :ds_sync_test, entity_id: 42, attenuation: nil}

      {_dataspace, updated_turn} = Entity.on_sync(dataspace, peer_ref, turn)

      # The turn should contain exactly one message action to the peer
      # with {:symbol, "synced"} as the body
      [action] = Turn.actions(updated_turn)
      assert %Event.Message{ref: ^peer_ref, body: {:symbol, "synced"}} = action
    end
  end

  describe "sync ordering guarantees" do
    test "default entity sync responds with synced message" do
      {:ok, actor} = Actor.start_link(id: :default_sync_test)

      # Create a collector to receive the sync response
      collector = %CollectorEntity{}
      {:ok, collector_ref} = Actor.spawn_entity(actor, :root, collector)

      # Use Entity.Default which relies on the default on_sync callback
      # that sends the synced message to the peer
      default_entity = %Entity.Default{
        on_message: fn entity, _msg, turn ->
          {entity, turn}
        end
      }

      {:ok, default_ref} = Actor.spawn_entity(actor, :root, default_entity)

      # Sync with default entity, expecting response at collector
      Actor.sync(actor, default_ref, collector_ref)

      Process.sleep(50)

      # Collector should have received the synced message from default behavior
      state = :sys.get_state(actor)
      {_facet_id, updated_collector} = Map.get(state.entities, Ref.entity_id(collector_ref))

      assert [{:message, {:symbol, "synced"}}] = updated_collector.received

      GenServer.stop(actor)
    end

    test "sync creates happens-before relationship" do
      # This test verifies that when we sync, all prior events are processed
      # before the sync response is received
      {:ok, actor} = Actor.start_link(id: :ordering_test)

      # Create a collector that tracks events in order
      collector = %CollectorEntity{}
      {:ok, collector_ref} = Actor.spawn_entity(actor, :root, collector)

      # Create a responder entity
      responder = %CollectorEntity{}
      {:ok, responder_ref} = Actor.spawn_entity(actor, :root, responder)

      # Send multiple messages, then sync
      Actor.send_message(actor, responder_ref, {:string, "msg1"})
      Actor.send_message(actor, responder_ref, {:string, "msg2"})
      Actor.sync(actor, responder_ref, collector_ref)

      Process.sleep(50)

      # Responder should have received all messages
      state = :sys.get_state(actor)
      {_facet_id, updated_responder} = Map.get(state.entities, Ref.entity_id(responder_ref))

      # Messages are prepended, so most recent first
      assert [{:message, {:string, "msg2"}}, {:message, {:string, "msg1"}}] =
               updated_responder.received

      # Collector should have received the sync response
      {_facet_id, updated_collector} = Map.get(state.entities, Ref.entity_id(collector_ref))
      assert [{:message, {:symbol, "synced"}}] = updated_collector.received

      GenServer.stop(actor)
    end
  end

  describe "cross-actor messaging" do
    test "messages route between actors" do
      # Start two actors
      {:ok, actor1} = Actor.start_link(id: :actor1, name: :actor1)
      {:ok, actor2} = Actor.start_link(id: :actor2, name: :actor2)

      # Create collector in actor2
      collector = %CollectorEntity{}
      {:ok, collector_ref} = Actor.spawn_entity(actor2, :root, collector)

      # Modify ref to point to actor2
      cross_actor_ref = %Ref{actor_id: :actor2, entity_id: Ref.entity_id(collector_ref)}

      # Create forwarder in actor1 that forwards to actor2's collector
      forwarder = %ForwarderEntity{target_ref: cross_actor_ref}
      {:ok, forwarder_ref} = Actor.spawn_entity(actor1, :root, forwarder)

      # Send message to forwarder in actor1
      Actor.send_message(actor1, forwarder_ref, {:string, "cross-actor"})

      # Give time for cross-actor routing
      Process.sleep(100)

      # Check collector in actor2 received the message
      state2 = :sys.get_state(actor2)
      {_facet_id, updated_collector} = Map.get(state2.entities, Ref.entity_id(collector_ref))

      assert [{:message, {:string, "cross-actor"}}] = updated_collector.received

      GenServer.stop(actor1)
      GenServer.stop(actor2)
    end

    test "assertions route between actors" do
      # Start two actors
      {:ok, actor1} = Actor.start_link(id: :assert_actor1, name: :assert_actor1)
      {:ok, actor2} = Actor.start_link(id: :assert_actor2, name: :assert_actor2)

      # Create collector in actor2
      collector = %CollectorEntity{}
      {:ok, collector_ref} = Actor.spawn_entity(actor2, :root, collector)

      # Create a cross-actor ref pointing to actor2's collector
      cross_actor_ref = %Ref{actor_id: :assert_actor2, entity_id: Ref.entity_id(collector_ref)}

      # Assert from actor1 to entity in actor2
      {:ok, handle} = Actor.assert(actor1, cross_actor_ref, {:string, "cross-actor-assertion"})

      # Give time for cross-actor routing
      Process.sleep(100)

      # Check collector in actor2 received the assertion
      state2 = :sys.get_state(actor2)
      {_facet_id, updated_collector} = Map.get(state2.entities, Ref.entity_id(collector_ref))

      assert [{:assert, {:string, "cross-actor-assertion"}, ^handle}] = updated_collector.received

      GenServer.stop(actor1)
      GenServer.stop(actor2)
    end

    test "retractions route between actors" do
      # Start two actors
      {:ok, actor1} = Actor.start_link(id: :retract_actor1, name: :retract_actor1)
      {:ok, actor2} = Actor.start_link(id: :retract_actor2, name: :retract_actor2)

      # Create collector in actor2
      collector = %CollectorEntity{}
      {:ok, collector_ref} = Actor.spawn_entity(actor2, :root, collector)

      # Create a cross-actor ref pointing to actor2's collector
      cross_actor_ref = %Ref{actor_id: :retract_actor2, entity_id: Ref.entity_id(collector_ref)}

      # Assert from actor1 to entity in actor2
      {:ok, handle} = Actor.assert(actor1, cross_actor_ref, {:string, "temp"})
      Process.sleep(50)

      # Now retract from actor1
      :ok = Actor.retract(actor1, handle)
      Process.sleep(100)

      # Check collector in actor2 received both assert and retract
      state2 = :sys.get_state(actor2)
      {_facet_id, updated_collector} = Map.get(state2.entities, Ref.entity_id(collector_ref))

      # Should have retract first (most recent), then assert
      assert [{:retract, ^handle}, {:assert, {:string, "temp"}, ^handle}] =
               updated_collector.received

      GenServer.stop(actor1)
      GenServer.stop(actor2)
    end
  end

  describe "crash cleanup" do
    test "assertions are auto-retracted when asserting actor crashes" do
      # Start two actors
      {:ok, receiver} = Actor.start_link(id: :crash_receiver, name: :crash_receiver)

      # Start asserter - unlink from test process so we can kill it without crashing the test
      {:ok, asserter} = Actor.start_link(id: :crash_asserter, name: :crash_asserter)
      Process.unlink(asserter)

      # Create collector in receiver
      collector = %CollectorEntity{}
      {:ok, collector_ref} = Actor.spawn_entity(receiver, :root, collector)

      # Create a cross-actor ref pointing to receiver's collector
      cross_actor_ref = %Ref{actor_id: :crash_receiver, entity_id: Ref.entity_id(collector_ref)}

      # Assert from asserter to receiver
      {:ok, handle} = Actor.assert(asserter, cross_actor_ref, {:string, "will-be-orphaned"})

      # Give time for assertion to arrive
      Process.sleep(100)

      # Verify assertion was received
      state = :sys.get_state(receiver)
      {_fid, collector_state} = Map.get(state.entities, Ref.entity_id(collector_ref))

      asserts =
        Enum.filter(collector_state.received, fn
          {:assert, _, _} -> true
          _ -> false
        end)

      assert length(asserts) == 1
      assert [{:assert, {:string, "will-be-orphaned"}, ^handle}] = asserts

      # Kill the asserter abruptly (simulating a crash, not normal termination)
      Process.exit(asserter, :kill)

      # Give time for DOWN message to be processed
      Process.sleep(100)

      # Verify retraction was auto-generated
      state = :sys.get_state(receiver)
      {_fid, collector_state} = Map.get(state.entities, Ref.entity_id(collector_ref))

      retracts =
        Enum.filter(collector_state.received, fn
          {:retract, _} -> true
          _ -> false
        end)

      assert length(retracts) == 1
      assert [{:retract, ^handle}] = retracts

      GenServer.stop(receiver)
    end

    test "multiple assertions from crashed actor are all retracted" do
      {:ok, receiver} = Actor.start_link(id: :multi_crash_receiver, name: :multi_crash_receiver)
      {:ok, asserter} = Actor.start_link(id: :multi_crash_asserter, name: :multi_crash_asserter)
      Process.unlink(asserter)

      # Create collector in receiver
      collector = %CollectorEntity{}
      {:ok, collector_ref} = Actor.spawn_entity(receiver, :root, collector)
      cross_ref = %Ref{actor_id: :multi_crash_receiver, entity_id: Ref.entity_id(collector_ref)}

      # Make multiple assertions
      {:ok, h1} = Actor.assert(asserter, cross_ref, {:string, "first"})
      {:ok, h2} = Actor.assert(asserter, cross_ref, {:string, "second"})
      {:ok, h3} = Actor.assert(asserter, cross_ref, {:string, "third"})

      Process.sleep(100)

      # Verify all assertions received
      state = :sys.get_state(receiver)
      {_fid, collector_state} = Map.get(state.entities, Ref.entity_id(collector_ref))

      asserts =
        Enum.filter(collector_state.received, fn
          {:assert, _, _} -> true
          _ -> false
        end)

      assert length(asserts) == 3

      # Kill the asserter
      Process.exit(asserter, :kill)
      Process.sleep(100)

      # Verify all assertions were retracted
      state = :sys.get_state(receiver)
      {_fid, collector_state} = Map.get(state.entities, Ref.entity_id(collector_ref))

      retracts =
        Enum.filter(collector_state.received, fn
          {:retract, _} -> true
          _ -> false
        end)

      assert length(retracts) == 3

      # Verify the correct handles were retracted
      retracted_handles = Enum.map(retracts, fn {:retract, h} -> h end) |> MapSet.new()
      assert MapSet.member?(retracted_handles, h1)
      assert MapSet.member?(retracted_handles, h2)
      assert MapSet.member?(retracted_handles, h3)

      GenServer.stop(receiver)
    end

    test "dataspace assertions are cleaned up on actor crash" do
      # This tests that assertions to a dataspace are properly retracted
      # when the asserting actor crashes

      {:ok, ds_actor} = Actor.start_link(id: :ds_crash_cleanup, name: :ds_crash_cleanup)
      {:ok, asserter} = Actor.start_link(id: :ds_asserter_crash, name: :ds_asserter_crash)
      Process.unlink(asserter)

      # Create dataspace entity
      dataspace = Dataspace.new()
      {:ok, ds_ref} = Actor.spawn_entity(ds_actor, :root, dataspace)
      ds_cross_ref = %Ref{actor_id: :ds_crash_cleanup, entity_id: Ref.entity_id(ds_ref)}

      # Assert to dataspace
      {:ok, _h} = Actor.assert(asserter, ds_cross_ref, {:string, "crashable-assertion"})

      Process.sleep(100)

      # Verify assertion is in dataspace
      state = :sys.get_state(ds_actor)
      {_fid, ds_state} = Map.get(state.entities, Ref.entity_id(ds_ref))
      assert Dataspace.has_assertion?(ds_state, {:string, "crashable-assertion"})

      # Kill the asserter
      Process.exit(asserter, :kill)
      Process.sleep(100)

      # Verify assertion was removed from dataspace
      state = :sys.get_state(ds_actor)
      {_fid, ds_state} = Map.get(state.entities, Ref.entity_id(ds_ref))
      refute Dataspace.has_assertion?(ds_state, {:string, "crashable-assertion"})

      GenServer.stop(ds_actor)
    end

    test "normal termination sends retractions without double-retract" do
      # This verifies that normal termination (GenServer.stop) doesn't cause
      # double retractions due to both terminate/2 and DOWN monitoring

      {:ok, receiver} = Actor.start_link(id: :no_double_receiver, name: :no_double_receiver)
      {:ok, asserter} = Actor.start_link(id: :no_double_asserter, name: :no_double_asserter)

      collector = %CollectorEntity{}
      {:ok, collector_ref} = Actor.spawn_entity(receiver, :root, collector)
      cross_ref = %Ref{actor_id: :no_double_receiver, entity_id: Ref.entity_id(collector_ref)}

      {:ok, handle} = Actor.assert(asserter, cross_ref, {:string, "normal-term"})
      Process.sleep(100)

      # Stop asserter normally
      GenServer.stop(asserter)
      Process.sleep(100)

      # Verify exactly one retraction
      state = :sys.get_state(receiver)
      {_fid, collector_state} = Map.get(state.entities, Ref.entity_id(collector_ref))

      retracts =
        Enum.filter(collector_state.received, fn
          {:retract, _} -> true
          _ -> false
        end)

      assert length(retracts) == 1
      assert [{:retract, ^handle}] = retracts

      GenServer.stop(receiver)
    end
  end
end
