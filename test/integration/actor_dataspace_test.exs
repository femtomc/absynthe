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
end
