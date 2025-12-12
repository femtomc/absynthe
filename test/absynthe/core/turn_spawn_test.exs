defmodule Absynthe.Core.TurnSpawnTest do
  use ExUnit.Case, async: true

  alias Absynthe.Core.{Actor, Turn}
  alias Absynthe.Protocol.Event

  defmodule SpawnTest do
    @moduledoc false
    defstruct [:ref_holder]

    defimpl Absynthe.Core.Entity do
      def on_message(entity, :spawn_child, turn) do
        # Spawn a child entity within the turn
        child_entity = %SpawnTest{ref_holder: nil}
        spawn_action = Event.spawn(:root, child_entity)
        turn = Turn.add_action(turn, spawn_action)
        {entity, turn}
      end

      def on_message(entity, {:set_ref, ref}, turn) do
        {%{entity | ref_holder: ref}, turn}
      end

      def on_message(entity, _message, turn) do
        {entity, turn}
      end

      def on_publish(entity, _assertion, _handle, turn), do: {entity, turn}
      def on_retract(entity, _handle, turn), do: {entity, turn}
      def on_sync(entity, _peer_ref, turn), do: {entity, turn}
    end
  end

  defmodule CollectorEntity do
    @moduledoc false
    defstruct messages: []

    defimpl Absynthe.Core.Entity do
      def on_message(entity, message, turn) do
        new_entity = %{entity | messages: [message | entity.messages]}
        {new_entity, turn}
      end

      def on_publish(entity, _assertion, _handle, turn), do: {entity, turn}
      def on_retract(entity, _handle, turn), do: {entity, turn}
      def on_sync(entity, _peer_ref, turn), do: {entity, turn}
    end
  end

  defmodule MultiSpawner do
    @moduledoc false
    defstruct []

    defimpl Absynthe.Core.Entity do
      def on_message(entity, :spawn_multiple, turn) do
        # Spawn multiple entities in one turn
        child1 = %Absynthe.Core.TurnSpawnTest.SpawnTest{ref_holder: nil}
        child2 = %Absynthe.Core.TurnSpawnTest.SpawnTest{ref_holder: nil}
        child3 = %Absynthe.Core.TurnSpawnTest.SpawnTest{ref_holder: nil}

        turn =
          turn
          |> Turn.add_action(Event.spawn(:root, child1))
          |> Turn.add_action(Event.spawn(:root, child2))
          |> Turn.add_action(Event.spawn(:root, child3))

        {entity, turn}
      end

      def on_message(entity, _message, turn), do: {entity, turn}
      def on_publish(entity, _assertion, _handle, turn), do: {entity, turn}
      def on_retract(entity, _handle, turn), do: {entity, turn}
      def on_sync(entity, _peer_ref, turn), do: {entity, turn}
    end
  end

  defmodule FacetSpawner do
    @moduledoc false
    defstruct []

    defimpl Absynthe.Core.Entity do
      def on_message(entity, :spawn_in_child, turn) do
        # Spawn entity in child facet
        child = %Absynthe.Core.TurnSpawnTest.SpawnTest{ref_holder: nil}
        spawn_action = Event.spawn(:child_facet, child)
        turn = Turn.add_action(turn, spawn_action)
        {entity, turn}
      end

      def on_message(entity, _message, turn), do: {entity, turn}
      def on_publish(entity, _assertion, _handle, turn), do: {entity, turn}
      def on_retract(entity, _handle, turn), do: {entity, turn}
      def on_sync(entity, _peer_ref, turn), do: {entity, turn}
    end
  end

  defmodule FailSpawner do
    @moduledoc false
    defstruct []

    defimpl Absynthe.Core.Entity do
      def on_message(entity, :spawn_with_assert, turn) do
        # Try to spawn in a non-existent facet (will fail)
        # Also add an assertion - it should NOT be delivered if spawn fails
        bad_spawn =
          Event.spawn(:nonexistent_facet, %Absynthe.Core.TurnSpawnTest.SpawnTest{ref_holder: nil})

        # This assertion should not be delivered since the spawn will fail
        ref = %Absynthe.Core.Ref{actor_id: :spawn_failure_test, entity_id: 999}
        assertion = {:string, "should not appear"}
        handle = %Absynthe.Assertions.Handle{id: make_ref()}
        assert_action = Event.assert(ref, assertion, handle)

        turn =
          turn
          |> Turn.add_action(assert_action)
          |> Turn.add_action(bad_spawn)

        {entity, turn}
      end

      def on_message(entity, _message, turn), do: {entity, turn}
      def on_publish(entity, _assertion, _handle, turn), do: {entity, turn}
      def on_retract(entity, _handle, turn), do: {entity, turn}
      def on_sync(entity, _peer_ref, turn), do: {entity, turn}
    end
  end

  describe "spawn action in turn" do
    test "spawn failure aborts entire turn" do
      {:ok, actor} = Actor.start_link(id: :spawn_failure_test)

      spawner = %FailSpawner{}
      {:ok, spawner_ref} = Actor.spawn_entity(actor, :root, spawner)

      # Get initial state
      state = :sys.get_state(actor)
      initial_entity_count = map_size(state.entities)
      initial_assertion_count = map_size(state.outbound_assertions)

      # Trigger action that will fail during spawn
      :ok = Actor.send_message(actor, spawner_ref, :spawn_with_assert)

      # Wait for processing
      Process.sleep(50)

      # Verify that NO entities were spawned and NO assertions were made
      # The entire turn should have been aborted
      state = :sys.get_state(actor)
      assert map_size(state.entities) == initial_entity_count
      assert map_size(state.outbound_assertions) == initial_assertion_count

      GenServer.stop(actor)
    end

    test "spawns entity transactionally" do
      {:ok, actor} = Actor.start_link(id: :spawn_turn_test)

      # Spawn a parent entity
      parent = %SpawnTest{ref_holder: nil}
      {:ok, parent_ref} = Actor.spawn_entity(actor, :root, parent)

      # Get initial entity count
      state = :sys.get_state(actor)
      initial_count = map_size(state.entities)

      # Send a message that causes the entity to spawn a child in its turn
      :ok = Actor.send_message(actor, parent_ref, :spawn_child)

      # Wait for processing
      Process.sleep(50)

      # Verify the child was spawned
      state = :sys.get_state(actor)
      new_count = map_size(state.entities)
      assert new_count == initial_count + 1

      GenServer.stop(actor)
    end

    test "multiple spawn actions in one turn" do
      {:ok, actor} = Actor.start_link(id: :multi_spawn_test)

      spawner = %MultiSpawner{}
      {:ok, spawner_ref} = Actor.spawn_entity(actor, :root, spawner)

      # Get initial entity count
      state = :sys.get_state(actor)
      initial_count = map_size(state.entities)

      # Trigger spawn of multiple entities
      :ok = Actor.send_message(actor, spawner_ref, :spawn_multiple)

      # Wait for processing
      Process.sleep(50)

      # Verify all three children were spawned
      state = :sys.get_state(actor)
      new_count = map_size(state.entities)
      assert new_count == initial_count + 3

      GenServer.stop(actor)
    end

    test "spawn in non-root facet" do
      {:ok, actor} = Actor.start_link(id: :facet_spawn_test)

      # Create a child facet
      :ok = Actor.create_facet(actor, :root, :child_facet)

      spawner = %FacetSpawner{}
      {:ok, spawner_ref} = Actor.spawn_entity(actor, :root, spawner)

      # Trigger spawn in child facet
      :ok = Actor.send_message(actor, spawner_ref, :spawn_in_child)

      # Wait for processing
      Process.sleep(50)

      # Verify entity was spawned in child facet
      state = :sys.get_state(actor)
      child_facet = Map.get(state.facets, :child_facet)
      assert child_facet != nil
      assert MapSet.size(child_facet.entities) == 1

      GenServer.stop(actor)
    end
  end
end
