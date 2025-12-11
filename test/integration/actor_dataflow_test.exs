defmodule Absynthe.Integration.ActorDataflowTest do
  use ExUnit.Case

  alias Absynthe.Core.{Actor, Turn, Entity}
  alias Absynthe.Dataflow.Field

  # A simple entity that uses dataflow fields
  defmodule CounterEntity do
    defstruct [:count_field]

    defimpl Entity do
      def on_message(%{count_field: field} = entity, {:increment, amount}, turn) do
        current = Field.get(field)
        turn = Field.set(field, current + amount, turn)
        {entity, turn}
      end

      def on_message(entity, _msg, turn), do: {entity, turn}
      def on_publish(entity, _assertion, _handle, turn), do: {entity, turn}
      def on_retract(entity, _handle, turn), do: {entity, turn}
      def on_sync(entity, _peer, turn), do: {entity, turn}
    end
  end

  describe "actor + dataflow integration" do
    test "field updates are executed when turn commits" do
      # Create a field
      counter = Field.new(0)

      # Create an entity that uses the field
      entity = %CounterEntity{count_field: counter}

      # Start an actor
      {:ok, actor} = Actor.start_link(id: :test_actor)

      # Spawn the entity in the actor
      {:ok, ref} = Actor.spawn_entity(actor, :root, entity)

      # Send increment messages
      Actor.send_message(actor, ref, {:increment, 5})
      Actor.send_message(actor, ref, {:increment, 3})

      # Give time for async processing
      Process.sleep(50)

      # Field should be updated
      assert Field.get(counter) == 8

      # Clean up
      GenServer.stop(actor)
    end

    test "computed fields react to field updates through actor" do
      # Create source field and computed field
      source = Field.new(10)

      doubled = Field.computed(fn ->
        Field.get(source) * 2
      end)

      # Initial computation
      assert Field.get(doubled) == 20

      # Create entity that updates the source field
      entity = %CounterEntity{count_field: source}

      {:ok, actor} = Actor.start_link(id: :test_actor_2)
      {:ok, ref} = Actor.spawn_entity(actor, :root, entity)

      # Send increment
      Actor.send_message(actor, ref, {:increment, 5})

      # Give time for processing
      Process.sleep(50)

      # Source should be updated
      assert Field.get(source) == 15

      # Computed field should reflect the change
      assert Field.get(doubled) == 30

      GenServer.stop(actor)
    end

    test "field cleanup on actor termination" do
      # Create fields
      field1 = Field.new("value1")
      field2 = Field.new("value2")

      # Verify fields exist
      assert Field.get(field1) == "value1"
      assert Field.get(field2) == "value2"

      # Cleanup current process fields
      Field.cleanup()

      # Fields created in this test process should be cleaned up
      # New fields can still be created
      field3 = Field.new("value3")
      assert Field.get(field3) == "value3"

      Field.cleanup()
    end

    test "field updates see fresh dependents" do
      # This tests the stale closure fix - dependents added after set() but before commit
      # should still be notified

      source = Field.new(1)

      # Create initial computed
      computed1 = Field.computed(fn ->
        Field.get(source) * 10
      end)

      # Trigger initial computation
      assert Field.get(computed1) == 10

      # Create a turn and stage an update
      turn = Turn.new(:actor, :facet)
      turn = Field.set(source, 2, turn)

      # Now add another dependent AFTER the set but BEFORE commit
      computed2 = Field.computed(fn ->
        Field.get(source) * 100
      end)

      # Trigger computation of computed2 to establish dependency
      assert Field.get(computed2) == 100

      # Now commit the field updates
      Field.commit_field_updates(turn)

      # Both computed fields should see the new value
      assert Field.get(source) == 2
      assert Field.get(computed1) == 20
      assert Field.get(computed2) == 200

      Field.cleanup()
    end

    test "cycle detection raises error" do
      # Create a computed field that depends on itself (directly)
      # This is done by having a mutable reference that we set after creation
      self_ref = Field.new(nil)

      cyclic = Field.computed(fn ->
        ref = Field.get(self_ref)
        if ref, do: Field.get(ref), else: 0
      end)

      # Initial read works (self_ref is nil, so no cycle yet)
      assert Field.get(cyclic) == 0

      # Now set self_ref to point to cyclic - this creates a cycle
      turn = Turn.new(:actor, :facet)
      turn = Field.set(self_ref, cyclic, turn)
      Field.commit_field_updates(turn)

      # Mark cyclic as dirty by updating self_ref again
      turn2 = Turn.new(:actor, :facet)
      turn2 = Field.set(self_ref, cyclic, turn2)
      Field.commit_field_updates(turn2)

      # Reading cyclic should now detect the cycle
      assert_raise ArgumentError, ~r/Cycle detected/, fn ->
        Field.get(cyclic)
      end

      Field.cleanup()
    end
  end
end
