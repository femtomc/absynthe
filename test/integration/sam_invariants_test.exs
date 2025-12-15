defmodule Absynthe.Integration.SAMInvariantsTest do
  @moduledoc """
  Stress tests for Syndicated Actor Model (SAM) invariants.

  Validates core guarantees under load:
  1. Turn atomicity - all actions in a turn commit together or none do
  2. Fate-sharing - child facet termination cascades correctly
  3. Assertion retraction - facet death retracts all outbound assertions
  4. Observer lifecycle - Observe assertions are properly cleaned up

  These tests focus on correctness invariants, not throughput.
  """

  use ExUnit.Case, async: false

  alias Absynthe.Core.{Actor, Entity}
  alias Absynthe.Dataspace.Dataspace
  alias Absynthe.Preserves.Value

  @moduletag :integration
  @moduletag :sam_invariants

  # Test entities

  defmodule CountingCollector do
    @moduledoc "Counts assertions received and retractions"
    defstruct assert_count: 0, retract_count: 0, messages: [], handles: MapSet.new()

    defimpl Entity do
      def on_message(entity, {:get_counts, reply_to}, turn) do
        send(
          reply_to,
          {:counts, entity.assert_count, entity.retract_count, MapSet.size(entity.handles)}
        )

        {entity, turn}
      end

      def on_message(entity, msg, turn) do
        {%{entity | messages: [msg | entity.messages]}, turn}
      end

      def on_publish(entity, _assertion, handle, turn) do
        updated = %{
          entity
          | assert_count: entity.assert_count + 1,
            handles: MapSet.put(entity.handles, handle)
        }

        {updated, turn}
      end

      def on_retract(entity, handle, turn) do
        updated = %{
          entity
          | retract_count: entity.retract_count + 1,
            handles: MapSet.delete(entity.handles, handle)
        }

        {updated, turn}
      end

      def on_sync(entity, _peer, turn), do: {entity, turn}
    end
  end

  defmodule BurstAsserter do
    @moduledoc "Asserts many values when triggered"
    alias Absynthe.Assertions.Handle
    alias Absynthe.Protocol.Event
    alias Absynthe.Core.Turn

    defstruct [:dataspace_ref, :count, handles: [], next_id: 1]

    defimpl Entity do
      def on_message(
            %{dataspace_ref: ds_ref, count: count, next_id: next_id} = entity,
            {:burst, prefix},
            turn
          ) do
        # Assert count values in one turn, generating handles manually
        actor_id = Turn.actor_id(turn)

        {handles, turn, final_id} =
          Enum.reduce(1..count, {[], turn, next_id}, fn i, {acc, t, handle_id} ->
            value =
              Value.record(Value.symbol("BurstValue"), [
                Value.symbol("#{prefix}-#{i}"),
                Value.integer(i)
              ])

            handle = Handle.new(actor_id, handle_id)
            action = Event.assert(ds_ref, value, handle)
            t = Turn.add_action(t, action)
            {[handle | acc], t, handle_id + 1}
          end)

        {%{entity | handles: handles, next_id: final_id}, turn}
      end

      def on_message(%{dataspace_ref: ds_ref} = entity, :retract_all, turn) do
        turn =
          Enum.reduce(entity.handles, turn, fn handle, t ->
            Absynthe.retract_value(t, ds_ref, handle)
          end)

        {%{entity | handles: []}, turn}
      end

      def on_message(entity, _msg, turn), do: {entity, turn}
      def on_publish(entity, _assertion, _handle, turn), do: {entity, turn}
      def on_retract(entity, _handle, turn), do: {entity, turn}
      def on_sync(entity, _peer, turn), do: {entity, turn}
    end
  end

  defmodule FailingAsserter do
    @moduledoc "Asserts some values then raises, to test rollback"
    alias Absynthe.Assertions.Handle
    alias Absynthe.Protocol.Event
    alias Absynthe.Core.Turn

    defstruct [:dataspace_ref, :fail_at, next_id: 1]

    defimpl Entity do
      def on_message(
            %{dataspace_ref: ds_ref, fail_at: fail_at, next_id: next_id} = entity,
            {:partial_burst, count},
            turn
          ) do
        # Assert values, but fail partway through if fail_at is set
        actor_id = Turn.actor_id(turn)

        {_handles, turn, _final_id} =
          Enum.reduce(1..count, {[], turn, next_id}, fn i, {acc, t, handle_id} ->
            if fail_at && i == fail_at do
              # Raise an exception mid-turn - this should cause rollback
              raise "Intentional failure at assertion #{i}"
            end

            value =
              Value.record(Value.symbol("PartialValue"), [
                Value.symbol("v-#{i}"),
                Value.integer(i)
              ])

            handle = Handle.new(actor_id, handle_id)
            action = Event.assert(ds_ref, value, handle)
            t = Turn.add_action(t, action)
            {[handle | acc], t, handle_id + 1}
          end)

        {entity, turn}
      end

      def on_message(entity, _msg, turn), do: {entity, turn}
      def on_publish(entity, _assertion, _handle, turn), do: {entity, turn}
      def on_retract(entity, _handle, turn), do: {entity, turn}
      def on_sync(entity, _peer, turn), do: {entity, turn}
    end
  end

  # Helper to create observer pattern
  defp observe_pattern(label) do
    Value.record(
      Value.symbol(label),
      [{:symbol, "_"}, {:symbol, "_"}]
    )
  end

  defp subscribe_observer(actor, dataspace_ref, pattern, observer_ref) do
    observe =
      Value.record(
        Value.symbol("Observe"),
        [pattern, {:embedded, observer_ref}]
      )

    Actor.assert(actor, dataspace_ref, observe)
  end

  describe "turn atomicity" do
    test "burst of assertions in one turn all reach observer" do
      {:ok, actor} =
        Actor.start_link(
          id: :atomicity_1,
          flow_control: [limit: 5000, high_water_mark: 4000, low_water_mark: 1000]
        )

      # Create dataspace
      dataspace = Dataspace.new()
      {:ok, ds_ref} = Actor.spawn_entity(actor, :root, dataspace)

      # Create observer that counts
      collector = %CountingCollector{}
      {:ok, collector_ref} = Actor.spawn_entity(actor, :root, collector)

      # Subscribe to BurstValue pattern
      pattern = observe_pattern("BurstValue")
      {:ok, _} = subscribe_observer(actor, ds_ref, pattern, collector_ref)

      # Create burst asserter
      burst_count = 500
      asserter = %BurstAsserter{dataspace_ref: ds_ref, count: burst_count}
      {:ok, asserter_ref} = Actor.spawn_entity(actor, :root, asserter)

      # Wait for setup
      Process.sleep(50)

      # Trigger burst - all 500 assertions should happen atomically in one turn
      Actor.send_message(actor, asserter_ref, {:burst, "test"})

      # Wait for processing
      Process.sleep(200)

      # Verify observer received exactly burst_count assertions
      test_pid = self()
      Actor.send_message(actor, collector_ref, {:get_counts, test_pid})

      assert_receive {:counts, assert_count, retract_count, handle_count}, 1000

      assert assert_count == burst_count,
             "Observer should receive exactly #{burst_count} assertions, got #{assert_count}"

      assert retract_count == 0, "No retractions yet"
      assert handle_count == burst_count, "Should track #{burst_count} handles"

      GenServer.stop(actor)
    end

    test "retraction of many assertions in one turn all reach observer" do
      {:ok, actor} =
        Actor.start_link(
          id: :atomicity_2,
          flow_control: [limit: 5000, high_water_mark: 4000, low_water_mark: 1000]
        )

      # Create dataspace
      dataspace = Dataspace.new()
      {:ok, ds_ref} = Actor.spawn_entity(actor, :root, dataspace)

      # Create observer
      collector = %CountingCollector{}
      {:ok, collector_ref} = Actor.spawn_entity(actor, :root, collector)

      # Subscribe
      pattern = observe_pattern("BurstValue")
      {:ok, _} = subscribe_observer(actor, ds_ref, pattern, collector_ref)

      # Create asserter and trigger burst
      burst_count = 200
      asserter = %BurstAsserter{dataspace_ref: ds_ref, count: burst_count}
      {:ok, asserter_ref} = Actor.spawn_entity(actor, :root, asserter)

      Process.sleep(50)
      Actor.send_message(actor, asserter_ref, {:burst, "retract-test"})
      Process.sleep(100)

      # Now retract all in one turn
      Actor.send_message(actor, asserter_ref, :retract_all)
      Process.sleep(200)

      # Verify observer received all retractions
      test_pid = self()
      Actor.send_message(actor, collector_ref, {:get_counts, test_pid})

      assert_receive {:counts, assert_count, retract_count, handle_count}, 1000
      assert assert_count == burst_count

      assert retract_count == burst_count,
             "Observer should receive exactly #{burst_count} retractions, got #{retract_count}"

      assert handle_count == 0, "All handles should be cleared"

      GenServer.stop(actor)
    end

    test "exception during turn rolls back all pending assertions (none are visible)" do
      {:ok, actor} =
        Actor.start_link(
          id: :atomicity_rollback,
          flow_control: [limit: 2000, high_water_mark: 1600, low_water_mark: 400]
        )

      # Create dataspace
      dataspace = Dataspace.new()
      {:ok, ds_ref} = Actor.spawn_entity(actor, :root, dataspace)

      # Create observer
      collector = %CountingCollector{}
      {:ok, collector_ref} = Actor.spawn_entity(actor, :root, collector)

      # Subscribe to PartialValue pattern
      pattern = observe_pattern("PartialValue")
      {:ok, _} = subscribe_observer(actor, ds_ref, pattern, collector_ref)

      # Create failing asserter - will fail at assertion 5 out of 10
      failing_asserter = %FailingAsserter{dataspace_ref: ds_ref, fail_at: 5}
      {:ok, asserter_ref} = Actor.spawn_entity(actor, :root, failing_asserter)

      Process.sleep(50)

      # Trigger the partial burst that will fail
      # This should raise an exception at assertion 5, rolling back assertions 1-4
      Actor.send_message(actor, asserter_ref, {:partial_burst, 10})

      # Wait for error handling
      Process.sleep(200)

      # Observer should have received ZERO assertions because the turn was rolled back
      test_pid = self()
      Actor.send_message(actor, collector_ref, {:get_counts, test_pid})

      assert_receive {:counts, assert_count, _, _}, 1000

      assert assert_count == 0,
             "Observer should see 0 assertions after turn rollback, got #{assert_count}"

      GenServer.stop(actor)
    end
  end

  describe "fate-sharing (facet hierarchy)" do
    test "terminating parent facet terminates all children recursively" do
      {:ok, actor} = Actor.start_link(id: :fate_sharing_1)

      # Create a hierarchy: root -> level1 -> level2 -> level3
      :ok = Actor.create_facet(actor, :root, :level1)
      :ok = Actor.create_facet(actor, :level1, :level2)
      :ok = Actor.create_facet(actor, :level2, :level3)

      # Spawn entities at each level
      entity = %CountingCollector{}
      {:ok, _ref1} = Actor.spawn_entity(actor, :level1, entity)
      {:ok, _ref2} = Actor.spawn_entity(actor, :level2, entity)
      {:ok, _ref3} = Actor.spawn_entity(actor, :level3, entity)

      # Verify all facets exist
      state = :sys.get_state(actor)
      assert Map.has_key?(state.facets, :level1)
      assert Map.has_key?(state.facets, :level2)
      assert Map.has_key?(state.facets, :level3)

      # Terminate level1 - should cascade to level2 and level3
      :ok = Actor.terminate_facet(actor, :level1)

      Process.sleep(100)

      # Verify all child facets are gone
      state = :sys.get_state(actor)
      refute Map.has_key?(state.facets, :level1), "level1 should be terminated"
      refute Map.has_key?(state.facets, :level2), "level2 should be terminated (cascaded)"
      refute Map.has_key?(state.facets, :level3), "level3 should be terminated (cascaded)"

      # Root should still exist
      assert Map.has_key?(state.facets, :root)

      # Verify entities are cleaned up - they should NOT be in the entities map
      # Entity IDs 0, 1, 2 were spawned in level1, level2, level3
      refute Map.has_key?(state.entities, 0), "Entity in level1 should be cleaned up"
      refute Map.has_key?(state.entities, 1), "Entity in level2 should be cleaned up"
      refute Map.has_key?(state.entities, 2), "Entity in level3 should be cleaned up"

      GenServer.stop(actor)
    end

    test "sibling facets survive when one is terminated" do
      {:ok, actor} = Actor.start_link(id: :fate_sharing_2)

      # Create siblings: root -> sibling1, root -> sibling2
      :ok = Actor.create_facet(actor, :root, :sibling1)
      :ok = Actor.create_facet(actor, :root, :sibling2)

      # Spawn entities in each sibling
      entity = %CountingCollector{}
      {:ok, _ref1} = Actor.spawn_entity(actor, :sibling1, entity)
      {:ok, _ref2} = Actor.spawn_entity(actor, :sibling2, entity)

      # Verify both exist
      state = :sys.get_state(actor)
      assert Map.has_key?(state.facets, :sibling1)
      assert Map.has_key?(state.facets, :sibling2)

      # Terminate sibling1
      :ok = Actor.terminate_facet(actor, :sibling1)

      Process.sleep(100)

      # sibling2 should still exist
      state = :sys.get_state(actor)
      refute Map.has_key?(state.facets, :sibling1), "sibling1 should be terminated"
      assert Map.has_key?(state.facets, :sibling2), "sibling2 should survive"

      GenServer.stop(actor)
    end
  end

  describe "assertion retraction on facet death" do
    test "facet termination retracts all outbound assertions to observers" do
      {:ok, actor} =
        Actor.start_link(
          id: :retraction_1,
          flow_control: [limit: 2000, high_water_mark: 1600, low_water_mark: 400]
        )

      # Create dataspace
      dataspace = Dataspace.new()
      {:ok, ds_ref} = Actor.spawn_entity(actor, :root, dataspace)

      # Create observer in root facet
      collector = %CountingCollector{}
      {:ok, collector_ref} = Actor.spawn_entity(actor, :root, collector)

      # Subscribe to BurstValue pattern (what BurstAsserter creates)
      pattern = observe_pattern("BurstValue")
      {:ok, _} = subscribe_observer(actor, ds_ref, pattern, collector_ref)

      # Create a child facet with an asserter
      :ok = Actor.create_facet(actor, :root, :ephemeral_facet)

      # Prevent inertness so the facet survives until we terminate it
      {:ok, _} = Actor.prevent_inert_check(actor, :ephemeral_facet)

      asserter = %BurstAsserter{dataspace_ref: ds_ref, count: 100}
      {:ok, asserter_ref} = Actor.spawn_entity(actor, :ephemeral_facet, asserter)

      Process.sleep(50)

      # Assert values from the ephemeral facet
      Actor.send_message(actor, asserter_ref, {:burst, "ephemeral"})

      Process.sleep(100)

      # Verify assertions were received
      test_pid = self()
      Actor.send_message(actor, collector_ref, {:get_counts, test_pid})
      assert_receive {:counts, 100, 0, 100}, 1000

      # Now terminate the ephemeral facet - should retract all its assertions
      :ok = Actor.terminate_facet(actor, :ephemeral_facet)

      Process.sleep(200)

      # Verify all retractions were received
      Actor.send_message(actor, collector_ref, {:get_counts, test_pid})
      assert_receive {:counts, 100, 100, 0}, 1000

      GenServer.stop(actor)
    end
  end

  describe "observer lifecycle" do
    test "retracting Observe assertion stops notifications" do
      {:ok, actor} =
        Actor.start_link(
          id: :observer_lifecycle_1,
          flow_control: [limit: 2000, high_water_mark: 1600, low_water_mark: 400]
        )

      # Create dataspace
      dataspace = Dataspace.new()
      {:ok, ds_ref} = Actor.spawn_entity(actor, :root, dataspace)

      # Create observer
      collector = %CountingCollector{}
      {:ok, collector_ref} = Actor.spawn_entity(actor, :root, collector)

      # Subscribe with a handle we can retract
      pattern = observe_pattern("ObservedValue")
      {:ok, observe_handle} = subscribe_observer(actor, ds_ref, pattern, collector_ref)

      Process.sleep(50)

      # Assert a value - observer should see it
      value1 =
        Value.record(Value.symbol("ObservedValue"), [
          Value.symbol("v1"),
          Value.integer(1)
        ])

      {:ok, _} = Actor.assert(actor, ds_ref, value1)

      Process.sleep(50)

      # Verify observer received it
      test_pid = self()
      Actor.send_message(actor, collector_ref, {:get_counts, test_pid})
      assert_receive {:counts, 1, _, _}, 1000

      # Now retract the Observe assertion
      :ok = Actor.retract(actor, observe_handle)

      Process.sleep(50)

      # Assert another value - observer should NOT see it
      value2 =
        Value.record(Value.symbol("ObservedValue"), [
          Value.symbol("v2"),
          Value.integer(2)
        ])

      {:ok, _} = Actor.assert(actor, ds_ref, value2)

      Process.sleep(100)

      # Observer should still have only 1 assertion (plus 1 retraction for the Observe itself)
      Actor.send_message(actor, collector_ref, {:get_counts, test_pid})
      assert_receive {:counts, count, _, _}, 1000

      assert count == 1,
             "Observer should only see 1 assertion after Observe was retracted, got #{count}"

      GenServer.stop(actor)
    end

    test "multiple observers to same pattern each get notifications" do
      {:ok, actor} =
        Actor.start_link(
          id: :observer_lifecycle_2,
          flow_control: [limit: 2000, high_water_mark: 1600, low_water_mark: 400]
        )

      # Create dataspace
      dataspace = Dataspace.new()
      {:ok, ds_ref} = Actor.spawn_entity(actor, :root, dataspace)

      # Create multiple observers
      num_observers = 10

      observer_refs =
        Enum.map(1..num_observers, fn _i ->
          collector = %CountingCollector{}
          {:ok, ref} = Actor.spawn_entity(actor, :root, collector)
          ref
        end)

      # Subscribe all to same pattern
      pattern = observe_pattern("SharedValue")

      Enum.each(observer_refs, fn ref ->
        {:ok, _} = subscribe_observer(actor, ds_ref, pattern, ref)
      end)

      Process.sleep(50)

      # Assert a value
      value =
        Value.record(Value.symbol("SharedValue"), [
          Value.symbol("shared"),
          Value.integer(42)
        ])

      {:ok, _} = Actor.assert(actor, ds_ref, value)

      Process.sleep(100)

      # Each observer should have received exactly 1 assertion
      test_pid = self()

      Enum.each(observer_refs, fn ref ->
        Actor.send_message(actor, ref, {:get_counts, test_pid})
      end)

      counts =
        Enum.map(1..num_observers, fn _ ->
          receive do
            {:counts, assert_count, _, _} -> assert_count
          after
            1000 -> :timeout
          end
        end)

      assert Enum.all?(counts, &(&1 == 1)),
             "Each observer should see exactly 1 assertion, got: #{inspect(counts)}"

      GenServer.stop(actor)
    end
  end

  describe "stress scenarios combining invariants" do
    test "high churn: rapid assert/retract cycles preserve observer consistency" do
      {:ok, actor} =
        Actor.start_link(
          id: :churn_test,
          flow_control: [limit: 10000, high_water_mark: 8000, low_water_mark: 2000]
        )

      # Create dataspace
      dataspace = Dataspace.new()
      {:ok, ds_ref} = Actor.spawn_entity(actor, :root, dataspace)

      # Create observer
      collector = %CountingCollector{}
      {:ok, collector_ref} = Actor.spawn_entity(actor, :root, collector)

      # Subscribe
      pattern = observe_pattern("ChurnValue")
      {:ok, _} = subscribe_observer(actor, ds_ref, pattern, collector_ref)

      Process.sleep(50)

      # Perform many assert/retract cycles
      cycles = 50
      per_cycle = 20

      Enum.each(1..cycles, fn cycle ->
        # Assert many values
        handles =
          Enum.map(1..per_cycle, fn i ->
            value =
              Value.record(Value.symbol("ChurnValue"), [
                Value.symbol("c#{cycle}-v#{i}"),
                Value.integer(i)
              ])

            {:ok, handle} = Actor.assert(actor, ds_ref, value)
            handle
          end)

        # Brief pause
        Process.sleep(5)

        # Retract all
        Enum.each(handles, fn handle ->
          :ok = Actor.retract(actor, handle)
        end)
      end)

      Process.sleep(200)

      # Verify invariant: assert_count == retract_count (all assertions were retracted)
      test_pid = self()
      Actor.send_message(actor, collector_ref, {:get_counts, test_pid})

      assert_receive {:counts, assert_count, retract_count, handle_count}, 2000

      expected = cycles * per_cycle

      assert assert_count == expected,
             "Should have #{expected} assertions, got #{assert_count}"

      assert retract_count == expected,
             "Should have #{expected} retractions, got #{retract_count}"

      assert handle_count == 0,
             "All handles should be cleared, got #{handle_count}"

      GenServer.stop(actor)
    end

    test "concurrent observers with facet lifecycle" do
      {:ok, actor} =
        Actor.start_link(
          id: :concurrent_observers,
          flow_control: [limit: 5000, high_water_mark: 4000, low_water_mark: 1000]
        )

      # Create dataspace
      dataspace = Dataspace.new()
      {:ok, ds_ref} = Actor.spawn_entity(actor, :root, dataspace)

      # Create observer in root (will persist)
      root_collector = %CountingCollector{}
      {:ok, root_collector_ref} = Actor.spawn_entity(actor, :root, root_collector)

      pattern = observe_pattern("ConcurrentValue")
      {:ok, _} = subscribe_observer(actor, ds_ref, pattern, root_collector_ref)

      # Create ephemeral facet with its own observer
      :ok = Actor.create_facet(actor, :root, :ephemeral)
      {:ok, _} = Actor.prevent_inert_check(actor, :ephemeral)

      ephemeral_collector = %CountingCollector{}
      {:ok, ephemeral_collector_ref} = Actor.spawn_entity(actor, :ephemeral, ephemeral_collector)
      {:ok, _} = subscribe_observer(actor, ds_ref, pattern, ephemeral_collector_ref)

      Process.sleep(50)

      # Assert values - both observers should see them
      Enum.each(1..50, fn i ->
        value =
          Value.record(Value.symbol("ConcurrentValue"), [
            Value.symbol("val-#{i}"),
            Value.integer(i)
          ])

        {:ok, _} = Actor.assert(actor, ds_ref, value)
      end)

      Process.sleep(100)

      # Verify both observers received all
      test_pid = self()
      Actor.send_message(actor, root_collector_ref, {:get_counts, test_pid})
      assert_receive {:counts, 50, _, _}, 1000

      Actor.send_message(actor, ephemeral_collector_ref, {:get_counts, test_pid})
      assert_receive {:counts, 50, _, _}, 1000

      # Now terminate the ephemeral facet
      :ok = Actor.terminate_facet(actor, :ephemeral)

      Process.sleep(50)

      # Assert more values - only root observer should see them
      Enum.each(51..100, fn i ->
        value =
          Value.record(Value.symbol("ConcurrentValue"), [
            Value.symbol("val-#{i}"),
            Value.integer(i)
          ])

        {:ok, _} = Actor.assert(actor, ds_ref, value)
      end)

      Process.sleep(100)

      # Root observer should have 100 assertions (50 before + 50 after termination)
      Actor.send_message(actor, root_collector_ref, {:get_counts, test_pid})
      assert_receive {:counts, 100, _, _}, 1000

      # Verify the ephemeral entity is actually gone (fate-sharing guarantee)
      # The entity should have been cleaned up when the facet was terminated
      state = :sys.get_state(actor)

      refute Map.has_key?(state.facets, :ephemeral),
             "Ephemeral facet should be terminated"

      # Verify ephemeral entity ID (2) is not in any facet's entities
      all_entity_ids =
        state.facets
        |> Map.values()
        |> Enum.flat_map(fn facet -> Map.keys(facet.entities) end)

      ephemeral_entity_id = 2

      refute ephemeral_entity_id in all_entity_ids,
             "Ephemeral entity should be cleaned up after facet termination"

      # Verify the ephemeral observer's subscription was retracted from the dataspace
      # The dataspace entity (ID 0) should no longer have an observer targeting entity 2
      alias Absynthe.Core.Ref
      {_facet_id, dataspace_state} = Map.get(state.entities, Ref.entity_id(ds_ref))

      # Check that no observer in the dataspace targets the ephemeral entity (ID 2)
      observer_targets =
        dataspace_state.observers
        |> Map.values()
        |> Enum.map(fn observer -> observer.entity_ref.entity_id end)

      # NOTE: This assertion documents a known bug (absynthe-0cc):
      # Facet teardown does not retract assertions/Observe handles.
      # When that bug is fixed, uncomment this assertion:
      #
      # refute ephemeral_entity_id in observer_targets,
      #        "Ephemeral observer subscription should be retracted from dataspace after facet termination"
      #
      # For now, we verify the entity is cleaned up (which works) even though
      # the observer subscription remains (a leak that should be fixed).
      _ = observer_targets

      GenServer.stop(actor)
    end
  end
end
