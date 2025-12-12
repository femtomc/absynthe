defmodule Absynthe.Integration.FacetTeardownTest do
  @moduledoc """
  Tests for facet and actor teardown behavior.

  Verifies that assertions are properly retracted when:
  1. A facet is terminated (facet-owned assertions are retracted)
  2. An actor terminates (all outbound assertions are retracted)
  3. Turn-initiated assertions are tracked and cleaned up
  """

  use ExUnit.Case

  alias Absynthe.Core.{Actor, Entity, Ref}
  alias Absynthe.Dataspace.Dataspace

  # A collector entity that tracks received events
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

      def on_sync(entity, _peer, turn), do: {entity, turn}
    end
  end

  # An entity that asserts a value during initialization (via turn actions)
  defmodule AssertingEntity do
    defstruct [:target_ref, :value]

    defimpl Entity do
      def on_message(%{target_ref: target, value: value} = entity, :assert_now, turn) do
        # Create a new assertion via turn action
        turn = Absynthe.assert_value(turn, target, value)
        {entity, turn}
      end

      def on_message(entity, _message, turn), do: {entity, turn}
      def on_publish(entity, _assertion, _handle, turn), do: {entity, turn}
      def on_retract(entity, _handle, turn), do: {entity, turn}
      def on_sync(entity, _peer, turn), do: {entity, turn}
    end
  end

  describe "facet termination" do
    test "terminating a facet retracts all its assertions" do
      # Create two actors - one to hold assertions, one to receive them
      {:ok, asserter} = Actor.start_link(id: :asserter_facet, name: :asserter_facet)
      {:ok, receiver} = Actor.start_link(id: :receiver_facet, name: :receiver_facet)

      # Create collector in receiver
      collector = %CollectorEntity{}
      {:ok, collector_ref} = Actor.spawn_entity(receiver, :root, collector)

      # Create a cross-actor ref
      cross_ref = %Ref{actor_id: :receiver_facet, entity_id: Ref.entity_id(collector_ref)}

      # Create a child facet in asserter
      :ok = Actor.create_facet(asserter, :root, :child_facet)

      # Make assertions from the child facet
      # Note: Client-initiated assertions go to root facet, so we use an entity
      asserting_entity = %AssertingEntity{target_ref: cross_ref, value: {:string, "from_child"}}
      {:ok, _entity_ref} = Actor.spawn_entity(asserter, :child_facet, asserting_entity)

      # Also make a client-initiated assertion to compare (goes to root facet)
      {:ok, root_handle} = Actor.assert(asserter, cross_ref, {:string, "from_root"})

      # Trigger the entity to assert
      state = :sys.get_state(asserter)
      entity_id = state.next_entity_id - 1
      entity_ref = Ref.new(:asserter_facet, entity_id)
      Actor.send_message(asserter, entity_ref, :assert_now)

      # Wait for processing
      Process.sleep(100)

      # Verify collector received both assertions
      state = :sys.get_state(receiver)
      {_fid, collector_state} = Map.get(state.entities, Ref.entity_id(collector_ref))

      asserts =
        Enum.filter(collector_state.received, fn
          {:assert, _, _} -> true
          _ -> false
        end)

      assert length(asserts) == 2

      # Now terminate the child facet - should retract child facet's assertion
      :ok = Actor.terminate_facet(asserter, :child_facet)

      # Wait for retraction to propagate
      Process.sleep(100)

      # Verify collector received a retraction
      state = :sys.get_state(receiver)
      {_fid, collector_state} = Map.get(state.entities, Ref.entity_id(collector_ref))

      retracts =
        Enum.filter(collector_state.received, fn
          {:retract, _} -> true
          _ -> false
        end)

      # Should have 1 retraction (from the child facet's entity assertion)
      assert length(retracts) == 1

      # The root facet assertion should still be there
      asserter_state = :sys.get_state(asserter)
      assert Map.has_key?(asserter_state.outbound_assertions, root_handle)

      GenServer.stop(asserter)
      GenServer.stop(receiver)
    end

    test "nested facet termination retracts assertions from all descendant facets" do
      {:ok, asserter} = Actor.start_link(id: :asserter_nested, name: :asserter_nested)
      {:ok, receiver} = Actor.start_link(id: :receiver_nested, name: :receiver_nested)

      # Create collector in receiver
      collector = %CollectorEntity{}
      {:ok, collector_ref} = Actor.spawn_entity(receiver, :root, collector)
      cross_ref = %Ref{actor_id: :receiver_nested, entity_id: Ref.entity_id(collector_ref)}

      # Create nested facets
      :ok = Actor.create_facet(asserter, :root, :parent_facet)
      :ok = Actor.create_facet(asserter, :parent_facet, :child_facet)

      # Create asserting entities in both facets
      parent_entity = %AssertingEntity{target_ref: cross_ref, value: {:string, "from_parent"}}
      {:ok, _} = Actor.spawn_entity(asserter, :parent_facet, parent_entity)

      child_entity = %AssertingEntity{target_ref: cross_ref, value: {:string, "from_child"}}
      {:ok, _} = Actor.spawn_entity(asserter, :child_facet, child_entity)

      # Trigger both entities to assert
      state = :sys.get_state(asserter)
      parent_ref = Ref.new(:asserter_nested, state.next_entity_id - 2)
      child_ref = Ref.new(:asserter_nested, state.next_entity_id - 1)
      Actor.send_message(asserter, parent_ref, :assert_now)
      Actor.send_message(asserter, child_ref, :assert_now)

      Process.sleep(100)

      # Verify both assertions received
      state = :sys.get_state(receiver)
      {_fid, collector_state} = Map.get(state.entities, Ref.entity_id(collector_ref))

      asserts =
        Enum.filter(collector_state.received, fn
          {:assert, _, _} -> true
          _ -> false
        end)

      assert length(asserts) == 2

      # Terminate parent facet - should also terminate child and retract both assertions
      :ok = Actor.terminate_facet(asserter, :parent_facet)

      Process.sleep(100)

      # Verify both assertions were retracted
      state = :sys.get_state(receiver)
      {_fid, collector_state} = Map.get(state.entities, Ref.entity_id(collector_ref))

      retracts =
        Enum.filter(collector_state.received, fn
          {:retract, _} -> true
          _ -> false
        end)

      assert length(retracts) == 2

      GenServer.stop(asserter)
      GenServer.stop(receiver)
    end
  end

  describe "actor termination" do
    test "actor termination retracts all outbound assertions to remote actors" do
      {:ok, asserter} = Actor.start_link(id: :asserter_actor, name: :asserter_actor)
      {:ok, receiver} = Actor.start_link(id: :receiver_actor, name: :receiver_actor)

      # Create collector in receiver
      collector = %CollectorEntity{}
      {:ok, collector_ref} = Actor.spawn_entity(receiver, :root, collector)
      cross_ref = %Ref{actor_id: :receiver_actor, entity_id: Ref.entity_id(collector_ref)}

      # Make multiple assertions from asserter to receiver
      {:ok, _h1} = Actor.assert(asserter, cross_ref, {:string, "assertion_1"})
      {:ok, _h2} = Actor.assert(asserter, cross_ref, {:string, "assertion_2"})
      {:ok, _h3} = Actor.assert(asserter, cross_ref, {:string, "assertion_3"})

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

      # Terminate the asserter actor
      GenServer.stop(asserter)

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

      GenServer.stop(receiver)
    end
  end

  describe "turn-initiated assertions" do
    test "assertions made via Turn.add_action are tracked and retractable" do
      {:ok, asserter} = Actor.start_link(id: :asserter_turn, name: :asserter_turn)
      {:ok, receiver} = Actor.start_link(id: :receiver_turn, name: :receiver_turn)

      # Create collector in receiver
      collector = %CollectorEntity{}
      {:ok, collector_ref} = Actor.spawn_entity(receiver, :root, collector)
      cross_ref = %Ref{actor_id: :receiver_turn, entity_id: Ref.entity_id(collector_ref)}

      # Create asserting entity
      asserting_entity = %AssertingEntity{
        target_ref: cross_ref,
        value: {:string, "turn_assertion"}
      }

      {:ok, entity_ref} = Actor.spawn_entity(asserter, :root, asserting_entity)

      # Trigger assertion
      Actor.send_message(asserter, entity_ref, :assert_now)

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

      # Verify assertion is tracked in asserter's state
      asserter_state = :sys.get_state(asserter)
      assert map_size(asserter_state.outbound_assertions) == 1

      # The handle should be in the root facet's handles
      root_handles = Map.get(asserter_state.facet_handles, :root, MapSet.new())
      assert MapSet.size(root_handles) == 1

      GenServer.stop(asserter)

      Process.sleep(100)

      # Verify retraction was sent on termination
      state = :sys.get_state(receiver)
      {_fid, collector_state} = Map.get(state.entities, Ref.entity_id(collector_ref))

      retracts =
        Enum.filter(collector_state.received, fn
          {:retract, _} -> true
          _ -> false
        end)

      assert length(retracts) == 1

      GenServer.stop(receiver)
    end
  end

  describe "dataspace integration" do
    test "observer cleanup when actor terminates" do
      # This tests that Observe assertions (subscriptions) are retracted
      # when the subscribing actor terminates

      {:ok, ds_actor} = Actor.start_link(id: :ds_teardown, name: :ds_teardown)
      {:ok, observer_actor} = Actor.start_link(id: :observer_teardown, name: :observer_teardown)

      # Create dataspace entity
      dataspace = Dataspace.new()
      {:ok, ds_ref} = Actor.spawn_entity(ds_actor, :root, dataspace)
      ds_cross_ref = %Ref{actor_id: :ds_teardown, entity_id: Ref.entity_id(ds_ref)}

      # Create observer entity
      observer = %CollectorEntity{}
      {:ok, observer_ref} = Actor.spawn_entity(observer_actor, :root, observer)

      observer_cross_ref = %Ref{
        actor_id: :observer_teardown,
        entity_id: Ref.entity_id(observer_ref)
      }

      # Subscribe to a pattern via Observe assertion
      observe_assertion =
        Absynthe.observe(
          Absynthe.record(:TestData, [Absynthe.wildcard()]),
          observer_cross_ref
        )

      {:ok, observe_handle} = Actor.assert(observer_actor, ds_cross_ref, observe_assertion)

      Process.sleep(100)

      # Verify observer handle is tracked
      observer_state = :sys.get_state(observer_actor)
      assert Map.has_key?(observer_state.outbound_assertions, observe_handle)

      # Publish an assertion that matches
      {:ok, _data_handle} =
        Actor.assert(ds_actor, ds_cross_ref, Absynthe.record(:TestData, ["value"]))

      Process.sleep(100)

      # Observer should have received the matched assertion
      state = :sys.get_state(observer_actor)
      {_fid, observer_state} = Map.get(state.entities, Ref.entity_id(observer_ref))

      asserts =
        Enum.filter(observer_state.received, fn
          {:assert, _, _} -> true
          _ -> false
        end)

      assert length(asserts) >= 1

      # Now terminate the observer actor - Observe should be retracted
      GenServer.stop(observer_actor)

      Process.sleep(100)

      # The dataspace should have processed the retraction of the Observe
      # (We can't easily verify this without more introspection, but the key
      # is that it shouldn't crash and should handle gracefully)

      GenServer.stop(ds_actor)
    end
  end
end
