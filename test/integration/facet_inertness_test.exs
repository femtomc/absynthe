defmodule Absynthe.Integration.FacetInertnessTest do
  @moduledoc """
  Tests for facet inertness detection and automatic teardown.

  Verifies that:
  1. Facets with no children, handles, tasks, or preventers are auto-terminated
  2. Parent facets are cascaded when children become inert
  3. prevent_inert_check guards prevent premature termination
  4. Root facet is never auto-terminated by inertness
  """

  use ExUnit.Case

  alias Absynthe.Core.{Actor, Entity, Ref}

  # A simple entity that does nothing
  defmodule NoOpEntity do
    defstruct []

    defimpl Entity do
      def on_message(entity, _message, turn), do: {entity, turn}
      def on_publish(entity, _assertion, _handle, turn), do: {entity, turn}
      def on_retract(entity, _handle, turn), do: {entity, turn}
      def on_sync(entity, _peer, turn), do: {entity, turn}
    end
  end

  # An entity that makes assertions when told to
  defmodule AssertingEntity do
    defstruct [:target_ref, :value]

    defimpl Entity do
      def on_message(%{target_ref: target, value: value} = entity, :assert_now, turn) do
        turn = Absynthe.assert_value(turn, target, value)
        {entity, turn}
      end

      def on_message(entity, _message, turn), do: {entity, turn}
      def on_publish(entity, _assertion, _handle, turn), do: {entity, turn}
      def on_retract(entity, _handle, turn), do: {entity, turn}
      def on_sync(entity, _peer, turn), do: {entity, turn}
    end
  end

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

  describe "automatic inertness detection" do
    test "empty child facet is auto-terminated after turn" do
      {:ok, actor} = Actor.start_link(id: :inert_actor_1, name: :inert_actor_1)

      # Create a child facet
      :ok = Actor.create_facet(actor, :root, :child_facet)

      # Verify facet was created
      state = :sys.get_state(actor)
      assert Map.has_key?(state.facets, :child_facet)

      # Spawn a no-op entity in the child facet to trigger a turn
      {:ok, ref} = Actor.spawn_entity(actor, :child_facet, %NoOpEntity{})

      # Send a message to the entity to trigger a turn
      Actor.send_message(actor, ref, :ping)

      # Wait for processing
      Process.sleep(100)

      # After the turn, the child facet should have been auto-terminated
      # because it's inert (no handles, no children, no tasks, no preventers)
      state = :sys.get_state(actor)
      refute Map.has_key?(state.facets, :child_facet)

      GenServer.stop(actor)
    end

    test "facet with outbound handles is NOT auto-terminated" do
      {:ok, asserter} = Actor.start_link(id: :inert_asserter, name: :inert_asserter)
      {:ok, receiver} = Actor.start_link(id: :inert_receiver, name: :inert_receiver)

      # Create collector in receiver
      {:ok, collector_ref} = Actor.spawn_entity(receiver, :root, %CollectorEntity{})
      cross_ref = %Ref{actor_id: :inert_receiver, entity_id: Ref.entity_id(collector_ref)}

      # Create a child facet in asserter
      :ok = Actor.create_facet(asserter, :root, :asserting_facet)

      # Create an entity that will make an assertion
      entity = %AssertingEntity{target_ref: cross_ref, value: {:string, "test"}}
      {:ok, entity_ref} = Actor.spawn_entity(asserter, :asserting_facet, entity)

      # Trigger the assertion
      Actor.send_message(asserter, entity_ref, :assert_now)

      # Wait for processing
      Process.sleep(100)

      # The facet should still exist because it has an outbound handle
      state = :sys.get_state(asserter)
      assert Map.has_key?(state.facets, :asserting_facet)

      # Verify the handle is tracked to the facet
      handles = Map.get(state.facet_handles, :asserting_facet, MapSet.new())
      assert MapSet.size(handles) == 1

      GenServer.stop(asserter)
      GenServer.stop(receiver)
    end

    test "facet becomes inert after retracting its assertions" do
      {:ok, asserter} = Actor.start_link(id: :retract_asserter, name: :retract_asserter)
      {:ok, receiver} = Actor.start_link(id: :retract_receiver, name: :retract_receiver)

      # Create collector in receiver
      {:ok, collector_ref} = Actor.spawn_entity(receiver, :root, %CollectorEntity{})
      cross_ref = %Ref{actor_id: :retract_receiver, entity_id: Ref.entity_id(collector_ref)}

      # Create a child facet in asserter
      :ok = Actor.create_facet(asserter, :root, :temp_facet)

      # Make an assertion from the client API (goes to root facet, not child)
      # Let's make the assertion directly from the child facet context
      entity = %AssertingEntity{target_ref: cross_ref, value: {:string, "temp"}}
      {:ok, entity_ref} = Actor.spawn_entity(asserter, :temp_facet, entity)

      # Trigger the assertion
      Actor.send_message(asserter, entity_ref, :assert_now)

      Process.sleep(100)

      # Facet should exist
      state = :sys.get_state(asserter)
      assert Map.has_key?(state.facets, :temp_facet)

      # Now terminate the facet (which retracts assertions)
      :ok = Actor.terminate_facet(asserter, :temp_facet)

      Process.sleep(100)

      # Facet should be gone
      state = :sys.get_state(asserter)
      refute Map.has_key?(state.facets, :temp_facet)

      GenServer.stop(asserter)
      GenServer.stop(receiver)
    end

    test "client-initiated retraction triggers inertness check and auto-teardown" do
      # This tests the fix for the bug where retracting via Actor.retract/2
      # didn't trigger inertness detection on the owning facet
      {:ok, asserter} = Actor.start_link(id: :client_retract, name: :client_retract)
      {:ok, receiver} = Actor.start_link(id: :client_receiver, name: :client_receiver)

      # Create collector in receiver
      {:ok, collector_ref} = Actor.spawn_entity(receiver, :root, %CollectorEntity{})
      cross_ref = %Ref{actor_id: :client_receiver, entity_id: Ref.entity_id(collector_ref)}

      # Create a child facet in asserter
      :ok = Actor.create_facet(asserter, :root, :retract_test_facet)

      # Create an entity that will make an assertion owned by the child facet
      entity = %AssertingEntity{target_ref: cross_ref, value: {:string, "client_retract"}}
      {:ok, entity_ref} = Actor.spawn_entity(asserter, :retract_test_facet, entity)

      # Trigger the assertion
      Actor.send_message(asserter, entity_ref, :assert_now)

      Process.sleep(100)

      # Facet should exist and have one handle
      state = :sys.get_state(asserter)
      assert Map.has_key?(state.facets, :retract_test_facet)
      handles = Map.get(state.facet_handles, :retract_test_facet, MapSet.new())
      assert MapSet.size(handles) == 1
      [handle] = MapSet.to_list(handles)

      # Now retract via client API - this should trigger inertness detection
      :ok = Actor.retract(asserter, handle)

      # Give time for async processing
      Process.sleep(100)

      # The facet should now be auto-terminated because it lost its last handle
      state = :sys.get_state(asserter)
      refute Map.has_key?(state.facets, :retract_test_facet)

      GenServer.stop(asserter)
      GenServer.stop(receiver)
    end
  end

  describe "parent cascade on child inertness" do
    test "parent becomes inert after child goes inert" do
      {:ok, actor} = Actor.start_link(id: :cascade_actor, name: :cascade_actor)

      # Create nested facets: root -> parent -> child
      :ok = Actor.create_facet(actor, :root, :parent_facet)
      :ok = Actor.create_facet(actor, :parent_facet, :child_facet)

      # Verify structure
      state = :sys.get_state(actor)
      assert Map.has_key?(state.facets, :parent_facet)
      assert Map.has_key?(state.facets, :child_facet)

      # Spawn entity in child facet to trigger a turn
      {:ok, ref} = Actor.spawn_entity(actor, :child_facet, %NoOpEntity{})
      Actor.send_message(actor, ref, :ping)

      Process.sleep(100)

      # Both facets should be auto-terminated due to cascade:
      # 1. Child facet becomes inert after turn
      # 2. Parent facet becomes inert after child is terminated
      state = :sys.get_state(actor)
      refute Map.has_key?(state.facets, :child_facet)
      refute Map.has_key?(state.facets, :parent_facet)

      GenServer.stop(actor)
    end

    test "parent with other children is NOT terminated when one child goes inert" do
      {:ok, actor} = Actor.start_link(id: :multi_child_actor, name: :multi_child_actor)
      {:ok, receiver} = Actor.start_link(id: :multi_receiver, name: :multi_receiver)

      {:ok, collector_ref} = Actor.spawn_entity(receiver, :root, %CollectorEntity{})
      cross_ref = %Ref{actor_id: :multi_receiver, entity_id: Ref.entity_id(collector_ref)}

      # Create parent with two children
      :ok = Actor.create_facet(actor, :root, :parent)
      :ok = Actor.create_facet(actor, :parent, :child1)
      :ok = Actor.create_facet(actor, :parent, :child2)

      # Put an assertion in child2 to keep it alive
      entity = %AssertingEntity{target_ref: cross_ref, value: {:string, "keep_alive"}}
      {:ok, entity_ref} = Actor.spawn_entity(actor, :child2, entity)
      Actor.send_message(actor, entity_ref, :assert_now)

      Process.sleep(100)

      # Spawn entity in child1 (which will become inert)
      {:ok, noop_ref} = Actor.spawn_entity(actor, :child1, %NoOpEntity{})
      Actor.send_message(actor, noop_ref, :ping)

      Process.sleep(100)

      # child1 should be terminated (inert)
      # but parent should remain (has child2)
      # and child2 should remain (has assertion)
      state = :sys.get_state(actor)
      refute Map.has_key?(state.facets, :child1)
      assert Map.has_key?(state.facets, :parent)
      assert Map.has_key?(state.facets, :child2)

      GenServer.stop(actor)
      GenServer.stop(receiver)
    end
  end

  describe "prevent_inert_check guard" do
    test "facet with preventer is NOT auto-terminated" do
      {:ok, actor} = Actor.start_link(id: :prevent_actor, name: :prevent_actor)

      # Create a child facet
      :ok = Actor.create_facet(actor, :root, :guarded_facet)

      # Prevent inertness check
      {:ok, _token} = Actor.prevent_inert_check(actor, :guarded_facet)

      # Spawn entity and trigger turn
      {:ok, ref} = Actor.spawn_entity(actor, :guarded_facet, %NoOpEntity{})
      Actor.send_message(actor, ref, :ping)

      Process.sleep(100)

      # Facet should still exist despite being "empty"
      state = :sys.get_state(actor)
      assert Map.has_key?(state.facets, :guarded_facet)

      GenServer.stop(actor)
    end

    test "facet is terminated after preventer is released" do
      {:ok, actor} = Actor.start_link(id: :release_actor, name: :release_actor)

      # Create a child facet
      :ok = Actor.create_facet(actor, :root, :temp_guarded)

      # Prevent inertness check
      {:ok, token} = Actor.prevent_inert_check(actor, :temp_guarded)

      # Spawn entity and trigger turn
      {:ok, ref} = Actor.spawn_entity(actor, :temp_guarded, %NoOpEntity{})
      Actor.send_message(actor, ref, :ping)

      Process.sleep(100)

      # Facet should still exist
      state = :sys.get_state(actor)
      assert Map.has_key?(state.facets, :temp_guarded)

      # Now release the preventer
      :ok = Actor.allow_inert_check(actor, :temp_guarded, token)

      # Facet should now be auto-terminated
      state = :sys.get_state(actor)
      refute Map.has_key?(state.facets, :temp_guarded)

      GenServer.stop(actor)
    end

    test "multiple preventers require multiple releases" do
      {:ok, actor} = Actor.start_link(id: :multi_prevent_actor, name: :multi_prevent_actor)

      # Create a child facet
      :ok = Actor.create_facet(actor, :root, :multi_guarded)

      # Add two preventers
      {:ok, token1} = Actor.prevent_inert_check(actor, :multi_guarded)
      {:ok, token2} = Actor.prevent_inert_check(actor, :multi_guarded)

      # Spawn entity
      {:ok, ref} = Actor.spawn_entity(actor, :multi_guarded, %NoOpEntity{})
      Actor.send_message(actor, ref, :ping)

      Process.sleep(100)

      # Release first preventer - should still exist
      :ok = Actor.allow_inert_check(actor, :multi_guarded, token1)
      state = :sys.get_state(actor)
      assert Map.has_key?(state.facets, :multi_guarded)

      # Release second preventer - now should be terminated
      :ok = Actor.allow_inert_check(actor, :multi_guarded, token2)
      state = :sys.get_state(actor)
      refute Map.has_key?(state.facets, :multi_guarded)

      GenServer.stop(actor)
    end
  end

  describe "root facet protection" do
    test "root facet is NEVER auto-terminated" do
      {:ok, actor} = Actor.start_link(id: :root_protect_actor, name: :root_protect_actor)

      # Spawn entity in root and trigger turn
      {:ok, ref} = Actor.spawn_entity(actor, :root, %NoOpEntity{})
      Actor.send_message(actor, ref, :ping)

      Process.sleep(100)

      # Root facet should still exist
      state = :sys.get_state(actor)
      assert Map.has_key?(state.facets, :root)

      GenServer.stop(actor)
    end
  end

  describe "error handling" do
    test "allow_inert_check with no preventers returns error" do
      {:ok, actor} = Actor.start_link(id: :error_actor, name: :error_actor)

      :ok = Actor.create_facet(actor, :root, :test_facet)

      # Try to allow without preventing first
      result = Actor.allow_inert_check(actor, :test_facet, make_ref())
      assert result == {:error, :no_preventers}

      GenServer.stop(actor)
    end

    test "prevent_inert_check on nonexistent facet returns error" do
      {:ok, actor} = Actor.start_link(id: :nonexist_actor, name: :nonexist_actor)

      result = Actor.prevent_inert_check(actor, :nonexistent)
      assert result == {:error, :facet_not_found}

      GenServer.stop(actor)
    end
  end
end
