defmodule Absynthe.Integration.UpdateAssertionTest do
  @moduledoc """
  Tests for atomic assertion update (SCN-style state-change notifications).

  These tests verify that the update_assertion API provides atomic replacement
  semantics, preventing stale assertions from leaking and ensuring observers
  see coherent state changes.
  """
  use ExUnit.Case

  alias Absynthe.Core.{Actor, Turn, Entity, Ref}
  alias Absynthe.Dataspace.Dataspace
  alias Absynthe.Assertions.Handle

  # A collector entity that tracks all received events in order
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

  describe "Actor.update_assertion/4" do
    test "initial assertion with nil handle works like regular assert" do
      {:ok, actor} = Actor.start_link(id: :update_test_1)

      collector = %CollectorEntity{}
      {:ok, collector_ref} = Actor.spawn_entity(actor, :root, collector)

      # Update with nil handle should just assert
      {:ok, handle} =
        Actor.update_assertion(actor, nil, collector_ref, {:string, "first_value"})

      assert %Handle{} = handle

      Process.sleep(50)

      state = :sys.get_state(actor)
      {_fid, collector_state} = Map.get(state.entities, Ref.entity_id(collector_ref))

      # Should have exactly one assert event
      asserts = Enum.filter(collector_state.received, &match?({:assert, _, _}, &1))
      assert length(asserts) == 1

      [{:assert, {:string, "first_value"}, ^handle}] = asserts

      GenServer.stop(actor)
    end

    test "atomic update retracts old and asserts new" do
      {:ok, actor} = Actor.start_link(id: :update_test_2)

      collector = %CollectorEntity{}
      {:ok, collector_ref} = Actor.spawn_entity(actor, :root, collector)

      # Initial assertion
      {:ok, handle1} =
        Actor.update_assertion(actor, nil, collector_ref, {:status, :online})

      Process.sleep(30)

      # Update to new value
      {:ok, handle2} =
        Actor.update_assertion(actor, handle1, collector_ref, {:status, :busy})

      Process.sleep(50)

      state = :sys.get_state(actor)
      {_fid, collector_state} = Map.get(state.entities, Ref.entity_id(collector_ref))

      # Should have two asserts and one retract
      asserts = Enum.filter(collector_state.received, &match?({:assert, _, _}, &1))
      retracts = Enum.filter(collector_state.received, &match?({:retract, _}, &1))

      assert length(asserts) == 2
      assert length(retracts) == 1

      # The retract should be for the old handle
      [{:retract, ^handle1}] = retracts

      # Handles should be different
      assert handle1 != handle2

      GenServer.stop(actor)
    end

    test "update with nil assertion only retracts" do
      {:ok, actor} = Actor.start_link(id: :update_test_3)

      collector = %CollectorEntity{}
      {:ok, collector_ref} = Actor.spawn_entity(actor, :root, collector)

      # Initial assertion
      {:ok, handle} =
        Actor.update_assertion(actor, nil, collector_ref, {:string, "temp"})

      Process.sleep(30)

      # Retract completely
      :ok = Actor.update_assertion(actor, handle, collector_ref, nil)

      Process.sleep(50)

      state = :sys.get_state(actor)
      {_fid, collector_state} = Map.get(state.entities, Ref.entity_id(collector_ref))

      retracts = Enum.filter(collector_state.received, &match?({:retract, _}, &1))
      assert length(retracts) == 1
      [{:retract, ^handle}] = retracts

      GenServer.stop(actor)
    end

    test "update with invalid handle returns error" do
      {:ok, actor} = Actor.start_link(id: :update_test_4)

      collector = %CollectorEntity{}
      {:ok, collector_ref} = Actor.spawn_entity(actor, :root, collector)

      # Try to update with a non-existent handle
      bogus_handle = Handle.new(:bogus, 999)
      result = Actor.update_assertion(actor, bogus_handle, collector_ref, {:string, "new"})

      assert result == {:error, :not_found}

      GenServer.stop(actor)
    end

    test "multiple sequential updates maintain consistency" do
      {:ok, actor} = Actor.start_link(id: :update_test_5)

      collector = %CollectorEntity{}
      {:ok, collector_ref} = Actor.spawn_entity(actor, :root, collector)

      # Chain of updates: online -> busy -> away -> offline -> nil
      {:ok, h1} = Actor.update_assertion(actor, nil, collector_ref, {:status, :online})
      Process.sleep(20)
      {:ok, h2} = Actor.update_assertion(actor, h1, collector_ref, {:status, :busy})
      Process.sleep(20)
      {:ok, h3} = Actor.update_assertion(actor, h2, collector_ref, {:status, :away})
      Process.sleep(20)
      {:ok, h4} = Actor.update_assertion(actor, h3, collector_ref, {:status, :offline})
      Process.sleep(20)
      :ok = Actor.update_assertion(actor, h4, collector_ref, nil)

      Process.sleep(50)

      state = :sys.get_state(actor)
      {_fid, collector_state} = Map.get(state.entities, Ref.entity_id(collector_ref))

      # Should have 4 asserts and 4 retracts
      asserts = Enum.filter(collector_state.received, &match?({:assert, _, _}, &1))
      retracts = Enum.filter(collector_state.received, &match?({:retract, _}, &1))

      assert length(asserts) == 4
      assert length(retracts) == 4

      GenServer.stop(actor)
    end

    test "update works with dataspace entity" do
      {:ok, actor} = Actor.start_link(id: :update_ds_test)

      dataspace = Dataspace.new()
      {:ok, ds_ref} = Actor.spawn_entity(actor, :root, dataspace)

      # Initial assertion to dataspace
      {:ok, h1} = Actor.update_assertion(actor, nil, ds_ref, {:string, "first"})
      Process.sleep(30)

      # Verify dataspace has the assertion
      state = :sys.get_state(actor)
      {_fid, ds_state} = Map.get(state.entities, Ref.entity_id(ds_ref))
      assert Dataspace.has_assertion?(ds_state, {:string, "first"})

      # Update to new value
      {:ok, _h2} = Actor.update_assertion(actor, h1, ds_ref, {:string, "second"})
      Process.sleep(50)

      # Verify dataspace has new assertion but not old
      state = :sys.get_state(actor)
      {_fid, ds_state} = Map.get(state.entities, Ref.entity_id(ds_ref))
      refute Dataspace.has_assertion?(ds_state, {:string, "first"})
      assert Dataspace.has_assertion?(ds_state, {:string, "second"})

      GenServer.stop(actor)
    end
  end

  describe "Absynthe.update_value/4 (turn-level API)" do
    test "queues both retract and assert actions atomically" do
      turn = Turn.new(:test_actor, :test_facet)
      ref = %Ref{actor_id: :test_actor, entity_id: 0}

      # Initial assertion
      {turn, handle1} = Absynthe.update_value(turn, nil, ref, {:string, "first"})
      assert %Handle{} = handle1
      assert Turn.action_count(turn) == 1

      # Update
      {turn, handle2} = Absynthe.update_value(turn, handle1, ref, {:string, "second"})
      assert %Handle{} = handle2
      assert handle1 != handle2
      # Should have retract + assert = 2 more actions (total 3)
      assert Turn.action_count(turn) == 3
    end

    test "only queues retract when new value is nil" do
      turn = Turn.new(:test_actor, :test_facet)
      ref = %Ref{actor_id: :test_actor, entity_id: 0}

      {turn, handle} = Absynthe.update_value(turn, nil, ref, {:string, "temp"})
      assert Turn.action_count(turn) == 1

      {turn, nil_handle} = Absynthe.update_value(turn, handle, ref, nil)
      assert nil_handle == nil
      # Should have added one retract action
      assert Turn.action_count(turn) == 2
    end

    test "only queues assert when old handle is nil" do
      turn = Turn.new(:test_actor, :test_facet)
      ref = %Ref{actor_id: :test_actor, entity_id: 0}

      {turn, handle} = Absynthe.update_value(turn, nil, ref, {:string, "new"})
      assert %Handle{} = handle
      assert Turn.action_count(turn) == 1

      # Verify the action is an assert
      [action] = Turn.actions(turn)
      assert %Absynthe.Protocol.Event.Assert{} = action
    end
  end

  describe "attenuation and atomicity" do
    test "failed attenuation does not retract old assertion" do
      # This tests the key atomicity guarantee: if the new assertion is rejected
      # by attenuation, the old assertion should remain intact
      {:ok, actor} = Actor.start_link(id: :attenuation_test)

      collector = %CollectorEntity{}
      {:ok, collector_ref} = Actor.spawn_entity(actor, :root, collector)

      # Create an attenuated ref that only allows Person records with any name
      alias Absynthe.Core.Caveat

      # Pattern that only matches Person records: <Person $name>
      pattern = {:record, {{:symbol, "Person"}, [{:symbol, "$"}]}}
      # Caveat that passes through matching values unchanged
      caveat = Caveat.rewrite(pattern, {:ref, 0})
      {:ok, attenuated_ref} = Ref.attenuate(collector_ref, caveat)

      # First, assert an allowed value (Person record)
      person_assertion = {:record, {{:symbol, "Person"}, [{:string, "Alice"}]}}
      {:ok, handle} = Actor.update_assertion(actor, nil, attenuated_ref, person_assertion)
      Process.sleep(30)

      # Verify the assertion was received (the name part, due to rewrite)
      state = :sys.get_state(actor)
      {_fid, collector_state} = Map.get(state.entities, Ref.entity_id(collector_ref))
      asserts = Enum.filter(collector_state.received, &match?({:assert, _, _}, &1))
      assert length(asserts) == 1

      # Now try to update to a disallowed value (Animal record) - should fail
      animal_assertion = {:record, {{:symbol, "Animal"}, [{:string, "Dog"}]}}
      result = Actor.update_assertion(actor, handle, attenuated_ref, animal_assertion)
      assert result == {:error, :attenuation_rejected}

      Process.sleep(50)

      # Verify no retraction was sent (old assertion should still be valid)
      state = :sys.get_state(actor)
      {_fid, collector_state} = Map.get(state.entities, Ref.entity_id(collector_ref))
      retracts = Enum.filter(collector_state.received, &match?({:retract, _}, &1))
      assert length(retracts) == 0

      # The old handle should still be tracked
      assert Map.has_key?(state.outbound_assertions, handle)

      GenServer.stop(actor)
    end
  end

  describe "cross-actor update_assertion" do
    test "update_assertion works across actors" do
      {:ok, actor1} = Actor.start_link(id: :cross_update_1, name: :cross_update_1)
      {:ok, actor2} = Actor.start_link(id: :cross_update_2, name: :cross_update_2)

      # Create collector in actor2
      collector = %CollectorEntity{}
      {:ok, collector_ref} = Actor.spawn_entity(actor2, :root, collector)

      # Create cross-actor ref
      cross_ref = %Ref{actor_id: :cross_update_2, entity_id: Ref.entity_id(collector_ref)}

      # Update assertion from actor1 to actor2
      {:ok, h1} = Actor.update_assertion(actor1, nil, cross_ref, {:string, "v1"})
      Process.sleep(50)

      {:ok, _h2} = Actor.update_assertion(actor1, h1, cross_ref, {:string, "v2"})
      Process.sleep(100)

      # Check collector in actor2
      state = :sys.get_state(actor2)
      {_fid, collector_state} = Map.get(state.entities, Ref.entity_id(collector_ref))

      asserts = Enum.filter(collector_state.received, &match?({:assert, _, _}, &1))
      retracts = Enum.filter(collector_state.received, &match?({:retract, _}, &1))

      assert length(asserts) == 2
      assert length(retracts) == 1

      GenServer.stop(actor1)
      GenServer.stop(actor2)
    end
  end
end
