defmodule Absynthe.Integration.DataspaceRoutingTest do
  @moduledoc """
  End-to-end tests for Syndicated Actor Model routing through dataspaces.

  These tests verify the full SAM flow:
  1. Assertions are stored in the dataspace bag
  2. Assertions are indexed in the skeleton
  3. Observers are notified when matching assertions appear
  4. Retractions properly clean up and notify observers
  """
  use ExUnit.Case

  alias Absynthe.Core.{Turn, Entity}
  alias Absynthe.Dataspace.Dataspace
  alias Absynthe.Assertions.Handle
  alias Absynthe.Preserves.Value

  describe "dataspace assertion storage" do
    test "assertions are stored in bag and retrievable" do
      dataspace = Dataspace.new()
      turn = Turn.new(:test_actor, :test_facet)
      handle = Handle.new(1)
      assertion = Value.string("hello world")

      # Publish assertion to dataspace
      {updated_ds, _turn} = Entity.on_publish(dataspace, assertion, handle, turn)

      # Verify assertion is stored
      assert Dataspace.has_assertion?(updated_ds, assertion)
      assert assertion in Dataspace.assertions(updated_ds)
    end

    test "multiple assertions are tracked independently" do
      dataspace = Dataspace.new()
      turn = Turn.new(:test_actor, :test_facet)

      h1 = Handle.new(1)
      h2 = Handle.new(2)
      a1 = Value.string("first")
      a2 = Value.integer(42)

      {dataspace, turn} = Entity.on_publish(dataspace, a1, h1, turn)
      {dataspace, _turn} = Entity.on_publish(dataspace, a2, h2, turn)

      assert Dataspace.has_assertion?(dataspace, a1)
      assert Dataspace.has_assertion?(dataspace, a2)
      assert length(Dataspace.assertions(dataspace)) == 2
    end

    test "retracting removes assertion from bag" do
      dataspace = Dataspace.new()
      turn = Turn.new(:test_actor, :test_facet)
      handle = Handle.new(1)
      assertion = Value.string("temporary")

      # Add and then retract
      {dataspace, turn} = Entity.on_publish(dataspace, assertion, handle, turn)
      assert Dataspace.has_assertion?(dataspace, assertion)

      {dataspace, _turn} = Entity.on_retract(dataspace, handle, turn)
      refute Dataspace.has_assertion?(dataspace, assertion)
    end

    test "reference counting - same assertion multiple times" do
      dataspace = Dataspace.new()
      turn = Turn.new(:test_actor, :test_facet)

      assertion = Value.symbol("shared")
      h1 = Handle.new(1)
      h2 = Handle.new(2)

      # Publish same assertion twice with different handles
      {dataspace, turn} = Entity.on_publish(dataspace, assertion, h1, turn)
      {dataspace, turn} = Entity.on_publish(dataspace, assertion, h2, turn)

      assert Dataspace.has_assertion?(dataspace, assertion)

      # Retract first - should still exist (refcount > 0)
      {dataspace, turn} = Entity.on_retract(dataspace, h1, turn)
      assert Dataspace.has_assertion?(dataspace, assertion)

      # Retract second - now should be gone
      {dataspace, _turn} = Entity.on_retract(dataspace, h2, turn)
      refute Dataspace.has_assertion?(dataspace, assertion)
    end
  end

  describe "dataspace pattern matching and observers" do
    test "observer receives existing matching assertions" do
      dataspace = Dataspace.new()
      turn = Turn.new(:test_actor, :test_facet)

      # First publish some assertions
      h1 = Handle.new(1)
      person1 = Value.record(Value.symbol("Person"), [Value.string("Alice"), Value.integer(30)])
      {dataspace, turn} = Entity.on_publish(dataspace, person1, h1, turn)

      h2 = Handle.new(2)
      person2 = Value.record(Value.symbol("Person"), [Value.string("Bob"), Value.integer(25)])
      {dataspace, turn} = Entity.on_publish(dataspace, person2, h2, turn)

      # Create an Observe assertion for Person records
      # Pattern matches any Person record
      pattern = Value.record(Value.symbol("Person"), [Value.symbol("_"), Value.symbol("_")])
      observer_ref = %Absynthe.Core.Ref{actor_id: :observer_actor, entity_id: :observer_entity}
      observe_assertion = Value.record(
        Value.symbol("Observe"),
        [pattern, {:embedded, observer_ref}]
      )

      observe_handle = Handle.new(100)
      {_dataspace, updated_turn} = Entity.on_publish(dataspace, observe_assertion, observe_handle, turn)

      # The turn should have actions to notify observer of existing matches
      {_committed, actions} = Turn.commit(updated_turn)

      # Should have 2 Assert actions (one for each existing Person)
      assert_actions = Enum.filter(actions, &match?(%Absynthe.Protocol.Event.Assert{}, &1))
      assert length(assert_actions) == 2
    end

    test "observer is notified when new matching assertion arrives" do
      dataspace = Dataspace.new()
      turn = Turn.new(:test_actor, :test_facet)

      # First set up observer
      pattern = Value.record(Value.symbol("Message"), [Value.symbol("_")])
      observer_ref = %Absynthe.Core.Ref{actor_id: :observer, entity_id: 0}
      observe_assertion = Value.record(
        Value.symbol("Observe"),
        [pattern, {:embedded, observer_ref}]
      )

      observe_handle = Handle.new(1)
      {dataspace, _turn} = Entity.on_publish(dataspace, observe_assertion, observe_handle, turn)

      # Create fresh turn for the next operation
      turn = Turn.new(:test_actor, :test_facet)

      # Now publish a matching assertion
      message = Value.record(Value.symbol("Message"), [Value.string("hello")])
      msg_handle = Handle.new(2)
      {_dataspace, updated_turn} = Entity.on_publish(dataspace, message, msg_handle, turn)

      # Turn should have action to notify observer
      {_committed, actions} = Turn.commit(updated_turn)

      assert_actions = Enum.filter(actions, &match?(%Absynthe.Protocol.Event.Assert{}, &1))
      assert length(assert_actions) == 1

      [notify_action] = assert_actions
      assert notify_action.ref == observer_ref
      assert notify_action.assertion == message
    end

    test "observer is notified on retraction" do
      dataspace = Dataspace.new()
      turn = Turn.new(:test_actor, :test_facet)

      # Publish assertion first
      message = Value.record(Value.symbol("Msg"), [Value.integer(1)])
      msg_handle = Handle.new(1)
      {dataspace, turn} = Entity.on_publish(dataspace, message, msg_handle, turn)

      # Set up observer
      pattern = Value.record(Value.symbol("Msg"), [Value.symbol("_")])
      observer_ref = %Absynthe.Core.Ref{actor_id: :observer, entity_id: 0}
      observe_assertion = Value.record(
        Value.symbol("Observe"),
        [pattern, {:embedded, observer_ref}]
      )
      observe_handle = Handle.new(100)
      {dataspace, _turn} = Entity.on_publish(dataspace, observe_assertion, observe_handle, turn)

      # Fresh turn for retraction
      turn = Turn.new(:test_actor, :test_facet)

      # Retract the message
      {_dataspace, updated_turn} = Entity.on_retract(dataspace, msg_handle, turn)

      # Turn should have Retract action to notify observer
      {_committed, actions} = Turn.commit(updated_turn)

      retract_actions = Enum.filter(actions, &match?(%Absynthe.Protocol.Event.Retract{}, &1))
      assert length(retract_actions) == 1

      [notify_action] = retract_actions
      assert notify_action.ref == observer_ref
      assert notify_action.handle == msg_handle
    end

    test "removing observer stops notifications" do
      dataspace = Dataspace.new()
      turn = Turn.new(:test_actor, :test_facet)

      # Set up observer
      pattern = Value.symbol("_")  # Match anything
      observer_ref = %Absynthe.Core.Ref{actor_id: :observer, entity_id: 0}
      observe_assertion = Value.record(
        Value.symbol("Observe"),
        [pattern, {:embedded, observer_ref}]
      )
      observe_handle = Handle.new(1)
      {dataspace, _turn} = Entity.on_publish(dataspace, observe_assertion, observe_handle, turn)

      # Remove observer
      turn = Turn.new(:test_actor, :test_facet)
      {dataspace, _turn} = Entity.on_retract(dataspace, observe_handle, turn)

      # Now publish something - should NOT generate observer notification
      turn = Turn.new(:test_actor, :test_facet)
      {_dataspace, updated_turn} = Entity.on_publish(dataspace, Value.string("test"), Handle.new(2), turn)

      {_committed, actions} = Turn.commit(updated_turn)

      # No assert actions should be generated (observer was removed)
      assert_actions = Enum.filter(actions, &match?(%Absynthe.Protocol.Event.Assert{}, &1))
      assert length(assert_actions) == 0
    end
  end

  describe "dataspace query" do
    test "query returns matching assertions with captures" do
      dataspace = Dataspace.new()
      turn = Turn.new(:test_actor, :test_facet)

      # Add some assertions
      {dataspace, turn} = Entity.on_publish(
        dataspace,
        Value.record(Value.symbol("User"), [Value.string("alice"), Value.integer(30)]),
        Handle.new(1),
        turn
      )
      {dataspace, turn} = Entity.on_publish(
        dataspace,
        Value.record(Value.symbol("User"), [Value.string("bob"), Value.integer(25)]),
        Handle.new(2),
        turn
      )
      {dataspace, _turn} = Entity.on_publish(
        dataspace,
        Value.record(Value.symbol("Other"), [Value.string("x")]),
        Handle.new(3),
        turn
      )

      # Query for User records
      pattern = Value.record(Value.symbol("User"), [Value.symbol("_"), Value.symbol("_")])
      results = Dataspace.query(dataspace, pattern)

      # Should find 2 User records
      assert length(results) == 2
    end
  end
end
