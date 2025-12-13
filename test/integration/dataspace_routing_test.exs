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
      handle = Handle.new(:test_actor, 1)
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

      h1 = Handle.new(:test_actor, 1)
      h2 = Handle.new(:test_actor, 2)
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
      handle = Handle.new(:test_actor, 1)
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
      h1 = Handle.new(:test_actor, 1)
      h2 = Handle.new(:test_actor, 2)

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
      h1 = Handle.new(:test_actor, 1)
      person1 = Value.record(Value.symbol("Person"), [Value.string("Alice"), Value.integer(30)])
      {dataspace, turn} = Entity.on_publish(dataspace, person1, h1, turn)

      h2 = Handle.new(:test_actor, 2)
      person2 = Value.record(Value.symbol("Person"), [Value.string("Bob"), Value.integer(25)])
      {dataspace, turn} = Entity.on_publish(dataspace, person2, h2, turn)

      # Create an Observe assertion for Person records
      # Pattern matches any Person record
      pattern = Value.record(Value.symbol("Person"), [Value.symbol("_"), Value.symbol("_")])
      observer_ref = %Absynthe.Core.Ref{actor_id: :observer_actor, entity_id: :observer_entity}

      observe_assertion =
        Value.record(
          Value.symbol("Observe"),
          [pattern, {:embedded, observer_ref}]
        )

      observe_handle = Handle.new(:test_actor, 100)

      {_dataspace, updated_turn} =
        Entity.on_publish(dataspace, observe_assertion, observe_handle, turn)

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

      observe_assertion =
        Value.record(
          Value.symbol("Observe"),
          [pattern, {:embedded, observer_ref}]
        )

      observe_handle = Handle.new(:test_actor, 1)
      {dataspace, _turn} = Entity.on_publish(dataspace, observe_assertion, observe_handle, turn)

      # Create fresh turn for the next operation
      turn = Turn.new(:test_actor, :test_facet)

      # Now publish a matching assertion
      message = Value.record(Value.symbol("Message"), [Value.string("hello")])
      msg_handle = Handle.new(:test_actor, 2)
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
      msg_handle = Handle.new(:test_actor, 1)
      {dataspace, turn} = Entity.on_publish(dataspace, message, msg_handle, turn)

      # Set up observer
      pattern = Value.record(Value.symbol("Msg"), [Value.symbol("_")])
      observer_ref = %Absynthe.Core.Ref{actor_id: :observer, entity_id: 0}

      observe_assertion =
        Value.record(
          Value.symbol("Observe"),
          [pattern, {:embedded, observer_ref}]
        )

      observe_handle = Handle.new(:test_actor, 100)
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
      # Match anything
      pattern = Value.symbol("_")
      observer_ref = %Absynthe.Core.Ref{actor_id: :observer, entity_id: 0}

      observe_assertion =
        Value.record(
          Value.symbol("Observe"),
          [pattern, {:embedded, observer_ref}]
        )

      observe_handle = Handle.new(:test_actor, 1)
      {dataspace, _turn} = Entity.on_publish(dataspace, observe_assertion, observe_handle, turn)

      # Remove observer
      turn = Turn.new(:test_actor, :test_facet)
      {dataspace, _turn} = Entity.on_retract(dataspace, observe_handle, turn)

      # Now publish something - should NOT generate observer notification
      turn = Turn.new(:test_actor, :test_facet)

      {_dataspace, updated_turn} =
        Entity.on_publish(dataspace, Value.string("test"), Handle.new(:test_actor, 2), turn)

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
      {dataspace, turn} =
        Entity.on_publish(
          dataspace,
          Value.record(Value.symbol("User"), [Value.string("alice"), Value.integer(30)]),
          Handle.new(:test_actor, 1),
          turn
        )

      {dataspace, turn} =
        Entity.on_publish(
          dataspace,
          Value.record(Value.symbol("User"), [Value.string("bob"), Value.integer(25)]),
          Handle.new(:test_actor, 2),
          turn
        )

      {dataspace, _turn} =
        Entity.on_publish(
          dataspace,
          Value.record(Value.symbol("Other"), [Value.string("x")]),
          Handle.new(:test_actor, 3),
          turn
        )

      # Query for User records
      pattern = Value.record(Value.symbol("User"), [Value.symbol("_"), Value.symbol("_")])
      results = Dataspace.query(dataspace, pattern)

      # Should find 2 User records
      assert length(results) == 2
    end
  end

  describe "observer active_handles tracking" do
    test "observer tracks active handles for existing matching assertions" do
      dataspace = Dataspace.new()
      turn = Turn.new(:test_actor, :test_facet)

      # First publish some assertions
      h1 = Handle.new(:test_actor, 1)
      person1 = Value.record(Value.symbol("Person"), [Value.string("Alice"), Value.integer(30)])
      {dataspace, turn} = Entity.on_publish(dataspace, person1, h1, turn)

      h2 = Handle.new(:test_actor, 2)
      person2 = Value.record(Value.symbol("Person"), [Value.string("Bob"), Value.integer(25)])
      {dataspace, turn} = Entity.on_publish(dataspace, person2, h2, turn)

      # Create an Observe assertion for Person records
      pattern = Value.record(Value.symbol("Person"), [Value.symbol("_"), Value.symbol("_")])
      observer_ref = %Absynthe.Core.Ref{actor_id: :observer_actor, entity_id: :observer_entity}

      observe_assertion =
        Value.record(
          Value.symbol("Observe"),
          [pattern, {:embedded, observer_ref}]
        )

      observe_handle = Handle.new(:test_actor, 100)

      {updated_dataspace, _turn} =
        Entity.on_publish(dataspace, observe_assertion, observe_handle, turn)

      # The observer should now track both h1 and h2 as active handles
      observer_id = {:observer, observe_handle}
      observer = Map.get(updated_dataspace.observers, observer_id)

      assert observer != nil
      assert Absynthe.Dataspace.Observer.match_count(observer) == 2
      assert Absynthe.Dataspace.Observer.matches?(observer, h1)
      assert Absynthe.Dataspace.Observer.matches?(observer, h2)
    end

    test "observer tracks new assertion handles as they arrive" do
      dataspace = Dataspace.new()
      turn = Turn.new(:test_actor, :test_facet)

      # First set up observer
      pattern = Value.record(Value.symbol("Message"), [Value.symbol("_")])
      observer_ref = %Absynthe.Core.Ref{actor_id: :observer, entity_id: 0}

      observe_assertion =
        Value.record(
          Value.symbol("Observe"),
          [pattern, {:embedded, observer_ref}]
        )

      observe_handle = Handle.new(:test_actor, 1)
      {dataspace, _turn} = Entity.on_publish(dataspace, observe_assertion, observe_handle, turn)

      # Check observer starts with no active handles
      observer_id = {:observer, observe_handle}
      observer = Map.get(dataspace.observers, observer_id)
      assert Absynthe.Dataspace.Observer.match_count(observer) == 0

      # Now publish a matching assertion
      turn = Turn.new(:test_actor, :test_facet)
      message = Value.record(Value.symbol("Message"), [Value.string("hello")])
      msg_handle = Handle.new(:test_actor, 2)
      {dataspace, _turn} = Entity.on_publish(dataspace, message, msg_handle, turn)

      # Observer should now track the new handle
      observer = Map.get(dataspace.observers, observer_id)
      assert Absynthe.Dataspace.Observer.match_count(observer) == 1
      assert Absynthe.Dataspace.Observer.matches?(observer, msg_handle)
    end

    test "observer removes handles on retraction" do
      dataspace = Dataspace.new()
      turn = Turn.new(:test_actor, :test_facet)

      # Publish assertion first
      message = Value.record(Value.symbol("Msg"), [Value.integer(1)])
      msg_handle = Handle.new(:test_actor, 1)
      {dataspace, turn} = Entity.on_publish(dataspace, message, msg_handle, turn)

      # Set up observer
      pattern = Value.record(Value.symbol("Msg"), [Value.symbol("_")])
      observer_ref = %Absynthe.Core.Ref{actor_id: :observer, entity_id: 0}

      observe_assertion =
        Value.record(
          Value.symbol("Observe"),
          [pattern, {:embedded, observer_ref}]
        )

      observe_handle = Handle.new(:test_actor, 100)
      {dataspace, _turn} = Entity.on_publish(dataspace, observe_assertion, observe_handle, turn)

      # Observer should track the message handle
      observer_id = {:observer, observe_handle}
      observer = Map.get(dataspace.observers, observer_id)
      assert Absynthe.Dataspace.Observer.match_count(observer) == 1
      assert Absynthe.Dataspace.Observer.matches?(observer, msg_handle)

      # Fresh turn for retraction
      turn = Turn.new(:test_actor, :test_facet)

      # Retract the message
      {dataspace, _turn} = Entity.on_retract(dataspace, msg_handle, turn)

      # Observer should no longer track the handle
      observer = Map.get(dataspace.observers, observer_id)
      assert Absynthe.Dataspace.Observer.match_count(observer) == 0
      refute Absynthe.Dataspace.Observer.matches?(observer, msg_handle)
    end

    test "multi-handle retraction correctly cleans up active_handles" do
      # This test verifies the fix for the refcounted assertion bug:
      # When the same assertion value is published with multiple handles,
      # retracting the non-canonical handle last should still properly
      # clean up the skeleton and observer active_handles.

      dataspace = Dataspace.new()
      turn = Turn.new(:test_actor, :test_facet)

      # Set up observer first
      pattern = Value.record(Value.symbol("Data"), [Value.symbol("_")])
      observer_ref = %Absynthe.Core.Ref{actor_id: :observer, entity_id: 0}

      observe_assertion =
        Value.record(
          Value.symbol("Observe"),
          [pattern, {:embedded, observer_ref}]
        )

      observe_handle = Handle.new(:test_actor, 100)
      {dataspace, turn} = Entity.on_publish(dataspace, observe_assertion, observe_handle, turn)

      # Publish same assertion value with two different handles
      data_value = Value.record(Value.symbol("Data"), [Value.integer(42)])
      h1 = Handle.new(:test_actor, 1)
      h2 = Handle.new(:test_actor, 2)

      # H1 is the canonical handle (first one added to skeleton)
      {dataspace, turn} = Entity.on_publish(dataspace, data_value, h1, turn)
      # H2 increments ref count but doesn't add to skeleton
      {dataspace, _turn} = Entity.on_publish(dataspace, data_value, h2, turn)

      # Observer should track the canonical handle (H1)
      observer_id = {:observer, observe_handle}
      observer = Map.get(dataspace.observers, observer_id)
      assert Absynthe.Dataspace.Observer.match_count(observer) == 1
      assert Absynthe.Dataspace.Observer.matches?(observer, h1)
      # H2 is NOT in active_handles (only canonical handle is tracked)
      refute Absynthe.Dataspace.Observer.matches?(observer, h2)

      # Retract H1 first (decrements count, assertion still exists)
      turn = Turn.new(:test_actor, :test_facet)
      {dataspace, turn} = Entity.on_retract(dataspace, h1, turn)

      # Observer should STILL have the match (assertion not fully removed)
      observer = Map.get(dataspace.observers, observer_id)
      assert Absynthe.Dataspace.Observer.match_count(observer) == 1

      # Retract H2 (last handle - should remove from skeleton and notify)
      {dataspace, updated_turn} = Entity.on_retract(dataspace, h2, turn)

      # Observer should now have NO matches
      observer = Map.get(dataspace.observers, observer_id)
      assert Absynthe.Dataspace.Observer.match_count(observer) == 0

      # Verify retraction notification was generated
      {_committed, actions} = Turn.commit(updated_turn)
      retract_actions = Enum.filter(actions, &match?(%Absynthe.Protocol.Event.Retract{}, &1))
      assert length(retract_actions) == 1
    end
  end

  describe "observer capture bindings" do
    test "observer notifications include capture bindings" do
      dataspace = Dataspace.new()
      turn = Turn.new(:test_actor, :test_facet)

      # First publish some assertions with specific values we want to capture
      h1 = Handle.new(:test_actor, 1)
      person1 = Value.record(Value.symbol("Person"), [Value.string("Alice"), Value.integer(30)])
      {dataspace, turn} = Entity.on_publish(dataspace, person1, h1, turn)

      h2 = Handle.new(:test_actor, 2)
      person2 = Value.record(Value.symbol("Person"), [Value.string("Bob"), Value.integer(25)])
      {dataspace, turn} = Entity.on_publish(dataspace, person2, h2, turn)

      # Create an Observe assertion with captures for name and age
      # Pattern: <Person $name $age> - uses Preserves pattern syntax
      pattern =
        Value.record(Value.symbol("Person"), [
          Value.symbol("$name"),
          Value.symbol("$age")
        ])

      observer_ref = %Absynthe.Core.Ref{actor_id: :observer_actor, entity_id: :observer_entity}

      observe_assertion =
        Value.record(
          Value.symbol("Observe"),
          [pattern, {:embedded, observer_ref}]
        )

      observe_handle = Handle.new(:test_actor, 100)

      {_dataspace, updated_turn} =
        Entity.on_publish(dataspace, observe_assertion, observe_handle, turn)

      # The turn should have actions with captured bindings
      {_committed, actions} = Turn.commit(updated_turn)

      assert_actions = Enum.filter(actions, &match?(%Absynthe.Protocol.Event.Assert{}, &1))
      assert length(assert_actions) == 2

      # Check that each action has captures
      Enum.each(assert_actions, fn action ->
        assert is_list(action.captures)
        assert length(action.captures) == 2

        # Captures should be [name_value, age_value]
        [name, age] = action.captures
        assert match?({:string, _}, name)
        assert match?({:integer, _}, age)
      end)

      # Verify specific captures match the assertions
      alice_action = Enum.find(assert_actions, fn a -> a.assertion == person1 end)
      assert alice_action.captures == [Value.string("Alice"), Value.integer(30)]

      bob_action = Enum.find(assert_actions, fn a -> a.assertion == person2 end)
      assert bob_action.captures == [Value.string("Bob"), Value.integer(25)]
    end

    test "new assertion notifications include capture bindings" do
      dataspace = Dataspace.new()
      turn = Turn.new(:test_actor, :test_facet)

      # First set up observer with captures using Preserves pattern syntax ($value)
      pattern =
        Value.record(Value.symbol("Counter"), [
          Value.symbol("$value")
        ])

      observer_ref = %Absynthe.Core.Ref{actor_id: :observer, entity_id: 0}

      observe_assertion =
        Value.record(
          Value.symbol("Observe"),
          [pattern, {:embedded, observer_ref}]
        )

      observe_handle = Handle.new(:test_actor, 1)
      {dataspace, _turn} = Entity.on_publish(dataspace, observe_assertion, observe_handle, turn)

      # Create fresh turn for the next operation
      turn = Turn.new(:test_actor, :test_facet)

      # Now publish a matching assertion
      counter = Value.record(Value.symbol("Counter"), [Value.integer(42)])
      counter_handle = Handle.new(:test_actor, 2)
      {_dataspace, updated_turn} = Entity.on_publish(dataspace, counter, counter_handle, turn)

      # Turn should have action with captures
      {_committed, actions} = Turn.commit(updated_turn)

      assert_actions = Enum.filter(actions, &match?(%Absynthe.Protocol.Event.Assert{}, &1))
      assert length(assert_actions) == 1

      [notify_action] = assert_actions
      assert notify_action.ref == observer_ref
      assert notify_action.assertion == counter
      assert notify_action.captures == [Value.integer(42)]
    end

    test "notifications without captures have empty captures list" do
      dataspace = Dataspace.new()
      turn = Turn.new(:test_actor, :test_facet)

      # Set up observer WITHOUT captures (just wildcards)
      pattern = Value.record(Value.symbol("Simple"), [Value.symbol("_")])
      observer_ref = %Absynthe.Core.Ref{actor_id: :observer, entity_id: 0}

      observe_assertion =
        Value.record(
          Value.symbol("Observe"),
          [pattern, {:embedded, observer_ref}]
        )

      observe_handle = Handle.new(:test_actor, 1)
      {dataspace, _turn} = Entity.on_publish(dataspace, observe_assertion, observe_handle, turn)

      # Create fresh turn for the next operation
      turn = Turn.new(:test_actor, :test_facet)

      # Publish a matching assertion
      simple = Value.record(Value.symbol("Simple"), [Value.string("test")])
      simple_handle = Handle.new(:test_actor, 2)
      {_dataspace, updated_turn} = Entity.on_publish(dataspace, simple, simple_handle, turn)

      {_committed, actions} = Turn.commit(updated_turn)

      assert_actions = Enum.filter(actions, &match?(%Absynthe.Protocol.Event.Assert{}, &1))
      assert length(assert_actions) == 1

      [notify_action] = assert_actions
      # No captures because pattern used wildcards, not captures
      assert notify_action.captures == []
    end
  end

  describe "dataspace message routing" do
    test "messages are routed to observers whose patterns match" do
      dataspace = Dataspace.new()
      turn = Turn.new(:test_actor, :test_facet)

      # Set up observer for Ping messages
      pattern = Value.record(Value.symbol("Ping"), [Value.symbol("$id")])
      observer_ref = %Absynthe.Core.Ref{actor_id: :observer, entity_id: 0}

      observe_assertion =
        Value.record(
          Value.symbol("Observe"),
          [pattern, {:embedded, observer_ref}]
        )

      observe_handle = Handle.new(:test_actor, 1)
      {dataspace, _turn} = Entity.on_publish(dataspace, observe_assertion, observe_handle, turn)

      # Send a message that matches the pattern
      turn = Turn.new(:test_actor, :test_facet)
      ping_message = Value.record(Value.symbol("Ping"), [Value.integer(42)])

      {_dataspace, updated_turn} = Entity.on_message(dataspace, ping_message, turn)

      # Turn should have a message action to notify the observer
      {_committed, actions} = Turn.commit(updated_turn)

      message_actions = Enum.filter(actions, &match?(%Absynthe.Protocol.Event.Message{}, &1))
      assert length(message_actions) == 1

      [notify_action] = message_actions
      assert notify_action.ref == observer_ref
      assert notify_action.body == ping_message
    end

    test "messages are NOT routed to observers that don't match" do
      dataspace = Dataspace.new()
      turn = Turn.new(:test_actor, :test_facet)

      # Set up observer for Ping messages
      pattern = Value.record(Value.symbol("Ping"), [Value.symbol("_")])
      observer_ref = %Absynthe.Core.Ref{actor_id: :observer, entity_id: 0}

      observe_assertion =
        Value.record(
          Value.symbol("Observe"),
          [pattern, {:embedded, observer_ref}]
        )

      observe_handle = Handle.new(:test_actor, 1)
      {dataspace, _turn} = Entity.on_publish(dataspace, observe_assertion, observe_handle, turn)

      # Send a message that does NOT match the pattern (different label)
      turn = Turn.new(:test_actor, :test_facet)
      pong_message = Value.record(Value.symbol("Pong"), [Value.integer(42)])

      {_dataspace, updated_turn} = Entity.on_message(dataspace, pong_message, turn)

      # Turn should have NO message actions (pattern doesn't match)
      {_committed, actions} = Turn.commit(updated_turn)

      message_actions = Enum.filter(actions, &match?(%Absynthe.Protocol.Event.Message{}, &1))
      assert message_actions == []
    end

    test "message is routed to multiple matching observers" do
      dataspace = Dataspace.new()
      turn = Turn.new(:test_actor, :test_facet)

      # Set up first observer for any Status message
      pattern1 = Value.record(Value.symbol("Status"), [Value.symbol("_")])
      observer_ref1 = %Absynthe.Core.Ref{actor_id: :observer1, entity_id: 0}

      observe1 =
        Value.record(
          Value.symbol("Observe"),
          [pattern1, {:embedded, observer_ref1}]
        )

      {dataspace, turn} = Entity.on_publish(dataspace, observe1, Handle.new(:test_actor, 1), turn)

      # Set up second observer with same pattern
      pattern2 = Value.record(Value.symbol("Status"), [Value.symbol("_")])
      observer_ref2 = %Absynthe.Core.Ref{actor_id: :observer2, entity_id: 0}

      observe2 =
        Value.record(
          Value.symbol("Observe"),
          [pattern2, {:embedded, observer_ref2}]
        )

      {dataspace, _turn} =
        Entity.on_publish(dataspace, observe2, Handle.new(:test_actor, 2), turn)

      # Send a status message
      turn = Turn.new(:test_actor, :test_facet)
      status_message = Value.record(Value.symbol("Status"), [Value.string("online")])

      {_dataspace, updated_turn} = Entity.on_message(dataspace, status_message, turn)

      {_committed, actions} = Turn.commit(updated_turn)

      message_actions = Enum.filter(actions, &match?(%Absynthe.Protocol.Event.Message{}, &1))
      # Both observers should receive the message
      assert length(message_actions) == 2

      refs = Enum.map(message_actions, & &1.ref)
      assert observer_ref1 in refs
      assert observer_ref2 in refs
    end

    test "message is NOT stored in dataspace (unlike assertions)" do
      dataspace = Dataspace.new()
      turn = Turn.new(:test_actor, :test_facet)

      # Send a message (without any observers)
      message = Value.string("hello world")
      {updated_dataspace, _turn} = Entity.on_message(dataspace, message, turn)

      # Message should NOT be stored as an assertion
      refute Dataspace.has_assertion?(updated_dataspace, message)
      assert Dataspace.assertions(updated_dataspace) == []
    end

    test "wildcard observer receives all messages" do
      dataspace = Dataspace.new()
      turn = Turn.new(:test_actor, :test_facet)

      # Set up wildcard observer (matches anything)
      pattern = Value.symbol("$")
      observer_ref = %Absynthe.Core.Ref{actor_id: :catch_all, entity_id: 0}

      observe_assertion =
        Value.record(
          Value.symbol("Observe"),
          [pattern, {:embedded, observer_ref}]
        )

      observe_handle = Handle.new(:test_actor, 1)
      {dataspace, _turn} = Entity.on_publish(dataspace, observe_assertion, observe_handle, turn)

      # Send various messages - all should be routed to the wildcard observer
      turn = Turn.new(:test_actor, :test_facet)
      msg1 = Value.string("string message")
      {dataspace, turn} = Entity.on_message(dataspace, msg1, turn)
      {_committed, actions1} = Turn.commit(turn)

      turn = Turn.new(:test_actor, :test_facet)
      msg2 = Value.integer(42)
      {dataspace, turn} = Entity.on_message(dataspace, msg2, turn)
      {_committed, actions2} = Turn.commit(turn)

      turn = Turn.new(:test_actor, :test_facet)
      msg3 = Value.record(Value.symbol("Record"), [Value.string("data")])
      {_dataspace, turn} = Entity.on_message(dataspace, msg3, turn)
      {_committed, actions3} = Turn.commit(turn)

      # All should have exactly one message action
      message_actions1 = Enum.filter(actions1, &match?(%Absynthe.Protocol.Event.Message{}, &1))
      message_actions2 = Enum.filter(actions2, &match?(%Absynthe.Protocol.Event.Message{}, &1))
      message_actions3 = Enum.filter(actions3, &match?(%Absynthe.Protocol.Event.Message{}, &1))

      assert length(message_actions1) == 1
      assert length(message_actions2) == 1
      assert length(message_actions3) == 1
    end

    test "removing observer stops message routing" do
      dataspace = Dataspace.new()
      turn = Turn.new(:test_actor, :test_facet)

      # Set up observer
      pattern = Value.symbol("_")
      observer_ref = %Absynthe.Core.Ref{actor_id: :observer, entity_id: 0}

      observe_assertion =
        Value.record(
          Value.symbol("Observe"),
          [pattern, {:embedded, observer_ref}]
        )

      observe_handle = Handle.new(:test_actor, 1)
      {dataspace, _turn} = Entity.on_publish(dataspace, observe_assertion, observe_handle, turn)

      # Remove observer
      turn = Turn.new(:test_actor, :test_facet)
      {dataspace, _turn} = Entity.on_retract(dataspace, observe_handle, turn)

      # Send a message - should NOT generate any notifications
      turn = Turn.new(:test_actor, :test_facet)
      message = Value.string("test")
      {_dataspace, updated_turn} = Entity.on_message(dataspace, message, turn)

      {_committed, actions} = Turn.commit(updated_turn)

      message_actions = Enum.filter(actions, &match?(%Absynthe.Protocol.Event.Message{}, &1))
      assert message_actions == []
    end
  end
end
