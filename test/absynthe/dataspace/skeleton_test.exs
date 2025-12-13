defmodule Absynthe.Dataspace.SkeletonTest do
  @moduledoc """
  Tests for the Skeleton index, including the observer indexing optimization
  that avoids O(n) scans of all observers on each assertion change.
  """
  use ExUnit.Case, async: true

  alias Absynthe.Dataspace.{Skeleton, Pattern}
  alias Absynthe.Assertions.Handle

  describe "new/0" do
    test "creates a skeleton with empty tables" do
      skeleton = Skeleton.new()

      assert is_reference(skeleton.path_index)
      assert is_reference(skeleton.assertions)
      assert is_reference(skeleton.observers)
      assert is_reference(skeleton.observer_index)
      assert skeleton.wildcard_observers == MapSet.new()
      assert skeleton.owner == self()
    end
  end

  describe "add_observer/4 and remove_observer/2 with observer indexing" do
    test "observer with constraints is indexed by path/value_class" do
      skeleton = Skeleton.new()

      # Pattern: <Person $name $age> - has constraint on label
      pattern =
        Pattern.compile({:record, {{:symbol, "Person"}, [{:symbol, "$name"}, {:symbol, "$age"}]}})

      observer_id = :test_observer
      observer_ref = make_ref()

      {updated_skeleton, matches} =
        Skeleton.add_observer(skeleton, observer_id, observer_ref, pattern)

      # No existing assertions, so no matches
      assert matches == []

      # Observer should NOT be in wildcard set (it has constraints)
      refute MapSet.member?(updated_skeleton.wildcard_observers, observer_id)

      # Observer should be indexed in observer_index
      # The constraint is on [:label] with value {:symbol, "Person"}
      index_key = {[:label], :symbol}

      case :ets.lookup(updated_skeleton.observer_index, index_key) do
        [{^index_key, entries}] ->
          assert MapSet.member?(entries, {observer_id, {:symbol, "Person"}})

        [] ->
          flunk("Expected observer to be indexed at #{inspect(index_key)}")
      end

      # Cleanup
      Skeleton.destroy(updated_skeleton)
    end

    test "wildcard observer is added to wildcard set" do
      skeleton = Skeleton.new()

      # Pattern: $ (capture anything) - no constraints
      pattern = Pattern.compile({:symbol, "$"})

      observer_id = :wildcard_observer
      observer_ref = make_ref()

      {updated_skeleton, _matches} =
        Skeleton.add_observer(skeleton, observer_id, observer_ref, pattern)

      # Observer should be in wildcard set
      assert MapSet.member?(updated_skeleton.wildcard_observers, observer_id)

      # Cleanup
      Skeleton.destroy(updated_skeleton)
    end

    test "remove_observer removes from observer_index" do
      skeleton = Skeleton.new()

      pattern =
        Pattern.compile({:record, {{:symbol, "User"}, [{:symbol, "$"}]}})

      observer_id = :indexed_observer
      observer_ref = make_ref()

      {skeleton, _matches} = Skeleton.add_observer(skeleton, observer_id, observer_ref, pattern)

      # Verify it's indexed
      index_key = {[:label], :symbol}

      case :ets.lookup(skeleton.observer_index, index_key) do
        [{^index_key, entries}] ->
          assert MapSet.member?(entries, {observer_id, {:symbol, "User"}})

        [] ->
          flunk("Expected observer to be indexed")
      end

      # Remove observer
      updated_skeleton = Skeleton.remove_observer(skeleton, observer_id)

      # Should be removed from index
      case :ets.lookup(updated_skeleton.observer_index, index_key) do
        [{^index_key, entries}] ->
          refute MapSet.member?(entries, {observer_id, {:symbol, "User"}})

        [] ->
          # Entry was deleted because it became empty - this is fine
          :ok
      end

      # Cleanup
      Skeleton.destroy(updated_skeleton)
    end

    test "remove_observer removes wildcard observer from wildcard set" do
      skeleton = Skeleton.new()

      # Wildcard pattern
      pattern = Pattern.compile({:symbol, "_"})

      observer_id = :wildcard_to_remove
      observer_ref = make_ref()

      {skeleton, _matches} = Skeleton.add_observer(skeleton, observer_id, observer_ref, pattern)

      assert MapSet.member?(skeleton.wildcard_observers, observer_id)

      updated_skeleton = Skeleton.remove_observer(skeleton, observer_id)

      refute MapSet.member?(updated_skeleton.wildcard_observers, observer_id)

      # Cleanup
      Skeleton.destroy(updated_skeleton)
    end
  end

  describe "add_assertion/3 with indexed observer lookup" do
    test "finds matching observers using index" do
      skeleton = Skeleton.new()

      # Add an observer for Person records
      pattern =
        Pattern.compile({:record, {{:symbol, "Person"}, [{:symbol, "$name"}, {:symbol, "$age"}]}})

      observer_id = :person_observer
      observer_ref = make_ref()

      {skeleton, _matches} = Skeleton.add_observer(skeleton, observer_id, observer_ref, pattern)

      # Add an assertion that matches
      handle = Handle.new(:test_actor, 1)
      person = {:record, {{:symbol, "Person"}, [{:string, "Alice"}, {:integer, 30}]}}

      notifications = Skeleton.add_assertion(skeleton, handle, person)

      # Should notify the observer
      assert length(notifications) == 1
      [{notified_id, notified_ref, _value, captures}] = notifications
      assert notified_id == observer_id
      assert notified_ref == observer_ref
      assert captures == [{:string, "Alice"}, {:integer, 30}]

      # Cleanup
      Skeleton.destroy(skeleton)
    end

    test "does not notify observers that don't match" do
      skeleton = Skeleton.new()

      # Add an observer for Person records
      pattern =
        Pattern.compile({:record, {{:symbol, "Person"}, [{:symbol, "$name"}]}})

      observer_id = :person_observer
      observer_ref = make_ref()

      {skeleton, _matches} = Skeleton.add_observer(skeleton, observer_id, observer_ref, pattern)

      # Add an assertion with different label
      handle = Handle.new(:test_actor, 1)
      user = {:record, {{:symbol, "User"}, [{:string, "Bob"}]}}

      notifications = Skeleton.add_assertion(skeleton, handle, user)

      # Should NOT notify the observer (different label)
      assert notifications == []

      # Cleanup
      Skeleton.destroy(skeleton)
    end

    test "wildcard observers are always notified" do
      skeleton = Skeleton.new()

      # Add a wildcard observer
      pattern = Pattern.compile({:symbol, "$"})

      observer_id = :catch_all
      observer_ref = make_ref()

      {skeleton, _matches} = Skeleton.add_observer(skeleton, observer_id, observer_ref, pattern)

      # Add any assertion
      handle = Handle.new(:test_actor, 1)
      value = {:integer, 42}

      notifications = Skeleton.add_assertion(skeleton, handle, value)

      # Wildcard observer should be notified
      assert length(notifications) == 1
      [{notified_id, _notified_ref, notified_value, captures}] = notifications
      assert notified_id == observer_id
      assert notified_value == value
      assert captures == [{:integer, 42}]

      # Cleanup
      Skeleton.destroy(skeleton)
    end

    test "multiple observers with overlapping constraints" do
      skeleton = Skeleton.new()

      # Observer 1: Person records
      pattern1 =
        Pattern.compile({:record, {{:symbol, "Person"}, [{:symbol, "$"}, {:symbol, "$"}]}})

      {skeleton, _} = Skeleton.add_observer(skeleton, :observer1, make_ref(), pattern1)

      # Observer 2: Also Person records with specific first field
      pattern2 =
        Pattern.compile({:record, {{:symbol, "Person"}, [{:string, "Alice"}, {:symbol, "$"}]}})

      {skeleton, _} = Skeleton.add_observer(skeleton, :observer2, make_ref(), pattern2)

      # Observer 3: User records (different label)
      pattern3 =
        Pattern.compile({:record, {{:symbol, "User"}, [{:symbol, "$"}]}})

      {skeleton, _} = Skeleton.add_observer(skeleton, :observer3, make_ref(), pattern3)

      # Add a Person assertion for Alice
      handle = Handle.new(:test_actor, 1)
      person = {:record, {{:symbol, "Person"}, [{:string, "Alice"}, {:integer, 30}]}}

      notifications = Skeleton.add_assertion(skeleton, handle, person)

      # Should notify observer1 and observer2, but not observer3
      notified_ids = Enum.map(notifications, fn {id, _, _, _} -> id end)
      assert :observer1 in notified_ids
      assert :observer2 in notified_ids
      refute :observer3 in notified_ids

      # Cleanup
      Skeleton.destroy(skeleton)
    end
  end

  describe "atomic assertion matching" do
    test "observer for literal integer matches atomic assertion" do
      skeleton = Skeleton.new()

      # Pattern: literal integer 42 (has constraint at root path [])
      pattern = Pattern.compile({:integer, 42})

      observer_id = :int_observer
      observer_ref = make_ref()

      {skeleton, _matches} = Skeleton.add_observer(skeleton, observer_id, observer_ref, pattern)

      # Add matching atomic assertion
      handle = Handle.new(:test_actor, 1)
      value = {:integer, 42}

      notifications = Skeleton.add_assertion(skeleton, handle, value)

      # Should notify the observer
      assert length(notifications) == 1
      [{notified_id, notified_ref, notified_value, captures}] = notifications
      assert notified_id == observer_id
      assert notified_ref == observer_ref
      assert notified_value == value
      assert captures == []

      # Cleanup
      Skeleton.destroy(skeleton)
    end

    test "observer for literal integer does not match different integer" do
      skeleton = Skeleton.new()

      # Pattern: literal integer 42
      pattern = Pattern.compile({:integer, 42})

      observer_id = :int_observer
      observer_ref = make_ref()

      {skeleton, _matches} = Skeleton.add_observer(skeleton, observer_id, observer_ref, pattern)

      # Add non-matching atomic assertion
      handle = Handle.new(:test_actor, 1)
      value = {:integer, 99}

      notifications = Skeleton.add_assertion(skeleton, handle, value)

      # Should NOT notify the observer (different value)
      assert notifications == []

      # Cleanup
      Skeleton.destroy(skeleton)
    end

    test "observer for literal string matches atomic assertion" do
      skeleton = Skeleton.new()

      # Pattern: literal string "hello"
      pattern = Pattern.compile({:string, "hello"})

      observer_id = :str_observer
      observer_ref = make_ref()

      {skeleton, _matches} = Skeleton.add_observer(skeleton, observer_id, observer_ref, pattern)

      # Add matching atomic assertion
      handle = Handle.new(:test_actor, 1)
      value = {:string, "hello"}

      notifications = Skeleton.add_assertion(skeleton, handle, value)

      # Should notify the observer
      assert length(notifications) == 1
      [{notified_id, _, _, _}] = notifications
      assert notified_id == observer_id

      # Cleanup
      Skeleton.destroy(skeleton)
    end
  end

  describe "sequence assertion matching" do
    test "observer for sequence pattern matches sequence assertion" do
      skeleton = Skeleton.new()

      # Pattern: [$first, $second] - captures two elements
      pattern = Pattern.compile({:sequence, [{:symbol, "$first"}, {:symbol, "$second"}]})

      observer_id = :seq_observer
      observer_ref = make_ref()

      {skeleton, _matches} = Skeleton.add_observer(skeleton, observer_id, observer_ref, pattern)

      # Add matching sequence assertion
      handle = Handle.new(:test_actor, 1)
      value = {:sequence, [{:integer, 1}, {:integer, 2}]}

      notifications = Skeleton.add_assertion(skeleton, handle, value)

      # Should notify the observer with captures
      assert length(notifications) == 1
      [{notified_id, _, _, captures}] = notifications
      assert notified_id == observer_id
      assert captures == [{:integer, 1}, {:integer, 2}]

      # Cleanup
      Skeleton.destroy(skeleton)
    end

    test "observer with literal sequence element constraint" do
      skeleton = Skeleton.new()

      # Pattern: [1, $second] - first element must be 1
      pattern = Pattern.compile({:sequence, [{:integer, 1}, {:symbol, "$second"}]})

      observer_id = :seq_observer
      observer_ref = make_ref()

      {skeleton, _matches} = Skeleton.add_observer(skeleton, observer_id, observer_ref, pattern)

      # Add matching sequence assertion
      handle1 = Handle.new(:test_actor, 1)
      value1 = {:sequence, [{:integer, 1}, {:integer, 2}]}

      notifications1 = Skeleton.add_assertion(skeleton, handle1, value1)
      assert length(notifications1) == 1

      # Add non-matching sequence assertion (different first element)
      handle2 = Handle.new(:test_actor, 2)
      value2 = {:sequence, [{:integer, 99}, {:integer, 2}]}

      notifications2 = Skeleton.add_assertion(skeleton, handle2, value2)
      assert notifications2 == []

      # Cleanup
      Skeleton.destroy(skeleton)
    end
  end

  describe "dictionary assertion matching" do
    test "observer for dictionary pattern matches dictionary assertion" do
      skeleton = Skeleton.new()

      # Pattern: {name: $n} - captures value for "name" key
      pattern = Pattern.compile({:dictionary, [{{:symbol, "name"}, {:symbol, "$n"}}]})

      observer_id = :dict_observer
      observer_ref = make_ref()

      {skeleton, _matches} = Skeleton.add_observer(skeleton, observer_id, observer_ref, pattern)

      # Add matching dictionary assertion
      handle = Handle.new(:test_actor, 1)
      value = {:dictionary, [{{:symbol, "name"}, {:string, "Alice"}}]}

      notifications = Skeleton.add_assertion(skeleton, handle, value)

      # Should notify the observer with capture
      assert length(notifications) == 1
      [{notified_id, _, _, captures}] = notifications
      assert notified_id == observer_id
      assert captures == [{:string, "Alice"}]

      # Cleanup
      Skeleton.destroy(skeleton)
    end
  end

  describe "set assertion matching" do
    test "observer for set pattern matches set assertion" do
      skeleton = Skeleton.new()

      # Pattern: set containing three captures
      pattern =
        Pattern.compile({:set, MapSet.new([{:symbol, "$a"}, {:symbol, "$b"}, {:symbol, "$c"}])})

      observer_id = :set_observer
      observer_ref = make_ref()

      {skeleton, _matches} = Skeleton.add_observer(skeleton, observer_id, observer_ref, pattern)

      # Add matching set assertion (same size)
      handle = Handle.new(:test_actor, 1)
      value = {:set, MapSet.new([{:integer, 1}, {:integer, 2}, {:integer, 3}])}

      notifications = Skeleton.add_assertion(skeleton, handle, value)

      # Should notify the observer with captures (in sorted order)
      assert length(notifications) == 1

      # Cleanup
      Skeleton.destroy(skeleton)
    end
  end

  describe "label-wildcard record patterns" do
    test "observer with wildcard label matches any record" do
      skeleton = Skeleton.new()

      # Pattern: <$ $field> - wildcard label, capture field
      # This pattern has no label constraint, so should be wildcard
      pattern = Pattern.compile({:record, {{:symbol, "$"}, [{:symbol, "$field"}]}})

      observer_id = :any_record_observer
      observer_ref = make_ref()

      {skeleton, _matches} = Skeleton.add_observer(skeleton, observer_id, observer_ref, pattern)

      # This should be in wildcard observers since label is a capture (no constraint)
      assert MapSet.member?(skeleton.wildcard_observers, observer_id)

      # Add a User record
      handle1 = Handle.new(:test_actor, 1)
      user = {:record, {{:symbol, "User"}, [{:string, "Alice"}]}}

      notifications1 = Skeleton.add_assertion(skeleton, handle1, user)

      # Should notify the observer
      assert length(notifications1) == 1
      [{_, _, _, captures}] = notifications1
      assert captures == [{:symbol, "User"}, {:string, "Alice"}]

      # Add a Person record
      handle2 = Handle.new(:test_actor, 2)
      person = {:record, {{:symbol, "Person"}, [{:string, "Bob"}]}}

      notifications2 = Skeleton.add_assertion(skeleton, handle2, person)

      # Should also notify the observer
      assert length(notifications2) == 1

      # Cleanup
      Skeleton.destroy(skeleton)
    end
  end

  describe "performance characteristics" do
    test "observer index prevents full scan" do
      skeleton = Skeleton.new()

      # Add many observers with different labels
      # This simulates a scenario where full scan would be expensive
      skeleton =
        Enum.reduce(1..100, skeleton, fn i, skel ->
          label = "Type#{i}"
          pattern = Pattern.compile({:record, {{:symbol, label}, [{:symbol, "$"}]}})
          observer_id = {:observer, i}
          {updated_skel, _} = Skeleton.add_observer(skel, observer_id, make_ref(), pattern)
          updated_skel
        end)

      # Add an assertion that only matches Type42
      handle = Handle.new(:test_actor, 1)
      value = {:record, {{:symbol, "Type42"}, [{:string, "test"}]}}

      notifications = Skeleton.add_assertion(skeleton, handle, value)

      # Should only notify observer for Type42
      assert length(notifications) == 1
      [{notified_id, _, _, _}] = notifications
      assert notified_id == {:observer, 42}

      # Cleanup
      Skeleton.destroy(skeleton)
    end
  end

  describe "send_message/2" do
    test "routes message to observers whose patterns match" do
      skeleton = Skeleton.new()

      # Add an observer for Ping messages
      pattern = Pattern.compile({:record, {{:symbol, "Ping"}, [{:symbol, "$id"}]}})
      observer_id = :ping_observer
      observer_ref = make_ref()

      {skeleton, _matches} = Skeleton.add_observer(skeleton, observer_id, observer_ref, pattern)

      # Send a matching message
      message = {:record, {{:symbol, "Ping"}, [{:integer, 42}]}}
      matches = Skeleton.send_message(skeleton, message)

      # Should find the matching observer
      assert length(matches) == 1
      [{matched_id, matched_ref, captures}] = matches
      assert matched_id == observer_id
      assert matched_ref == observer_ref
      assert captures == [{:integer, 42}]

      # Cleanup
      Skeleton.destroy(skeleton)
    end

    test "does not route message to observers that don't match" do
      skeleton = Skeleton.new()

      # Add an observer for Ping messages
      pattern = Pattern.compile({:record, {{:symbol, "Ping"}, [{:symbol, "$id"}]}})
      observer_id = :ping_observer
      observer_ref = make_ref()

      {skeleton, _matches} = Skeleton.add_observer(skeleton, observer_id, observer_ref, pattern)

      # Send a non-matching message (different label)
      message = {:record, {{:symbol, "Pong"}, [{:integer, 42}]}}
      matches = Skeleton.send_message(skeleton, message)

      # Should NOT find the observer (different label)
      assert matches == []

      # Cleanup
      Skeleton.destroy(skeleton)
    end

    test "routes message to multiple matching observers" do
      skeleton = Skeleton.new()

      # Add observer 1: Any Ping message
      pattern1 = Pattern.compile({:record, {{:symbol, "Ping"}, [{:symbol, "$"}]}})
      {skeleton, _} = Skeleton.add_observer(skeleton, :observer1, make_ref(), pattern1)

      # Add observer 2: Ping messages with specific value
      pattern2 = Pattern.compile({:record, {{:symbol, "Ping"}, [{:integer, 42}]}})
      {skeleton, _} = Skeleton.add_observer(skeleton, :observer2, make_ref(), pattern2)

      # Add observer 3: Different message type
      pattern3 = Pattern.compile({:record, {{:symbol, "Pong"}, [{:symbol, "$"}]}})
      {skeleton, _} = Skeleton.add_observer(skeleton, :observer3, make_ref(), pattern3)

      # Send a Ping with id=42
      message = {:record, {{:symbol, "Ping"}, [{:integer, 42}]}}
      matches = Skeleton.send_message(skeleton, message)

      # Should notify observer1 and observer2, but not observer3
      matched_ids = Enum.map(matches, fn {id, _, _} -> id end)
      assert :observer1 in matched_ids
      assert :observer2 in matched_ids
      refute :observer3 in matched_ids

      # Cleanup
      Skeleton.destroy(skeleton)
    end

    test "wildcard observer receives all messages" do
      skeleton = Skeleton.new()

      # Add a wildcard observer (matches anything)
      pattern = Pattern.compile({:symbol, "$"})
      observer_id = :catch_all
      observer_ref = make_ref()

      {skeleton, _matches} = Skeleton.add_observer(skeleton, observer_id, observer_ref, pattern)

      # Send any message
      message = {:string, "hello world"}
      matches = Skeleton.send_message(skeleton, message)

      # Wildcard observer should receive it
      assert length(matches) == 1
      [{matched_id, _, captures}] = matches
      assert matched_id == observer_id
      assert captures == [{:string, "hello world"}]

      # Cleanup
      Skeleton.destroy(skeleton)
    end

    test "message is not stored (unlike assertions)" do
      skeleton = Skeleton.new()

      # Send a message without any observers
      message = {:string, "test"}
      matches = Skeleton.send_message(skeleton, message)

      # No observers, so no matches
      assert matches == []

      # The message should NOT be stored in the assertions table
      all_assertions = :ets.tab2list(skeleton.assertions)
      assert all_assertions == []

      # Cleanup
      Skeleton.destroy(skeleton)
    end
  end

  describe "destroy/1" do
    test "cleans up all ETS tables including observer_index" do
      skeleton = Skeleton.new()

      path_index = skeleton.path_index
      assertions = skeleton.assertions
      observers = skeleton.observers
      observer_index = skeleton.observer_index

      :ok = Skeleton.destroy(skeleton)

      # All tables should be deleted
      refute :ets.info(path_index) != :undefined
      refute :ets.info(assertions) != :undefined
      refute :ets.info(observers) != :undefined
      refute :ets.info(observer_index) != :undefined
    end
  end
end
