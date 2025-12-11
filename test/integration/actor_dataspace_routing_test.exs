defmodule Absynthe.Integration.ActorDataspaceRoutingTest do
  @moduledoc """
  End-to-end tests for SAM routing through actors and dataspaces.

  These tests verify the FULL SAM flow through the Actor mechanism:
  1. Actor creates a dataspace entity
  2. Actor creates observer entities
  3. Observers subscribe to the dataspace with patterns
  4. Assertions to the dataspace trigger observer notifications
  5. Retractions notify observers

  This differs from dataspace_routing_test.exs which tests the Dataspace
  entity directly without going through Actor routing.
  """
  use ExUnit.Case

  alias Absynthe.Core.{Actor, Entity, Ref}
  alias Absynthe.Dataspace.Dataspace
  alias Absynthe.Preserves.Value

  # Observer entity that collects received assertions
  defmodule ObserverEntity do
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

      def on_sync(entity, _peer, turn) do
        {entity, turn}
      end
    end
  end

  describe "actor-mediated dataspace routing" do
    test "assertion to dataspace is stored" do
      {:ok, actor} = Actor.start_link(id: :ds_test_1)

      # Spawn a dataspace entity in the actor
      dataspace = Dataspace.new()
      {:ok, ds_ref} = Actor.spawn_entity(actor, :root, dataspace)

      # Assert to the dataspace
      person = Value.record(Value.symbol("Person"), [Value.string("Alice"), Value.integer(30)])
      {:ok, _handle} = Actor.assert(actor, ds_ref, person)

      # Give time for processing
      Process.sleep(50)

      # Check the dataspace entity state
      state = :sys.get_state(actor)
      {_facet_id, updated_ds} = Map.get(state.entities, Ref.entity_id(ds_ref))

      # Dataspace should have the assertion
      assert Dataspace.has_assertion?(updated_ds, person)

      GenServer.stop(actor)
    end

    test "observer receives notification when matching assertion arrives" do
      {:ok, actor} = Actor.start_link(id: :ds_test_2)

      # Spawn dataspace and observer
      dataspace = Dataspace.new()
      {:ok, ds_ref} = Actor.spawn_entity(actor, :root, dataspace)

      observer = %ObserverEntity{}
      {:ok, observer_ref} = Actor.spawn_entity(actor, :root, observer)

      # Create an Observe assertion - observer subscribes to Person records
      # Pattern: match any Person record (using Dataspace.Pattern syntax)
      pattern = Value.record(Value.symbol("Person"), [Value.symbol("_"), Value.symbol("_")])

      observe_assertion = Value.record(
        Value.symbol("Observe"),
        [pattern, {:embedded, observer_ref}]
      )

      # Subscribe observer to the dataspace
      {:ok, _observe_handle} = Actor.assert(actor, ds_ref, observe_assertion)
      Process.sleep(50)

      # Now assert a matching Person to the dataspace
      person = Value.record(Value.symbol("Person"), [Value.string("Bob"), Value.integer(25)])
      {:ok, _person_handle} = Actor.assert(actor, ds_ref, person)

      # Give time for notification delivery
      Process.sleep(100)

      # Check observer received the notification
      state = :sys.get_state(actor)
      {_facet_id, updated_observer} = Map.get(state.entities, Ref.entity_id(observer_ref))

      # Observer should have received an assert notification for the Person
      assert_notifications = Enum.filter(updated_observer.received, fn
        {:assert, assertion, _handle} ->
          case assertion do
            {:record, {{:symbol, "Person"}, _}} -> true
            _ -> false
          end
        _ -> false
      end)

      assert length(assert_notifications) >= 1,
        "Observer should receive notification for Person assertion, got: #{inspect(updated_observer.received)}"

      GenServer.stop(actor)
    end

    test "observer receives notification for existing assertions when subscribing" do
      {:ok, actor} = Actor.start_link(id: :ds_test_3)

      # Spawn dataspace
      dataspace = Dataspace.new()
      {:ok, ds_ref} = Actor.spawn_entity(actor, :root, dataspace)

      # First, assert some data to the dataspace
      person1 = Value.record(Value.symbol("User"), [Value.string("Alice")])
      person2 = Value.record(Value.symbol("User"), [Value.string("Bob")])

      {:ok, _h1} = Actor.assert(actor, ds_ref, person1)
      {:ok, _h2} = Actor.assert(actor, ds_ref, person2)
      Process.sleep(50)

      # Now create observer and subscribe
      observer = %ObserverEntity{}
      {:ok, observer_ref} = Actor.spawn_entity(actor, :root, observer)

      # Subscribe to User records
      pattern = Value.record(Value.symbol("User"), [Value.symbol("_")])
      observe_assertion = Value.record(
        Value.symbol("Observe"),
        [pattern, {:embedded, observer_ref}]
      )

      {:ok, _observe_handle} = Actor.assert(actor, ds_ref, observe_assertion)
      Process.sleep(100)

      # Check observer received notifications for existing Users
      state = :sys.get_state(actor)
      {_facet_id, updated_observer} = Map.get(state.entities, Ref.entity_id(observer_ref))

      user_notifications = Enum.filter(updated_observer.received, fn
        {:assert, {:record, {{:symbol, "User"}, _}}, _handle} -> true
        _ -> false
      end)

      assert length(user_notifications) == 2,
        "Observer should receive 2 notifications for existing Users, got: #{inspect(updated_observer.received)}"

      GenServer.stop(actor)
    end

    test "observer receives retraction notification" do
      {:ok, actor} = Actor.start_link(id: :ds_test_4)

      # Spawn dataspace and observer
      dataspace = Dataspace.new()
      {:ok, ds_ref} = Actor.spawn_entity(actor, :root, dataspace)

      observer = %ObserverEntity{}
      {:ok, observer_ref} = Actor.spawn_entity(actor, :root, observer)

      # Subscribe observer to Status records
      pattern = Value.record(Value.symbol("Status"), [Value.symbol("_")])
      observe_assertion = Value.record(
        Value.symbol("Observe"),
        [pattern, {:embedded, observer_ref}]
      )

      {:ok, _observe_handle} = Actor.assert(actor, ds_ref, observe_assertion)
      Process.sleep(50)

      # Assert a Status
      status = Value.record(Value.symbol("Status"), [Value.symbol("online")])
      {:ok, status_handle} = Actor.assert(actor, ds_ref, status)
      Process.sleep(50)

      # Now retract the Status
      :ok = Actor.retract(actor, status_handle)
      Process.sleep(100)

      # Check observer received retraction
      state = :sys.get_state(actor)
      {_facet_id, updated_observer} = Map.get(state.entities, Ref.entity_id(observer_ref))

      retract_notifications = Enum.filter(updated_observer.received, &match?({:retract, _}, &1))

      assert length(retract_notifications) >= 1,
        "Observer should receive retraction notification, got: #{inspect(updated_observer.received)}"

      GenServer.stop(actor)
    end
  end
end
