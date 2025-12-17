defmodule Absynthe.Integration.BoxAndClientTest do
  @moduledoc """
  Integration test for the Box and Client example.

  Tests the classic syndicate pattern where:
  - Box maintains a value, asserting <box-state value> to a dataspace
  - Client observes <box-state v> and sends <set-box v+1> messages
  - BoxHandler receives <set-box v> messages and forwards to Box
  """
  use ExUnit.Case

  alias Absynthe.Core.{Actor, Turn, Entity, Ref}
  alias Absynthe.Protocol.Event
  alias Absynthe.Assertions.Handle

  # BoxHandler: receives set-box messages from dataspace and forwards to Box
  defmodule BoxHandler do
    defstruct [:box_ref]

    defimpl Entity do
      def on_message(handler, {:record, {{:symbol, "set-box"}, [{:integer, value}]}}, turn) do
        action = Event.message(handler.box_ref, {:set_box, value})
        turn = Turn.add_action(turn, action)
        {handler, turn}
      end

      def on_message(handler, _msg, turn), do: {handler, turn}
      def on_publish(entity, _assertion, _handle, turn), do: {entity, turn}
      def on_retract(entity, _handle, turn), do: {entity, turn}
      def on_sync(entity, _peer, turn), do: {entity, turn}
    end
  end

  # Box: maintains value, asserts <box-state value>
  defmodule Box do
    defstruct [:ds_ref, :state_handle, current_value: 0, max_value: 100, updates: []]

    defimpl Entity do
      def on_message(box, {:set_box, new_value}, turn) when is_integer(new_value) do
        # Only accept updates if we haven't reached max and new value is higher
        if new_value > box.current_value and box.current_value < box.max_value do
          # Clamp to max_value if overshooting
          clamped_value = min(new_value, box.max_value)

          # Retract old assertion
          turn =
            case box.state_handle do
              nil -> turn
              handle -> Absynthe.retract_value(turn, box.ds_ref, handle)
            end

          # Create new assertion with clamped value
          new_handle =
            Handle.new(Turn.actor_id(turn), :erlang.unique_integer([:positive]))

          assertion = Absynthe.record(:"box-state", [clamped_value])
          action = Event.assert(box.ds_ref, assertion, new_handle)
          turn = Turn.add_action(turn, action)

          {%{
             box
             | current_value: clamped_value,
               state_handle: new_handle,
               updates: [clamped_value | box.updates]
           }, turn}
        else
          {box, turn}
        end
      end

      def on_message(box, {:init, ds_ref}, turn) do
        handle = Handle.new(Turn.actor_id(turn), :erlang.unique_integer([:positive]))
        assertion = Absynthe.record(:"box-state", [box.current_value])
        action = Event.assert(ds_ref, assertion, handle)
        turn = Turn.add_action(turn, action)
        {%{box | ds_ref: ds_ref, state_handle: handle}, turn}
      end

      def on_message(box, _msg, turn), do: {box, turn}
      def on_publish(entity, _assertion, _handle, turn), do: {entity, turn}
      def on_retract(entity, _handle, turn), do: {entity, turn}
      def on_sync(entity, _peer, turn), do: {entity, turn}
    end
  end

  # Client: observes <box-state v>, sends <set-box v+1>
  defmodule Client do
    defstruct [:ds_ref, assertion_count: 0, values_seen: []]

    defimpl Entity do
      def on_message(client, {:init, ds_ref}, turn) do
        {%{client | ds_ref: ds_ref}, turn}
      end

      def on_message(client, _msg, turn), do: {client, turn}

      def on_publish(
            client,
            {:record, {{:symbol, "box-state"}, [{:integer, value}]}},
            _handle,
            turn
          ) do
        new_count = client.assertion_count + 1
        next_value = value + 1

        set_box_msg = Absynthe.record(:"set-box", [next_value])
        action = Event.message(client.ds_ref, set_box_msg)
        turn = Turn.add_action(turn, action)

        {%{client | assertion_count: new_count, values_seen: [value | client.values_seen]}, turn}
      end

      def on_publish(client, _assertion, _handle, turn), do: {client, turn}

      def on_retract(client, _handle, turn) do
        new_count = max(0, client.assertion_count - 1)
        {%{client | assertion_count: new_count}, turn}
      end

      def on_sync(entity, _peer, turn), do: {entity, turn}
    end
  end

  describe "box and client interaction" do
    test "box and client count up to max_value" do
      max_value = 5

      {:ok, actor} = Actor.start_link(id: :box_client_test)

      # Create dataspace
      dataspace = Absynthe.new_dataspace()
      {:ok, ds_ref} = Actor.spawn_entity(actor, :root, dataspace)

      # Create Box
      {:ok, box_ref} = Actor.spawn_entity(actor, :root, %Box{max_value: max_value})

      # Create BoxHandler
      {:ok, box_handler_ref} = Actor.spawn_entity(actor, :root, %BoxHandler{box_ref: box_ref})

      # Subscribe BoxHandler to <set-box $> pattern
      set_box_pattern =
        Absynthe.observe(
          Absynthe.record(:"set-box", [Absynthe.capture()]),
          box_handler_ref
        )

      {:ok, _} = Actor.assert(actor, ds_ref, set_box_pattern)
      Process.sleep(10)

      # Create Client
      {:ok, client_ref} = Actor.spawn_entity(actor, :root, %Client{})

      # Subscribe Client to <box-state $> pattern
      box_state_pattern =
        Absynthe.observe(
          Absynthe.record(:"box-state", [Absynthe.capture()]),
          client_ref
        )

      {:ok, _} = Actor.assert(actor, ds_ref, box_state_pattern)
      Process.sleep(10)

      # Initialize Client with dataspace ref
      Actor.send_message(actor, client_ref, {:init, ds_ref})
      Process.sleep(10)

      # Initialize Box - starts the cascade
      Actor.send_message(actor, box_ref, {:init, ds_ref})

      # Wait for interaction to complete
      Process.sleep(max_value * 50 + 100)

      # Verify Box state
      state = :sys.get_state(actor)
      {_facet_id, box_state} = Map.get(state.entities, Ref.entity_id(box_ref))
      assert box_state.current_value == max_value
      assert length(box_state.updates) == max_value

      # Verify Client saw all values (including final max_value)
      {_facet_id, client_state} = Map.get(state.entities, Ref.entity_id(client_ref))
      assert Enum.sort(client_state.values_seen) == Enum.to_list(0..max_value)

      GenServer.stop(actor)
    end

    test "box stops at max_value without updating further" do
      max_value = 3

      {:ok, actor} = Actor.start_link(id: :box_stop_test)

      dataspace = Absynthe.new_dataspace()
      {:ok, ds_ref} = Actor.spawn_entity(actor, :root, dataspace)

      {:ok, box_ref} = Actor.spawn_entity(actor, :root, %Box{max_value: max_value})
      {:ok, box_handler_ref} = Actor.spawn_entity(actor, :root, %BoxHandler{box_ref: box_ref})

      set_box_pattern =
        Absynthe.observe(
          Absynthe.record(:"set-box", [Absynthe.capture()]),
          box_handler_ref
        )

      {:ok, _} = Actor.assert(actor, ds_ref, set_box_pattern)
      Process.sleep(10)

      {:ok, client_ref} = Actor.spawn_entity(actor, :root, %Client{})

      box_state_pattern =
        Absynthe.observe(
          Absynthe.record(:"box-state", [Absynthe.capture()]),
          client_ref
        )

      {:ok, _} = Actor.assert(actor, ds_ref, box_state_pattern)
      Process.sleep(10)

      Actor.send_message(actor, client_ref, {:init, ds_ref})
      Process.sleep(10)

      Actor.send_message(actor, box_ref, {:init, ds_ref})
      Process.sleep(max_value * 50 + 100)

      # Manually try to set beyond max
      Actor.send_message(actor, box_ref, {:set_box, max_value + 10})
      Process.sleep(50)

      state = :sys.get_state(actor)
      {_facet_id, box_state} = Map.get(state.entities, Ref.entity_id(box_ref))

      # Should still be at max_value, not beyond
      assert box_state.current_value == max_value

      GenServer.stop(actor)
    end

    test "multiple iterations with higher max_value" do
      max_value = 20

      {:ok, actor} = Actor.start_link(id: :box_high_max_test)

      dataspace = Absynthe.new_dataspace()
      {:ok, ds_ref} = Actor.spawn_entity(actor, :root, dataspace)

      {:ok, box_ref} = Actor.spawn_entity(actor, :root, %Box{max_value: max_value})
      {:ok, box_handler_ref} = Actor.spawn_entity(actor, :root, %BoxHandler{box_ref: box_ref})

      set_box_pattern =
        Absynthe.observe(
          Absynthe.record(:"set-box", [Absynthe.capture()]),
          box_handler_ref
        )

      {:ok, _} = Actor.assert(actor, ds_ref, set_box_pattern)
      Process.sleep(10)

      {:ok, client_ref} = Actor.spawn_entity(actor, :root, %Client{})

      box_state_pattern =
        Absynthe.observe(
          Absynthe.record(:"box-state", [Absynthe.capture()]),
          client_ref
        )

      {:ok, _} = Actor.assert(actor, ds_ref, box_state_pattern)
      Process.sleep(10)

      Actor.send_message(actor, client_ref, {:init, ds_ref})
      Process.sleep(10)

      Actor.send_message(actor, box_ref, {:init, ds_ref})
      Process.sleep(max_value * 50 + 200)

      state = :sys.get_state(actor)
      {_facet_id, box_state} = Map.get(state.entities, Ref.entity_id(box_ref))

      assert box_state.current_value == max_value
      assert length(box_state.updates) == max_value

      GenServer.stop(actor)
    end
  end
end
