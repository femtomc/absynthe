# Box and Client Example
#
# Port of syndicate-rs box-and-client example demonstrating:
# - Dataspace assertions (box-state value)
# - Pattern observation for messages (set-box v) and assertions (box-state v)
# - Two cooperating actors sharing one dataspace
# - Stop conditions on value threshold or retraction
#
# Run with: MIX_ENV=test mix run examples/box_and_client.exs
# (Protocol consolidation must be disabled for runtime implementations)

defmodule BoxAndClient.BoxHandler do
  @moduledoc """
  Entity that observes <set-box v> messages and forwards them to the Box.
  When a message to the dataspace matches the observer's pattern,
  the dataspace forwards the message via on_message (not on_publish).
  """
  defstruct [:box_ref]

  defimpl Absynthe.Core.Entity do
    alias Absynthe.Protocol.Event

    # Messages matching <set-box $> pattern are forwarded here
    def on_message(handler, {:record, {{:symbol, "set-box"}, [{:integer, value}]}}, turn) do
      action = Event.message(handler.box_ref, {:set_box, value})
      turn = Absynthe.Core.Turn.add_action(turn, action)
      {handler, turn}
    end

    def on_message(handler, _msg, turn), do: {handler, turn}
    def on_publish(entity, _assertion, _handle, turn), do: {entity, turn}
    def on_retract(entity, _handle, turn), do: {entity, turn}
    def on_sync(entity, _peer, turn), do: {entity, turn}
  end
end

defmodule BoxAndClient.Box do
  @moduledoc """
  Box actor maintains a value, asserting <box-state value> to the dataspace.
  Receives {:set_box, v} messages from BoxHandler to update its state.
  Stops when value reaches max_value.
  """
  defstruct [:ds_ref, :state_handle, current_value: 0, max_value: 100]

  defimpl Absynthe.Core.Entity do
    alias Absynthe.Protocol.Event

    # Handle set_box messages forwarded from BoxHandler
    # Only accept updates if we haven't reached max and new value is higher
    def on_message(box, {:set_box, new_value}, turn) when is_integer(new_value) do
      if new_value > box.current_value and box.current_value < box.max_value do
        # Clamp to max_value if overshooting
        clamped_value = min(new_value, box.max_value)
        IO.puts("[Box] Received set-box #{new_value}" <> if(clamped_value != new_value, do: " (clamped to #{clamped_value})", else: ""))

        # Update state: retract old assertion, assert new one
        turn =
          case box.state_handle do
            nil -> turn
            handle -> Absynthe.retract_value(turn, box.ds_ref, handle)
          end

        # Create new assertion with clamped value
        new_handle =
          Absynthe.Assertions.Handle.new(
            Absynthe.Core.Turn.actor_id(turn),
            :erlang.unique_integer([:positive])
          )

        assertion = Absynthe.record(:"box-state", [clamped_value])
        action = Event.assert(box.ds_ref, assertion, new_handle)
        turn = Absynthe.Core.Turn.add_action(turn, action)

        IO.puts("[Box] Asserting box-state #{clamped_value}")

        if clamped_value >= box.max_value do
          IO.puts("[Box] Reached max value #{box.max_value}, stopping")
        end

        {%{box | current_value: clamped_value, state_handle: new_handle}, turn}
      else
        {box, turn}
      end
    end

    # Handle initialization message to set up the dataspace ref and initial assertion
    def on_message(box, {:init, ds_ref}, turn) do
      IO.puts("[Box] Initializing with value #{box.current_value}")

      # Assert initial state
      handle =
        Absynthe.Assertions.Handle.new(
          Absynthe.Core.Turn.actor_id(turn),
          :erlang.unique_integer([:positive])
        )

      assertion = Absynthe.record(:"box-state", [box.current_value])
      action = Event.assert(ds_ref, assertion, handle)
      turn = Absynthe.Core.Turn.add_action(turn, action)

      {%{box | ds_ref: ds_ref, state_handle: handle}, turn}
    end

    def on_message(box, _msg, turn), do: {box, turn}

    def on_publish(entity, _assertion, _handle, turn), do: {entity, turn}
    def on_retract(entity, _handle, turn), do: {entity, turn}
    def on_sync(entity, _peer, turn), do: {entity, turn}
  end
end

defmodule BoxAndClient.Client do
  @moduledoc """
  Client observes <box-state value> assertions and sends <set-box value+1> messages.
  Tracks assertion count and stops when all box-state assertions are retracted.
  """
  defstruct [:ds_ref, assertion_count: 0, values_seen: []]

  defimpl Absynthe.Core.Entity do
    alias Absynthe.Protocol.Event

    # Handle initialization
    def on_message(client, {:init, ds_ref}, turn) do
      {%{client | ds_ref: ds_ref}, turn}
    end

    def on_message(client, _msg, turn), do: {client, turn}

    # When box-state assertion appears, increment and send set-box message
    def on_publish(
          client,
          {:record, {{:symbol, "box-state"}, [{:integer, value}]}},
          handle,
          turn
        ) do
      IO.puts("[Client] Observed box-state #{value} (handle: #{inspect(handle)})")
      new_count = client.assertion_count + 1
      next_value = value + 1

      # Send set-box message to dataspace (will be routed to BoxHandler)
      set_box_msg = Absynthe.record(:"set-box", [next_value])
      action = Event.message(client.ds_ref, set_box_msg)
      turn = Absynthe.Core.Turn.add_action(turn, action)

      IO.puts("[Client] Sending set-box #{next_value}")

      {%{client | assertion_count: new_count, values_seen: [value | client.values_seen]}, turn}
    end

    def on_publish(client, _assertion, _handle, turn), do: {client, turn}

    # When box-state retracted, decrement count
    def on_retract(client, _handle, turn) do
      new_count = max(0, client.assertion_count - 1)

      if new_count == 0 and client.assertion_count > 0 do
        IO.puts("[Client] All box-state assertions retracted")
      end

      {%{client | assertion_count: new_count}, turn}
    end

    def on_sync(entity, _peer, turn), do: {entity, turn}
  end
end

defmodule BoxAndClient.Runner do
  def run(max_value \\ 10) do
    IO.puts("=== Box and Client Example ===")
    IO.puts("Box will count from 0 to #{max_value}\n")

    # Start the actor
    {:ok, actor} = Absynthe.start_actor(id: :box_and_client)

    # Create a dataspace
    dataspace = Absynthe.new_dataspace()
    {:ok, ds_ref} = Absynthe.spawn_entity(actor, :root, dataspace)

    # Create Box entity
    {:ok, box_ref} = Absynthe.spawn_entity(actor, :root, %BoxAndClient.Box{max_value: max_value})

    # Create BoxHandler to receive set-box messages and forward to Box
    {:ok, box_handler_ref} =
      Absynthe.spawn_entity(actor, :root, %BoxAndClient.BoxHandler{box_ref: box_ref})

    # Subscribe BoxHandler to <set-box $> pattern (for messages)
    set_box_pattern =
      Absynthe.observe(
        Absynthe.record(:"set-box", [Absynthe.capture()]),
        box_handler_ref
      )

    {:ok, _} = Absynthe.assert_to(actor, ds_ref, set_box_pattern)
    Process.sleep(10)

    # Create Client entity
    {:ok, client_ref} = Absynthe.spawn_entity(actor, :root, %BoxAndClient.Client{})

    # Subscribe Client to <box-state $> pattern (for assertions)
    box_state_pattern =
      Absynthe.observe(
        Absynthe.record(:"box-state", [Absynthe.capture()]),
        client_ref
      )

    {:ok, _} = Absynthe.assert_to(actor, ds_ref, box_state_pattern)
    Process.sleep(10)

    # Initialize Client with dataspace ref
    Absynthe.send_to(actor, client_ref, {:init, ds_ref})
    Process.sleep(10)

    IO.puts("\n--- Starting Box-Client interaction ---\n")

    # Initialize Box - this starts the cascade:
    # 1. Box asserts <box-state 0>
    # 2. Client observes assertion, sends <set-box 1> message
    # 3. BoxHandler receives message, forwards to Box
    # 4. Box updates state to 1, asserts <box-state 1>
    # 5. Client observes new assertion, sends <set-box 2> message
    # ... repeats until max_value
    Absynthe.send_to(actor, box_ref, {:init, ds_ref})

    # Wait for the interaction to complete
    # Each round involves multiple async message passes
    wait_time = max_value * 50 + 200
    Process.sleep(wait_time)

    IO.puts("\n=== Done ===")
    GenServer.stop(actor)
  end
end

# Parse command line argument for max_value
max_value =
  case System.argv() do
    [n] -> String.to_integer(n)
    _ -> 10
  end

BoxAndClient.Runner.run(max_value)
