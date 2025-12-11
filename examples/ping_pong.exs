# Ping-Pong Example
#
# Demonstrates basic message passing between entities in the same actor.
# Run with: mix run examples/ping_pong.exs

defmodule PingPong.Pinger do
  @moduledoc "Entity that sends pings and waits for pongs"
  defstruct [:ponger_ref, count: 0, max: 5]
end

defmodule PingPong.Ponger do
  @moduledoc "Entity that responds to pings with pongs"
  defstruct []
end

defimpl Absynthe.Core.Entity, for: PingPong.Pinger do
  alias Absynthe.Protocol.Event

  def on_message(%{count: count, max: max} = pinger, :start, turn) when count < max do
    IO.puts("[Pinger] Starting ping-pong, sending ping ##{count + 1}")
    action = Event.message(pinger.ponger_ref, {:ping, count + 1, self_ref(turn)})
    turn = Absynthe.Core.Turn.add_action(turn, action)
    {%{pinger | count: count + 1}, turn}
  end

  def on_message(%{count: count, max: max} = pinger, {:pong, n}, turn) when count < max do
    IO.puts("[Pinger] Received pong ##{n}, sending ping ##{count + 1}")
    action = Event.message(pinger.ponger_ref, {:ping, count + 1, self_ref(turn)})
    turn = Absynthe.Core.Turn.add_action(turn, action)
    {%{pinger | count: count + 1}, turn}
  end

  def on_message(%{max: max} = pinger, {:pong, n}, turn) do
    IO.puts("[Pinger] Received final pong ##{n}. Done after #{max} rounds!")
    {pinger, turn}
  end

  def on_publish(entity, _assertion, _handle, turn), do: {entity, turn}
  def on_retract(entity, _handle, turn), do: {entity, turn}
  def on_sync(entity, _peer, turn), do: {entity, turn}

  # Helper to create a self-reference (simplified for example)
  defp self_ref(turn) do
    # In a real implementation, you'd get this from the turn context
    # For this example, we pass it explicitly
    %Absynthe.Core.Ref{actor_id: turn.actor_id, entity_id: 0}
  end
end

defimpl Absynthe.Core.Entity, for: PingPong.Ponger do
  alias Absynthe.Protocol.Event

  def on_message(ponger, {:ping, n, reply_ref}, turn) do
    IO.puts("[Ponger] Received ping ##{n}, sending pong")
    action = Event.message(reply_ref, {:pong, n})
    turn = Absynthe.Core.Turn.add_action(turn, action)
    {ponger, turn}
  end

  def on_publish(entity, _assertion, _handle, turn), do: {entity, turn}
  def on_retract(entity, _handle, turn), do: {entity, turn}
  def on_sync(entity, _peer, turn), do: {entity, turn}
end

# Run the example
IO.puts("=== Ping-Pong Example ===\n")

{:ok, actor} = Absynthe.start_actor(id: :ping_pong)

# Create ponger first
{:ok, ponger_ref} = Absynthe.spawn_entity(actor, :root, %PingPong.Ponger{})

# Create pinger with reference to ponger
{:ok, pinger_ref} = Absynthe.spawn_entity(actor, :root, %PingPong.Pinger{
  ponger_ref: ponger_ref,
  max: 5
})

# Start the ping-pong
Absynthe.send_to(actor, pinger_ref, :start)

# Wait for messages to be processed
Process.sleep(100)

IO.puts("\n=== Done ===")
GenServer.stop(actor)
