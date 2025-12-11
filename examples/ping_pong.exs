# Ping-Pong Example
#
# Demonstrates basic message passing between entities in the same actor.
# Run with: MIX_ENV=test mix run examples/ping_pong.exs
# (Protocol consolidation must be disabled for runtime implementations)

defmodule PingPong.Pinger do
  @moduledoc "Entity that sends pings and waits for pongs"
  # Note: self_ref is set after spawning via :init message
  defstruct [:ponger_ref, :self_ref, count: 0, max: 5]

  defimpl Absynthe.Core.Entity do
    alias Absynthe.Protocol.Event

    # Initialize with self reference
    def on_message(pinger, {:init, self_ref}, turn) do
      {%{pinger | self_ref: self_ref}, turn}
    end

    def on_message(%{count: count, max: max, self_ref: self_ref} = pinger, :start, turn)
        when count < max and self_ref != nil do
      IO.puts("[Pinger] Starting ping-pong, sending ping ##{count + 1}")
      action = Event.message(pinger.ponger_ref, {:ping, count + 1, self_ref})
      turn = Absynthe.Core.Turn.add_action(turn, action)
      {%{pinger | count: count + 1}, turn}
    end

    def on_message(%{count: count, max: max, self_ref: self_ref} = pinger, {:pong, n}, turn)
        when count < max do
      IO.puts("[Pinger] Received pong ##{n}, sending ping ##{count + 1}")
      action = Event.message(pinger.ponger_ref, {:ping, count + 1, self_ref})
      turn = Absynthe.Core.Turn.add_action(turn, action)
      {%{pinger | count: count + 1}, turn}
    end

    def on_message(%{max: max} = pinger, {:pong, n}, turn) do
      IO.puts("[Pinger] Received final pong ##{n}. Done after #{max} rounds!")
      {pinger, turn}
    end

    def on_message(pinger, _msg, turn), do: {pinger, turn}

    def on_publish(entity, _assertion, _handle, turn), do: {entity, turn}
    def on_retract(entity, _handle, turn), do: {entity, turn}
    def on_sync(entity, _peer, turn), do: {entity, turn}
  end
end

defmodule PingPong.Ponger do
  @moduledoc "Entity that responds to pings with pongs"
  defstruct []

  defimpl Absynthe.Core.Entity do
    alias Absynthe.Protocol.Event

    def on_message(ponger, {:ping, n, reply_ref}, turn) do
      IO.puts("[Ponger] Received ping ##{n}, sending pong")
      action = Event.message(reply_ref, {:pong, n})
      turn = Absynthe.Core.Turn.add_action(turn, action)
      {ponger, turn}
    end

    def on_message(ponger, _msg, turn), do: {ponger, turn}

    def on_publish(entity, _assertion, _handle, turn), do: {entity, turn}
    def on_retract(entity, _handle, turn), do: {entity, turn}
    def on_sync(entity, _peer, turn), do: {entity, turn}
  end
end

defmodule PingPong.Runner do
  def run do
    IO.puts("=== Ping-Pong Example ===\n")

    {:ok, actor} = Absynthe.start_actor(id: :ping_pong)

    # Create ponger first
    {:ok, ponger_ref} = Absynthe.spawn_entity(actor, :root, %PingPong.Ponger{})

    # Create pinger with reference to ponger
    {:ok, pinger_ref} = Absynthe.spawn_entity(actor, :root, %PingPong.Pinger{
      ponger_ref: ponger_ref,
      max: 5
    })

    # Initialize pinger with its own reference (workaround for missing self-ref API)
    Absynthe.send_to(actor, pinger_ref, {:init, pinger_ref})
    Process.sleep(10)

    # Start the ping-pong
    Absynthe.send_to(actor, pinger_ref, :start)

    # Wait for messages to be processed
    Process.sleep(100)

    IO.puts("\n=== Done ===")
    GenServer.stop(actor)
  end
end

PingPong.Runner.run()
