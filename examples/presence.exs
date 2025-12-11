# Presence Example
#
# Demonstrates dataspace-based presence tracking using assertions.
# Run with: MIX_ENV=test mix run examples/presence.exs
# (Protocol consolidation must be disabled for runtime implementations)

defmodule Presence.Monitor do
  @moduledoc "Observes Online assertions and tracks who's present"
  defstruct users: MapSet.new()

  defimpl Absynthe.Core.Entity do
    def on_publish(monitor, {:record, {{:symbol, "Online"}, [{:string, username}]}}, handle, turn) do
      IO.puts("[Monitor] User came online: #{username} (handle: #{inspect(handle)})")
      {%{monitor | users: MapSet.put(monitor.users, {username, handle})}, turn}
    end

    def on_publish(monitor, assertion, _handle, turn) do
      IO.puts("[Monitor] Ignoring non-Online assertion: #{inspect(assertion)}")
      {monitor, turn}
    end

    def on_retract(monitor, handle, turn) do
      case Enum.find(monitor.users, fn {_name, h} -> h == handle end) do
        {username, _} ->
          IO.puts("[Monitor] User went offline: #{username}")
          {%{monitor | users: MapSet.delete(monitor.users, {username, handle})}, turn}
        nil ->
          {monitor, turn}
      end
    end

    def on_message(monitor, :status, turn) do
      users = monitor.users |> Enum.map(fn {name, _} -> name end) |> Enum.sort()
      IO.puts("[Monitor] Currently online: #{inspect(users)}")
      {monitor, turn}
    end

    def on_message(entity, _msg, turn), do: {entity, turn}
    def on_sync(entity, _peer, turn), do: {entity, turn}
  end
end

defmodule Presence.Runner do
  def run do
    IO.puts("=== Presence Tracking Example ===\n")

    {:ok, actor} = Absynthe.start_actor(id: :presence_demo)

    # Create a dataspace
    dataspace = Absynthe.new_dataspace()
    {:ok, ds_ref} = Absynthe.spawn_entity(actor, :root, dataspace)

    # Create a presence monitor
    {:ok, monitor_ref} = Absynthe.spawn_entity(actor, :root, %Presence.Monitor{})

    # Subscribe monitor to Online records in the dataspace
    # The observe assertion tells the dataspace to forward matching assertions
    observe_pattern = Absynthe.observe(
      Absynthe.record(:Online, [Absynthe.wildcard()]),
      monitor_ref
    )
    {:ok, _} = Absynthe.assert_to(actor, ds_ref, observe_pattern)

    Process.sleep(20)
    IO.puts("\n--- Users joining ---")

    # Simulate users coming online
    {:ok, alice_handle} = Absynthe.assert_to(actor, ds_ref, Absynthe.record(:Online, ["alice"]))
    Process.sleep(20)

    {:ok, bob_handle} = Absynthe.assert_to(actor, ds_ref, Absynthe.record(:Online, ["bob"]))
    Process.sleep(20)

    {:ok, charlie_handle} = Absynthe.assert_to(actor, ds_ref, Absynthe.record(:Online, ["charlie"]))
    Process.sleep(20)

    # Check status
    Absynthe.send_to(actor, monitor_ref, :status)
    Process.sleep(20)

    IO.puts("\n--- Users leaving ---")

    # Simulate users going offline
    :ok = Absynthe.retract_from(actor, bob_handle)
    Process.sleep(20)

    # Check status again
    Absynthe.send_to(actor, monitor_ref, :status)
    Process.sleep(20)

    # Alice and Charlie leave
    :ok = Absynthe.retract_from(actor, alice_handle)
    :ok = Absynthe.retract_from(actor, charlie_handle)
    Process.sleep(20)

    # Final status
    Absynthe.send_to(actor, monitor_ref, :status)
    Process.sleep(20)

    IO.puts("\n=== Done ===")
    GenServer.stop(actor)
  end
end

Presence.Runner.run()
