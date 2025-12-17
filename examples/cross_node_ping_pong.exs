# Cross-Node Ping-Pong Example
#
# Demonstrates cross-network communication using the Syndicate relay protocol.
# A server hosts a PingService that responds to Ping assertions with Pong messages.
# A client connects via Unix socket, sends Pings, and receives Pongs back.
#
# Run with: MIX_ENV=test mix run examples/cross_node_ping_pong.exs
# (Protocol consolidation must be disabled for runtime implementations)

# Define a PingService entity that lives on the server.
# It observes Ping assertions and responds with Pong messages to the embedded sender ref.
defmodule CrossNodePingPong.PingService do
  @moduledoc """
  Server-side service that responds to Ping assertions with Pong messages.

  When a client asserts <Ping seq #ref> to the dataspace, the PingService
  observes it and sends <Pong seq> back to the embedded ref.
  """
  defstruct [:name]
end

defimpl Absynthe.Core.Entity, for: CrossNodePingPong.PingService do
  alias Absynthe.Core.{Actor, Ref}
  alias Absynthe.Protocol.Event

  def on_publish(service, assertion, _handle, turn) do
    case assertion do
      # Match <Ping seq #ref> records
      {:record, {{:symbol, "Ping"}, [{:integer, seq}, {:embedded, %Ref{} = reply_ref}]}} ->
        IO.puts("[Server PingService] Received Ping ##{seq}, sending Pong to client")

        # Send Pong message back to the embedded reply ref
        pong = {:record, {{:symbol, "Pong"}, [{:integer, seq}]}}
        action = Event.message(reply_ref, pong)
        turn = Absynthe.Core.Turn.add_action(turn, action)

        {service, turn}

      _ ->
        {service, turn}
    end
  end

  def on_retract(service, _handle, turn), do: {service, turn}
  def on_message(service, _message, turn), do: {service, turn}
  def on_sync(service, peer, turn) do
    send(Ref.actor_id(peer), {:synced, peer})
    {service, turn}
  end
end

# Client-side receiver entity that collects Pong responses
defmodule CrossNodePingPong.PongReceiver do
  @moduledoc """
  Client-side entity that receives Pong messages from the server.
  """
  defstruct pongs: [], notify: nil
end

defimpl Absynthe.Core.Entity, for: CrossNodePingPong.PongReceiver do
  alias Absynthe.Core.Ref

  def on_publish(receiver, _assertion, _handle, turn), do: {receiver, turn}
  def on_retract(receiver, _handle, turn), do: {receiver, turn}

  def on_message(receiver, message, turn) do
    case message do
      {:record, {{:symbol, "Pong"}, [{:integer, seq}]}} ->
        IO.puts("[Client PongReceiver] Received Pong ##{seq}")
        receiver = %{receiver | pongs: [seq | receiver.pongs]}

        # Notify waiting process if configured
        if receiver.notify do
          send(receiver.notify, {:pong_received, seq})
        end

        {receiver, turn}

      _ ->
        {receiver, turn}
    end
  end

  def on_sync(receiver, peer, turn) do
    send(Ref.actor_id(peer), {:synced, peer})
    {receiver, turn}
  end
end

defmodule CrossNodePingPong.Runner do
  @moduledoc """
  Orchestrates the cross-node ping-pong demonstration.
  """

  alias Absynthe.Core.{Actor, Ref}
  alias Absynthe.Relay.{Broker, Client, Framing}
  alias Absynthe.Relay.Packet.{Turn, TurnEvent, Assert, Message, Sync}

  def run do
    IO.puts("=== Cross-Node Ping-Pong Example ===\n")

    socket_path = "/tmp/absynthe_cross_node_ping_pong_#{:erlang.unique_integer([:positive])}.sock"

    # --- SERVER SIDE ---
    IO.puts("[Server] Starting broker on #{socket_path}")

    {:ok, broker} = Broker.start_link(socket_path: socket_path)
    dataspace_ref = Broker.dataspace_ref(broker)
    actor_pid = Broker.actor_pid(broker)

    # Spawn PingService that observes Ping patterns
    {:ok, ping_service_ref} = Actor.spawn_entity(
      actor_pid,
      :root,
      %CrossNodePingPong.PingService{name: "ping-service"}
    )

    # Subscribe PingService to Ping assertions in the dataspace
    # Pattern: <Ping $ $> (capture seq and reply_ref)
    observe_pattern = Absynthe.observe(
      Absynthe.record(:Ping, [Absynthe.capture(), Absynthe.capture()]),
      ping_service_ref
    )
    {:ok, _observe_handle} = Actor.assert(actor_pid, dataspace_ref, observe_pattern)

    IO.puts("[Server] PingService ready, waiting for clients\n")
    Process.sleep(100)

    # --- CLIENT SIDE ---
    IO.puts("[Client] Connecting to server...")

    # Connect to the broker
    {:ok, client} = Client.connect_unix(socket_path)
    IO.puts("[Client] Connected!\n")

    # Create a local actor for the client side to receive Pong responses
    {:ok, client_actor} = Actor.start_link(id: :client_actor)

    # Spawn a PongReceiver entity to collect responses
    {:ok, pong_receiver_ref} = Actor.spawn_entity(
      client_actor,
      :root,
      %CrossNodePingPong.PongReceiver{notify: self()}
    )

    IO.puts("[Client] Starting ping-pong exchange...\n")

    # Send multiple pings
    for seq <- 1..3 do
      IO.puts("[Client] Sending Ping ##{seq}")

      # Create the Ping assertion with embedded reply ref
      # The relay will translate the embedded ref to a wire ref
      ping_assertion = {:record,
        {{:symbol, "Ping"},
         [{:integer, seq}, {:embedded, pong_receiver_ref}]}}

      # Create wire packet to OID 0 (the dataspace)
      turn = %Turn{
        events: [
          %TurnEvent{
            oid: 0,
            event: %Assert{
              assertion: translate_ref_to_wire(ping_assertion),
              handle: seq
            }
          }
        ]
      }

      {:ok, data} = Framing.encode(turn)
      {:ok, client} = Client.send_data(client, data)

      Process.sleep(200)
    end

    IO.puts("\n[Client] Waiting for Pongs...")

    # Collect responses with timeout
    collect_pongs(3, 1000)

    IO.puts("\n=== Done! ===\n")

    # Cleanup
    Client.close(client)
    GenServer.stop(client_actor)
    Broker.stop(broker)
    File.rm(socket_path)
  end

  # Helper to translate Elixir Ref to wire format for sending
  # The relay will properly import this as a "yours" reference
  # Wire ref format: [0, oid] for "mine", [1, oid, ...attenuation] for "yours"
  defp translate_ref_to_wire({:record, {label, fields}}) do
    {:record, {translate_ref_to_wire(label), Enum.map(fields, &translate_ref_to_wire/1)}}
  end

  defp translate_ref_to_wire({:embedded, %Ref{} = ref}) do
    # For the client's own ref, we export it as "mine" with OID based on entity_id
    # The server's relay will import this and create a proxy
    oid = ref.entity_id
    wire_ref = {:sequence, [{:integer, 0}, {:integer, oid}]}  # [0, oid] = mine
    {:embedded, wire_ref}
  end

  defp translate_ref_to_wire(other), do: other

  defp collect_pongs(count, timeout) do
    collect_pongs(count, timeout, 0)
  end

  defp collect_pongs(0, _timeout, received) do
    IO.puts("[Client] Received all #{received} Pongs!")
  end

  defp collect_pongs(remaining, timeout, received) do
    receive do
      {:pong_received, seq} ->
        IO.puts("[Client] Pong ##{seq} collected")
        collect_pongs(remaining - 1, timeout, received + 1)
    after
      timeout ->
        IO.puts("[Client] Timeout waiting for Pongs (received #{received}/#{received + remaining})")
    end
  end
end

CrossNodePingPong.Runner.run()
