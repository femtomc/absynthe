defmodule Absynthe.Relay.Relay do
  @moduledoc """
  Per-connection relay worker for the Syndicate protocol.

  A relay is a GenServer that manages a single connection to a remote peer,
  translating between wire protocol packets and local actor events. Each
  relay maintains:

  - **Export membrane**: Maps local Refs to OIDs exposed to the peer
  - **Import membrane**: Maps peer OIDs to local proxy entities
  - **Handle mapping**: Tracks assertion handles for retraction
  - **Socket state**: Buffer management and connection status

  ## Protocol Flow

  ### Inbound (peer -> local)

  1. Receive binary data from socket
  2. Decode Preserves packets using framing
  3. For each TurnEvent in a Turn packet:
     - Translate wire references (WireRef) to local Refs via membranes
     - Translate wire handles to local handles
     - Forward events to the target entity (usually a dataspace)

  ### Outbound (local -> peer)

  1. Receive events from local actors (via entity callbacks)
  2. Translate local Refs to WireRefs via membranes
  3. Translate local handles to wire handles
  4. Encode and send Turn packets over the socket

  ## Entity Behavior

  The relay spawns a proxy entity for each imported OID. These proxies
  forward events back through the relay to the remote peer. When
  assertions are made to proxy entities, they become outbound assertions
  over the wire.

  ## Lifecycle

  - On connect: Initialize membranes, set up OID 0 as the root target
  - On disconnect: Retract all assertions from this relay
  - On error: Send error packet, close connection, retract assertions

  ## Example

      # Start a relay for an accepted socket
      {:ok, relay} = Relay.start_link(
        socket: socket,
        transport: :gen_tcp,
        dataspace_ref: dataspace_ref
      )
  """

  use GenServer
  require Logger

  alias Absynthe.Core.{Actor, Ref, Entity}
  alias Absynthe.Assertions.Handle
  alias Absynthe.Protocol.Event
  alias Absynthe.Relay.{Membrane, Packet, Framing}

  # Proxy entity for imported remote refs
  defmodule Proxy do
    @moduledoc false
    defstruct [:relay_pid, :oid]
  end

  defimpl Entity, for: Proxy do
    def on_publish(%Proxy{relay_pid: relay_pid, oid: oid} = entity, assertion, handle, turn) do
      # Forward assertion to relay for outbound transmission
      send(relay_pid, {:outbound_assert, oid, assertion, handle})
      {entity, turn}
    end

    def on_retract(%Proxy{relay_pid: relay_pid, oid: _oid} = entity, handle, turn) do
      # Forward retraction to relay
      send(relay_pid, {:outbound_retract, handle})
      {entity, turn}
    end

    def on_message(%Proxy{relay_pid: relay_pid, oid: oid} = entity, message, turn) do
      # Forward message to relay
      send(relay_pid, {:outbound_message, oid, message})
      {entity, turn}
    end

    def on_sync(%Proxy{relay_pid: relay_pid, oid: oid} = entity, peer, turn) do
      # Forward sync to relay
      send(relay_pid, {:outbound_sync, oid, peer})
      {entity, turn}
    end
  end

  # State structure

  @typedoc """
  Relay state.
  """
  @type state :: %{
          socket: port() | :ssl.sslsocket(),
          transport: module(),
          actor_pid: pid(),
          dataspace_ref: Ref.t(),
          export_membrane: Membrane.t(),
          import_membrane: Membrane.t(),
          recv_buffer: binary(),
          local_handles: %{Handle.t() => non_neg_integer()},
          wire_handles: %{non_neg_integer() => Handle.t()},
          handle_to_oid: %{Handle.t() => non_neg_integer()},
          next_wire_handle: non_neg_integer(),
          pending_syncs: %{non_neg_integer() => Ref.t()},
          next_sync_id: non_neg_integer(),
          closed: boolean()
        }

  # Client API

  @doc """
  Starts a relay for the given socket.

  ## Options

  - `:socket` - The connected socket (required)
  - `:transport` - Transport module, `:gen_tcp` or `:ssl` (default: `:gen_tcp`)
  - `:dataspace_ref` - Ref to the target dataspace entity (required)
  - `:actor_pid` - PID of the hosting actor (default: start a new actor)

  ## Returns

  `{:ok, pid}` on success.
  """
  @spec start_link(Keyword.t()) :: GenServer.on_start()
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  @doc """
  Sends a packet to the remote peer.
  """
  @spec send_packet(GenServer.server(), Packet.packet()) :: :ok | {:error, term()}
  def send_packet(relay, packet) do
    GenServer.call(relay, {:send_packet, packet})
  end

  @doc """
  Closes the relay connection gracefully.
  """
  @spec close(GenServer.server()) :: :ok
  def close(relay) do
    GenServer.cast(relay, :close)
  end

  @doc """
  Activates the relay socket for receiving data.

  Call this after transferring socket ownership via controlling_process.
  """
  @spec activate(GenServer.server()) :: :ok
  def activate(relay) do
    GenServer.cast(relay, :activate_socket)
  end

  # GenServer callbacks

  @impl GenServer
  def init(opts) do
    socket = Keyword.fetch!(opts, :socket)
    transport = Keyword.get(opts, :transport, :gen_tcp)
    dataspace_ref = Keyword.fetch!(opts, :dataspace_ref)

    # Start or use provided actor for spawning proxy entities
    actor_pid =
      case Keyword.get(opts, :actor_pid) do
        nil ->
          {:ok, pid} = Actor.start_link(id: {:relay, self()})
          pid

        pid ->
          pid
      end

    # Initialize membranes
    # Export membrane: local refs -> OIDs we expose
    # Import membrane: peer OIDs -> local proxies
    export_membrane = Membrane.new()
    import_membrane = Membrane.new()

    # Export the dataspace ref as OID 0 (well-known root)
    {_oid, export_membrane} = Membrane.export(export_membrane, dataspace_ref)

    state = %{
      socket: socket,
      transport: transport,
      actor_pid: actor_pid,
      dataspace_ref: dataspace_ref,
      export_membrane: export_membrane,
      import_membrane: import_membrane,
      recv_buffer: Framing.new_state(),
      local_handles: %{},
      wire_handles: %{},
      handle_to_oid: %{},
      next_wire_handle: 0,
      pending_syncs: %{},
      next_sync_id: 0,
      closed: false
    }

    Logger.debug("Relay started for socket #{inspect(socket)}")

    # Don't activate socket yet - wait for activate/1 to be called
    # after controlling_process has transferred ownership

    {:ok, state}
  end

  @impl GenServer
  def handle_call({:send_packet, packet}, _from, state) do
    case do_send_packet(state, packet) do
      {:ok, state} -> {:reply, :ok, state}
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  @impl GenServer
  def handle_cast(:close, state) do
    {:stop, :normal, state}
  end

  def handle_cast(:activate_socket, state) do
    Logger.debug("Relay: activating socket")
    set_socket_active(state)
    {:noreply, state}
  end

  # Handle incoming TCP data
  @impl GenServer
  def handle_info({:tcp, socket, data}, %{socket: socket} = state) do
    handle_incoming_data(state, data)
  end

  def handle_info({:ssl, socket, data}, %{socket: socket} = state) do
    handle_incoming_data(state, data)
  end

  # Handle TCP close
  def handle_info({:tcp_closed, socket}, %{socket: socket} = state) do
    Logger.info("Relay: connection closed by peer")
    {:stop, :normal, %{state | closed: true}}
  end

  def handle_info({:ssl_closed, socket}, %{socket: socket} = state) do
    Logger.info("Relay: SSL connection closed by peer")
    {:stop, :normal, %{state | closed: true}}
  end

  # Handle TCP error
  def handle_info({:tcp_error, socket, reason}, %{socket: socket} = state) do
    Logger.error("Relay: TCP error: #{inspect(reason)}")
    {:stop, {:error, reason}, %{state | closed: true}}
  end

  def handle_info({:ssl_error, socket, reason}, %{socket: socket} = state) do
    Logger.error("Relay: SSL error: #{inspect(reason)}")
    {:stop, {:error, reason}, %{state | closed: true}}
  end

  # Handle outbound events from proxy entities
  def handle_info({:outbound_assert, oid, assertion, handle}, state) do
    state = handle_outbound_assert(state, oid, assertion, handle)
    {:noreply, state}
  end

  def handle_info({:outbound_retract, handle}, state) do
    state = handle_outbound_retract(state, handle)
    {:noreply, state}
  end

  def handle_info({:outbound_message, oid, message}, state) do
    state = handle_outbound_message(state, oid, message)
    {:noreply, state}
  end

  def handle_info({:outbound_sync, oid, peer}, state) do
    state = handle_outbound_sync(state, oid, peer)
    {:noreply, state}
  end

  # Handle sync responses from local actors
  def handle_info({:synced, sync_id}, state) do
    state = handle_sync_response(state, sync_id)
    {:noreply, state}
  end

  @impl GenServer
  def terminate(reason, state) do
    Logger.debug("Relay terminating: #{inspect(reason)}")

    # Retract all assertions we made to the local dataspace
    retract_all_inbound_assertions(state)

    # Close socket if still open
    unless state.closed do
      close_socket(state)
    end

    :ok
  end

  # Implementation

  defp handle_incoming_data(state, data) do
    {packets, buffer} = Framing.append_and_decode(state.recv_buffer, data)
    state = %{state | recv_buffer: buffer}

    # Process each packet with error handling
    state =
      Enum.reduce(packets, state, fn packet, acc_state ->
        try do
          process_inbound_packet(acc_state, packet)
        rescue
          error ->
            Logger.error("Relay: error processing packet: #{inspect(error)}")
            Logger.error(Exception.format_stacktrace(__STACKTRACE__))
            acc_state
        end
      end)

    # Re-enable socket for more data
    set_socket_active(state)

    {:noreply, state}
  end

  defp process_inbound_packet(state, :nop) do
    # Keepalive - no action needed
    state
  end

  defp process_inbound_packet(state, %Packet.Turn{events: events}) do
    Enum.reduce(events, state, &process_turn_event/2)
  end

  defp process_inbound_packet(state, %Packet.Error{message: msg, detail: detail}) do
    Logger.error("Relay: received error from peer: #{msg} (#{inspect(detail)})")
    # The connection should be closed after an error
    state
  end

  defp process_inbound_packet(state, %Packet.Extension{label: label}) do
    Logger.debug("Relay: ignoring unknown extension: #{inspect(label)}")
    state
  end

  defp process_turn_event(%Packet.TurnEvent{oid: oid, event: event}, state) do
    # Look up the target ref from our export membrane
    case Membrane.lookup_by_oid(state.export_membrane, oid) do
      {:ok, ref} ->
        process_event_for_ref(state, ref, event)

      :error ->
        Logger.warning("Relay: received event for unknown OID #{oid}")
        state
    end
  end

  defp process_event_for_ref(state, ref, %Packet.Assert{assertion: assertion, handle: wire_handle}) do
    # Translate embedded wire refs in the assertion to local refs
    {translated_assertion, state} = translate_inbound_refs(state, assertion)

    # Create a local handle
    local_handle = Handle.new({:relay, self()}, wire_handle)

    # Track handle mapping
    state = %{
      state
      | wire_handles: Map.put(state.wire_handles, wire_handle, local_handle),
        local_handles: Map.put(state.local_handles, local_handle, wire_handle)
    }

    # Deliver assertion to the local entity
    Actor.deliver(
      state.actor_pid,
      ref,
      Event.assert(ref, translated_assertion, local_handle)
    )

    state
  end

  defp process_event_for_ref(state, ref, %Packet.Retract{handle: wire_handle}) do
    case Map.get(state.wire_handles, wire_handle) do
      nil ->
        Logger.warning("Relay: retract for unknown handle #{wire_handle}")
        state

      local_handle ->
        # Clean up handle mapping
        state = %{
          state
          | wire_handles: Map.delete(state.wire_handles, wire_handle),
            local_handles: Map.delete(state.local_handles, local_handle)
        }

        # Deliver retraction
        Actor.deliver(state.actor_pid, ref, Event.retract(ref, local_handle))

        state
    end
  end

  defp process_event_for_ref(state, ref, %Packet.Message{body: body}) do
    # Translate embedded wire refs
    {translated_body, state} = translate_inbound_refs(state, body)

    # Deliver message
    Actor.deliver(state.actor_pid, ref, Event.message(ref, translated_body))

    state
  end

  defp process_event_for_ref(state, ref, %Packet.Sync{}) do
    # Generate a sync ID to track the response
    sync_id = state.next_sync_id

    # Track the pending sync and respond when the local actor completes it
    state = %{
      state
      | pending_syncs: Map.put(state.pending_syncs, sync_id, ref),
        next_sync_id: sync_id + 1
    }

    # Create a peer ref that will notify us when sync completes
    # We use the relay process itself as a simple sync endpoint
    peer_ref = Ref.new({:relay, self()}, {:sync, sync_id})

    Actor.deliver(state.actor_pid, ref, Event.sync(ref, peer_ref))

    state
  end

  # Translate wire refs in a value to local refs
  defp translate_inbound_refs(state, {:embedded, wire_ref_value}) do
    case Packet.decode_wire_ref(wire_ref_value) do
      {:ok, %Packet.WireRef{variant: :mine, oid: oid}} ->
        # Peer is sending us a reference they manage
        # Import it via our import membrane
        {ref, import_membrane} =
          Membrane.import(state.import_membrane, oid, [], fn oid ->
            # Create a proxy entity for this remote ref
            {:ok, proxy_ref} =
              Actor.spawn_entity(state.actor_pid, :root, %Proxy{
                relay_pid: self(),
                oid: oid
              })

            proxy_ref
          end)

        state = %{state | import_membrane: import_membrane}
        {{:embedded, ref}, state}

      {:ok, %Packet.WireRef{variant: :yours, oid: oid, attenuation: attenuation}} ->
        # Peer is returning a reference we exported to them
        case Membrane.lookup_by_oid(state.export_membrane, oid) do
          {:ok, ref} ->
            # Apply any attenuation
            ref =
              if attenuation == [] do
                ref
              else
                Ref.with_attenuation(ref, attenuation)
              end

            {{:embedded, ref}, state}

          :error ->
            Logger.warning("Relay: yours ref for unknown OID #{oid}")
            {{:embedded, nil}, state}
        end

      {:error, reason} ->
        Logger.warning("Relay: failed to decode wire ref: #{inspect(reason)}")
        {{:embedded, nil}, state}
    end
  end

  defp translate_inbound_refs(state, {:record, {label, fields}}) do
    {translated_label, state} = translate_inbound_refs(state, label)
    {translated_fields, state} = translate_inbound_refs_list(state, fields)
    {{:record, {translated_label, translated_fields}}, state}
  end

  defp translate_inbound_refs(state, {:sequence, elements}) do
    {translated, state} = translate_inbound_refs_list(state, elements)
    {{:sequence, translated}, state}
  end

  defp translate_inbound_refs(state, {:set, elements}) do
    {translated, state} = translate_inbound_refs_list(state, MapSet.to_list(elements))
    {{:set, MapSet.new(translated)}, state}
  end

  defp translate_inbound_refs(state, {:dictionary, entries}) do
    {translated, state} =
      Enum.reduce(entries, {[], state}, fn {k, v}, {acc, s} ->
        {tk, s} = translate_inbound_refs(s, k)
        {tv, s} = translate_inbound_refs(s, v)
        {[{tk, tv} | acc], s}
      end)

    {{:dictionary, Map.new(translated)}, state}
  end

  # Atomic values pass through unchanged
  defp translate_inbound_refs(state, value), do: {value, state}

  defp translate_inbound_refs_list(state, list) do
    {translated, state} =
      Enum.reduce(list, {[], state}, fn elem, {acc, s} ->
        {t, s} = translate_inbound_refs(s, elem)
        {[t | acc], s}
      end)

    {Enum.reverse(translated), state}
  end

  # Handle outbound events (local -> peer)

  defp handle_outbound_assert(state, oid, assertion, handle) do
    # Translate local refs to wire refs
    {translated_assertion, state} = translate_outbound_refs(state, assertion)

    # Map local handle to wire handle
    wire_handle = state.next_wire_handle
    state = %{state | next_wire_handle: wire_handle + 1}

    # Track handle -> wire_handle, wire_handle -> handle, and handle -> oid
    state = %{
      state
      | local_handles: Map.put(state.local_handles, handle, wire_handle),
        wire_handles: Map.put(state.wire_handles, wire_handle, handle),
        handle_to_oid: Map.put(state.handle_to_oid, handle, oid)
    }

    # Send assert packet
    event = Packet.event(oid, Packet.assert(translated_assertion, wire_handle))
    turn = Packet.turn([event])

    case do_send_packet(state, turn) do
      {:ok, state} -> state
      {:error, _reason} -> state
    end
  end

  defp handle_outbound_retract(state, handle) do
    case Map.get(state.local_handles, handle) do
      nil ->
        Logger.warning("Relay: retract for unknown local handle")
        state

      wire_handle ->
        # Look up the OID that this assertion was originally made to
        case Map.fetch(state.handle_to_oid, handle) do
          {:ok, oid} ->
            # Clean up all mappings
            state = %{
              state
              | local_handles: Map.delete(state.local_handles, handle),
                wire_handles: Map.delete(state.wire_handles, wire_handle),
                handle_to_oid: Map.delete(state.handle_to_oid, handle)
            }

            event = Packet.event(oid, Packet.retract(wire_handle))
            turn = Packet.turn([event])

            case do_send_packet(state, turn) do
              {:ok, state} -> state
              {:error, _reason} -> state
            end

          :error ->
            Logger.error(
              "Relay: missing OID mapping for handle #{inspect(handle)}, dropping retract"
            )

            # Clean up handle mappings but don't send a retract with wrong OID
            %{
              state
              | local_handles: Map.delete(state.local_handles, handle),
                wire_handles: Map.delete(state.wire_handles, wire_handle)
            }
        end
    end
  end

  defp handle_outbound_message(state, oid, message) do
    {translated_message, state} = translate_outbound_refs(state, message)

    event = Packet.event(oid, Packet.message(translated_message))
    turn = Packet.turn([event])

    case do_send_packet(state, turn) do
      {:ok, state} -> state
      {:error, _reason} -> state
    end
  end

  defp handle_outbound_sync(state, oid, peer) do
    # Track the peer to notify when we get Synced back
    sync_id = state.next_sync_id

    state = %{
      state
      | pending_syncs: Map.put(state.pending_syncs, sync_id, peer),
        next_sync_id: sync_id + 1
    }

    event = Packet.event(oid, Packet.sync())
    turn = Packet.turn([event])

    case do_send_packet(state, turn) do
      {:ok, state} -> state
      {:error, _reason} -> state
    end
  end

  defp handle_sync_response(state, sync_id) do
    case Map.get(state.pending_syncs, sync_id) do
      nil ->
        state

      peer_ref ->
        state = %{state | pending_syncs: Map.delete(state.pending_syncs, sync_id)}

        # Send Synced message to the peer
        Actor.send_message(state.actor_pid, peer_ref, {:symbol, "synced"})

        state
    end
  end

  # Translate local refs in a value to wire refs
  defp translate_outbound_refs(state, {:embedded, %Ref{} = ref}) do
    # Check if this ref is in our import membrane (peer's export)
    case Membrane.lookup_by_ref(state.import_membrane, ref) do
      {:ok, oid} ->
        # Return to sender with attenuation if any
        attenuation = Ref.attenuation(ref) || []
        {:ok, wire_ref} = Packet.encode_wire_ref(Packet.WireRef.yours(oid, attenuation))
        {{:embedded, wire_ref}, state}

      :error ->
        # Export this ref to the peer
        {oid, export_membrane} = Membrane.export(state.export_membrane, ref)
        state = %{state | export_membrane: export_membrane}
        {:ok, wire_ref} = Packet.encode_wire_ref(Packet.WireRef.mine(oid))
        {{:embedded, wire_ref}, state}
    end
  end

  defp translate_outbound_refs(state, {:record, {label, fields}}) do
    {translated_label, state} = translate_outbound_refs(state, label)
    {translated_fields, state} = translate_outbound_refs_list(state, fields)
    {{:record, {translated_label, translated_fields}}, state}
  end

  defp translate_outbound_refs(state, {:sequence, elements}) do
    {translated, state} = translate_outbound_refs_list(state, elements)
    {{:sequence, translated}, state}
  end

  defp translate_outbound_refs(state, {:set, elements}) do
    {translated, state} = translate_outbound_refs_list(state, MapSet.to_list(elements))
    {{:set, MapSet.new(translated)}, state}
  end

  defp translate_outbound_refs(state, {:dictionary, entries}) do
    {translated, state} =
      Enum.reduce(entries, {[], state}, fn {k, v}, {acc, s} ->
        {tk, s} = translate_outbound_refs(s, k)
        {tv, s} = translate_outbound_refs(s, v)
        {[{tk, tv} | acc], s}
      end)

    {{:dictionary, Map.new(translated)}, state}
  end

  defp translate_outbound_refs(state, value), do: {value, state}

  defp translate_outbound_refs_list(state, list) do
    {translated, state} =
      Enum.reduce(list, {[], state}, fn elem, {acc, s} ->
        {t, s} = translate_outbound_refs(s, elem)
        {[t | acc], s}
      end)

    {Enum.reverse(translated), state}
  end

  # Retract all assertions when relay terminates
  defp retract_all_inbound_assertions(state) do
    Enum.each(state.wire_handles, fn {_wire_handle, local_handle} ->
      # Find the ref this was asserted to
      # For simplicity, use the dataspace ref
      Actor.deliver(
        state.actor_pid,
        state.dataspace_ref,
        Event.retract(state.dataspace_ref, local_handle)
      )
    end)
  end

  # Socket helpers

  defp set_socket_active(%{socket: socket, transport: :gen_tcp}) do
    case :inet.setopts(socket, active: :once) do
      :ok ->
        :ok

      {:error, reason} ->
        Logger.error("Relay: failed to set socket active: #{inspect(reason)}")
        {:error, reason}
    end
  end

  defp set_socket_active(%{socket: socket, transport: :ssl}) do
    :ssl.setopts(socket, active: :once)
  end

  defp do_send_packet(state, packet) do
    case Framing.encode(packet) do
      {:ok, data} ->
        case send_data(state, data) do
          :ok -> {:ok, state}
          {:error, reason} -> {:error, reason}
        end

      {:error, reason} ->
        {:error, {:encode_error, reason}}
    end
  end

  defp send_data(%{socket: socket, transport: :gen_tcp}, data) do
    :gen_tcp.send(socket, data)
  end

  defp send_data(%{socket: socket, transport: :ssl}, data) do
    :ssl.send(socket, data)
  end

  defp close_socket(%{socket: socket, transport: :gen_tcp}) do
    :gen_tcp.close(socket)
  end

  defp close_socket(%{socket: socket, transport: :ssl}) do
    :ssl.close(socket)
  end
end
