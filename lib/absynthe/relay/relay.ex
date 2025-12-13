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
          # Inbound handle mappings (peer -> local)
          inbound_wire_handles: %{non_neg_integer() => Handle.t()},
          inbound_local_handles: %{Handle.t() => non_neg_integer()},
          # Outbound handle mappings (local -> peer)
          outbound_wire_handles: %{non_neg_integer() => Handle.t()},
          outbound_local_handles: %{Handle.t() => non_neg_integer()},
          handle_to_oid: %{Handle.t() => non_neg_integer()},
          # Track which OIDs were imported for each inbound assertion (for decref on retract)
          inbound_handle_oids: %{Handle.t() => [non_neg_integer()]},
          # Track which ref each inbound assertion was delivered to (for correct retraction on termination)
          inbound_handle_refs: %{Handle.t() => Ref.t()},
          # Track which OIDs were exported for each outbound assertion (for decref on retract)
          outbound_handle_oids: %{Handle.t() => [non_neg_integer()]},
          next_wire_handle: non_neg_integer(),
          pending_syncs: %{non_neg_integer() => Ref.t()},
          next_sync_id: non_neg_integer(),
          closed: boolean(),
          # Idle timeout configuration
          idle_timeout: timeout(),
          idle_timer_ref: reference() | nil,
          idle_timer_token: reference() | nil,
          # Connection identifier for logging
          connection_id: String.t()
        }

  # Client API

  @doc """
  Starts a relay for the given socket.

  ## Options

  - `:socket` - The connected socket (required)
  - `:transport` - Transport module, `:gen_tcp` or `:ssl` (default: `:gen_tcp`)
  - `:dataspace_ref` - Ref to the target dataspace entity (required)
  - `:actor_pid` - PID of the hosting actor (default: start a new actor)
  - `:idle_timeout` - Idle timeout in milliseconds (default: `:infinity`)
  - `:connection_id` - Unique identifier for logging (default: auto-generated)

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
    idle_timeout = Keyword.get(opts, :idle_timeout, :infinity)

    connection_id =
      Keyword.get_lazy(opts, :connection_id, fn ->
        generate_connection_id()
      end)

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
      inbound_wire_handles: %{},
      inbound_local_handles: %{},
      outbound_wire_handles: %{},
      outbound_local_handles: %{},
      handle_to_oid: %{},
      inbound_handle_oids: %{},
      inbound_handle_refs: %{},
      outbound_handle_oids: %{},
      next_wire_handle: 0,
      pending_syncs: %{},
      next_sync_id: 0,
      closed: false,
      idle_timeout: idle_timeout,
      idle_timer_ref: nil,
      idle_timer_token: nil,
      connection_id: connection_id
    }

    Logger.info("[#{connection_id}] Relay started")

    # Don't activate socket yet - wait for activate/1 to be called
    # after controlling_process has transferred ownership

    {:ok, state}
  end

  defp generate_connection_id do
    :crypto.strong_rand_bytes(4) |> Base.encode16(case: :lower)
  end

  defp reset_idle_timer(%{idle_timeout: :infinity} = state), do: state

  defp reset_idle_timer(state) do
    # Cancel existing timer if any
    if state.idle_timer_ref do
      Process.cancel_timer(state.idle_timer_ref)
    end

    # Generate a unique token for this timer to avoid acting on stale messages
    timer_token = make_ref()
    timer_ref = Process.send_after(self(), {:idle_timeout, timer_token}, state.idle_timeout)
    %{state | idle_timer_ref: timer_ref, idle_timer_token: timer_token}
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
    Logger.debug("[#{state.connection_id}] Activating socket")
    set_socket_active(state)
    state = reset_idle_timer(state)
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
    Logger.info("[#{state.connection_id}] Connection closed by peer")
    {:stop, :normal, %{state | closed: true}}
  end

  def handle_info({:ssl_closed, socket}, %{socket: socket} = state) do
    Logger.info("[#{state.connection_id}] SSL connection closed by peer")
    {:stop, :normal, %{state | closed: true}}
  end

  # Handle TCP error
  def handle_info({:tcp_error, socket, reason}, %{socket: socket} = state) do
    Logger.error("[#{state.connection_id}] TCP error: #{inspect(reason)}")
    {:stop, {:error, reason}, %{state | closed: true}}
  end

  def handle_info({:ssl_error, socket, reason}, %{socket: socket} = state) do
    Logger.error("[#{state.connection_id}] SSL error: #{inspect(reason)}")
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

  # Handle idle timeout - verify token matches to avoid acting on stale timer messages
  def handle_info({:idle_timeout, token}, state) when token == state.idle_timer_token do
    Logger.info("[#{state.connection_id}] Idle timeout, closing connection")
    {:stop, {:shutdown, :idle_timeout}, %{state | closed: true}}
  end

  # Ignore stale idle timeout messages with non-matching tokens
  def handle_info({:idle_timeout, _stale_token}, state) do
    {:noreply, state}
  end

  @impl GenServer
  def terminate(reason, state) do
    Logger.info("[#{state.connection_id}] Relay terminating: #{inspect(reason)}")

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
    # Reset idle timer on any incoming data
    state = reset_idle_timer(state)

    case Framing.append_and_decode(state.recv_buffer, data) do
      {:ok, packets, buffer} ->
        state = %{state | recv_buffer: buffer}

        # Process each packet with error handling
        # Use reduce_while to stop early if we receive an Error packet
        result =
          Enum.reduce_while(packets, {:ok, state}, fn packet, {:ok, acc_state} ->
            try do
              case process_inbound_packet(acc_state, packet) do
                {:ok, new_state} -> {:cont, {:ok, new_state}}
                {:error, new_state, reason} -> {:halt, {:error, new_state, reason}}
              end
            rescue
              error ->
                Logger.error(
                  "[#{state.connection_id}] Error processing packet: #{inspect(error)}"
                )

                Logger.error(Exception.format_stacktrace(__STACKTRACE__))
                {:cont, {:ok, acc_state}}
            end
          end)

        case result do
          {:ok, state} ->
            # Re-enable socket for more data
            set_socket_active(state)
            {:noreply, state}

          {:error, state, reason} ->
            # Error packet received - close connection gracefully.
            # Per Syndicate protocol: receiving an error packet means the peer crashed.
            # Use {:shutdown, reason} to avoid cascading crashes to linked processes.
            {:stop, {:shutdown, {:peer_error, reason}}, %{state | closed: true}}
        end

      {:error, reason, packets, _buffer} ->
        # Process any packets that were successfully decoded before the error
        # Use reduce_while to stop early if we receive an Error packet
        result =
          Enum.reduce_while(packets, {:ok, state}, fn packet, {:ok, acc_state} ->
            try do
              case process_inbound_packet(acc_state, packet) do
                {:ok, new_state} -> {:cont, {:ok, new_state}}
                {:error, new_state, reason} -> {:halt, {:error, new_state, reason}}
              end
            rescue
              error ->
                Logger.error(
                  "[#{state.connection_id}] Error processing packet: #{inspect(error)}"
                )

                Logger.error(Exception.format_stacktrace(__STACKTRACE__))
                {:cont, {:ok, acc_state}}
            end
          end)

        # Check if an error packet was received before the decode error
        state =
          case result do
            {:ok, s} -> s
            {:error, s, _peer_error} -> s
          end

        # Log the decode error and close the connection
        Logger.error(
          "[#{state.connection_id}] Framing decode error: #{inspect(reason)}, closing connection"
        )

        # Send error packet to peer before closing
        error_packet = Packet.error("protocol error", {:symbol, "decode_error"})

        case do_send_packet(state, error_packet) do
          {:ok, state} ->
            {:stop, {:error, {:decode_error, reason}}, %{state | closed: true}}

          {:error, _send_error} ->
            {:stop, {:error, {:decode_error, reason}}, %{state | closed: true}}
        end
    end
  end

  defp process_inbound_packet(state, :nop) do
    # Keepalive - no action needed
    {:ok, state}
  end

  defp process_inbound_packet(state, %Packet.Turn{events: events}) do
    {:ok, Enum.reduce(events, state, &process_turn_event/2)}
  end

  defp process_inbound_packet(state, %Packet.Error{message: msg, detail: detail}) do
    Logger.error("[#{state.connection_id}] Received error from peer: #{msg} (#{inspect(detail)})")
    # Per Syndicate protocol: receiving an error packet means the sender has
    # crashed and will not respond further. Close the connection.
    {:error, state, {:peer_error, msg, detail}}
  end

  defp process_inbound_packet(state, %Packet.Extension{label: label}) do
    Logger.debug("[#{state.connection_id}] Ignoring unknown extension: #{inspect(label)}")
    {:ok, state}
  end

  defp process_turn_event(%Packet.TurnEvent{oid: oid, event: event}, state) do
    # Look up the target ref from our export membrane
    case Membrane.lookup_by_oid(state.export_membrane, oid) do
      {:ok, ref} ->
        process_event_for_ref(state, ref, oid, event)

      :error ->
        Logger.warning("[#{state.connection_id}] Received event for unknown OID #{oid}")
        state
    end
  end

  defp process_event_for_ref(state, ref, _target_oid, %Packet.Assert{
         assertion: assertion,
         handle: wire_handle
       }) do
    # Translate embedded wire refs in the assertion to local refs
    {translated_assertion, state, oids_referenced} = translate_inbound_refs(state, assertion)

    # Create a local handle
    local_handle = Handle.new({:relay, self()}, wire_handle)

    # Increment refcount for each OID embedded in the assertion
    # Note: We do NOT count the target OID - exported OIDs (like OID 0 for dataspace)
    # remain valid for the lifetime of the relay, not tied to individual assertion lifecycles.
    # The refcount tracks embedded OIDs which represent transient capabilities.
    {import_membrane, export_membrane} =
      Enum.reduce(oids_referenced, {state.import_membrane, state.export_membrane}, fn
        {:export, oid}, {imp, exp} -> {imp, Membrane.inc_ref(exp, oid)}
        oid, {imp, exp} -> {Membrane.inc_ref(imp, oid), exp}
      end)

    # Track handle mapping, OIDs, and target ref for later retraction
    state = %{
      state
      | inbound_wire_handles: Map.put(state.inbound_wire_handles, wire_handle, local_handle),
        inbound_local_handles: Map.put(state.inbound_local_handles, local_handle, wire_handle),
        import_membrane: import_membrane,
        export_membrane: export_membrane,
        inbound_handle_oids: Map.put(state.inbound_handle_oids, local_handle, oids_referenced),
        inbound_handle_refs: Map.put(state.inbound_handle_refs, local_handle, ref)
    }

    # Deliver assertion to the local entity
    Actor.deliver(
      state.actor_pid,
      ref,
      Event.assert(ref, translated_assertion, local_handle)
    )

    state
  end

  defp process_event_for_ref(state, ref, _target_oid, %Packet.Retract{handle: wire_handle}) do
    case Map.get(state.inbound_wire_handles, wire_handle) do
      nil ->
        Logger.warning(
          "[#{state.connection_id}] Retract for unknown inbound handle #{wire_handle}"
        )

        state

      local_handle ->
        # Get the OIDs that were referenced by this assertion
        oids_referenced = Map.get(state.inbound_handle_oids, local_handle, [])

        # Decrement refcount for each OID, potentially GC'ing them
        {import_membrane, export_membrane} =
          Enum.reduce(oids_referenced, {state.import_membrane, state.export_membrane}, fn
            {:export, oid}, {imp, exp} ->
              {exp, _status} = Membrane.dec_ref(exp, oid)
              {imp, exp}

            oid, {imp, exp} ->
              {imp, _status} = Membrane.dec_ref(imp, oid)
              {imp, exp}
          end)

        # Clean up handle mapping
        state = %{
          state
          | inbound_wire_handles: Map.delete(state.inbound_wire_handles, wire_handle),
            inbound_local_handles: Map.delete(state.inbound_local_handles, local_handle),
            import_membrane: import_membrane,
            export_membrane: export_membrane,
            inbound_handle_oids: Map.delete(state.inbound_handle_oids, local_handle),
            inbound_handle_refs: Map.delete(state.inbound_handle_refs, local_handle)
        }

        # Deliver retraction
        Actor.deliver(state.actor_pid, ref, Event.retract(ref, local_handle))

        state
    end
  end

  defp process_event_for_ref(state, ref, _target_oid, %Packet.Message{body: body}) do
    # Translate embedded wire refs
    # Note: Messages don't need refcount tracking since they're not persisted
    {translated_body, state, _oids} = translate_inbound_refs(state, body)

    # Deliver message
    Actor.deliver(state.actor_pid, ref, Event.message(ref, translated_body))

    state
  end

  defp process_event_for_ref(state, ref, _target_oid, %Packet.Sync{}) do
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
  # Returns {translated_value, state, imported_oids}
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
        # Track this OID as imported (for refcounting)
        {{:embedded, ref}, state, [oid]}

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

            # Track this OID in export membrane (for refcounting)
            {{:embedded, ref}, state, [{:export, oid}]}

          :error ->
            Logger.warning("[#{state.connection_id}] Yours ref for unknown OID #{oid}")
            {{:embedded, nil}, state, []}
        end

      {:error, reason} ->
        Logger.warning("[#{state.connection_id}] Failed to decode wire ref: #{inspect(reason)}")
        {{:embedded, nil}, state, []}
    end
  end

  defp translate_inbound_refs(state, {:record, {label, fields}}) do
    {translated_label, state, oids1} = translate_inbound_refs(state, label)
    {translated_fields, state, oids2} = translate_inbound_refs_list(state, fields)
    {{:record, {translated_label, translated_fields}}, state, oids1 ++ oids2}
  end

  defp translate_inbound_refs(state, {:sequence, elements}) do
    {translated, state, oids} = translate_inbound_refs_list(state, elements)
    {{:sequence, translated}, state, oids}
  end

  defp translate_inbound_refs(state, {:set, elements}) do
    {translated, state, oids} = translate_inbound_refs_list(state, MapSet.to_list(elements))
    {{:set, MapSet.new(translated)}, state, oids}
  end

  defp translate_inbound_refs(state, {:dictionary, entries}) do
    {translated, state, oids} =
      Enum.reduce(entries, {[], state, []}, fn {k, v}, {acc, s, acc_oids} ->
        {tk, s, oids_k} = translate_inbound_refs(s, k)
        {tv, s, oids_v} = translate_inbound_refs(s, v)
        {[{tk, tv} | acc], s, acc_oids ++ oids_k ++ oids_v}
      end)

    {{:dictionary, Map.new(translated)}, state, oids}
  end

  # Atomic values pass through unchanged
  defp translate_inbound_refs(state, value), do: {value, state, []}

  defp translate_inbound_refs_list(state, list) do
    {translated, state, oids} =
      Enum.reduce(list, {[], state, []}, fn elem, {acc, s, acc_oids} ->
        {t, s, elem_oids} = translate_inbound_refs(s, elem)
        {[t | acc], s, acc_oids ++ elem_oids}
      end)

    {Enum.reverse(translated), state, oids}
  end

  # Handle outbound events (local -> peer)

  defp handle_outbound_assert(state, oid, assertion, handle) do
    # Translate local refs to wire refs
    {translated_assertion, state, oids_referenced} = translate_outbound_refs(state, assertion)

    # Map local handle to wire handle
    wire_handle = state.next_wire_handle
    state = %{state | next_wire_handle: wire_handle + 1}

    # Increment refcount for each OID referenced in the assertion
    {export_membrane, import_membrane} =
      Enum.reduce(oids_referenced, {state.export_membrane, state.import_membrane}, fn
        {:import, oid}, {exp, imp} -> {exp, Membrane.inc_ref(imp, oid)}
        oid, {exp, imp} -> {Membrane.inc_ref(exp, oid), imp}
      end)

    # Track handle -> wire_handle, wire_handle -> handle, handle -> oid, and handle -> oids
    state = %{
      state
      | outbound_local_handles: Map.put(state.outbound_local_handles, handle, wire_handle),
        outbound_wire_handles: Map.put(state.outbound_wire_handles, wire_handle, handle),
        handle_to_oid: Map.put(state.handle_to_oid, handle, oid),
        export_membrane: export_membrane,
        import_membrane: import_membrane,
        outbound_handle_oids: Map.put(state.outbound_handle_oids, handle, oids_referenced)
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
    case Map.get(state.outbound_local_handles, handle) do
      nil ->
        Logger.warning("[#{state.connection_id}] Retract for unknown outbound local handle")
        state

      wire_handle ->
        # Look up the OID that this assertion was originally made to
        case Map.fetch(state.handle_to_oid, handle) do
          {:ok, oid} ->
            # Get the OIDs that were referenced by this assertion
            oids_referenced = Map.get(state.outbound_handle_oids, handle, [])

            # Decrement refcount for each OID, potentially GC'ing them
            {export_membrane, import_membrane} =
              Enum.reduce(oids_referenced, {state.export_membrane, state.import_membrane}, fn
                {:import, oid}, {exp, imp} ->
                  {imp, _status} = Membrane.dec_ref(imp, oid)
                  {exp, imp}

                oid, {exp, imp} ->
                  {exp, _status} = Membrane.dec_ref(exp, oid)
                  {exp, imp}
              end)

            # Clean up all mappings
            state = %{
              state
              | outbound_local_handles: Map.delete(state.outbound_local_handles, handle),
                outbound_wire_handles: Map.delete(state.outbound_wire_handles, wire_handle),
                handle_to_oid: Map.delete(state.handle_to_oid, handle),
                export_membrane: export_membrane,
                import_membrane: import_membrane,
                outbound_handle_oids: Map.delete(state.outbound_handle_oids, handle)
            }

            event = Packet.event(oid, Packet.retract(wire_handle))
            turn = Packet.turn([event])

            case do_send_packet(state, turn) do
              {:ok, state} -> state
              {:error, _reason} -> state
            end

          :error ->
            Logger.error(
              "[#{state.connection_id}] Missing OID mapping for handle #{inspect(handle)}, dropping retract"
            )

            # Clean up handle mappings but don't send a retract with wrong OID
            # Also clean up OID tracking
            %{
              state
              | outbound_local_handles: Map.delete(state.outbound_local_handles, handle),
                outbound_wire_handles: Map.delete(state.outbound_wire_handles, wire_handle),
                outbound_handle_oids: Map.delete(state.outbound_handle_oids, handle)
            }
        end
    end
  end

  defp handle_outbound_message(state, oid, message) do
    # Note: Messages don't need refcount tracking since they're transient
    {translated_message, state, _oids} = translate_outbound_refs(state, message)

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
  # Returns {translated_value, state, exported_oids}
  defp translate_outbound_refs(state, {:embedded, %Ref{} = ref}) do
    # Check if this ref is in our import membrane (peer's export)
    case Membrane.lookup_by_ref(state.import_membrane, ref) do
      {:ok, oid} ->
        # Return to sender with attenuation if any
        attenuation = Ref.attenuation(ref) || []
        {:ok, wire_ref} = Packet.encode_wire_ref(Packet.WireRef.yours(oid, attenuation))
        # Track this OID reference in import membrane
        {{:embedded, wire_ref}, state, [{:import, oid}]}

      :error ->
        # Export this ref to the peer
        {oid, export_membrane} = Membrane.export(state.export_membrane, ref)
        state = %{state | export_membrane: export_membrane}
        {:ok, wire_ref} = Packet.encode_wire_ref(Packet.WireRef.mine(oid))
        # Track this OID as exported
        {{:embedded, wire_ref}, state, [oid]}
    end
  end

  defp translate_outbound_refs(state, {:record, {label, fields}}) do
    {translated_label, state, oids1} = translate_outbound_refs(state, label)
    {translated_fields, state, oids2} = translate_outbound_refs_list(state, fields)
    {{:record, {translated_label, translated_fields}}, state, oids1 ++ oids2}
  end

  defp translate_outbound_refs(state, {:sequence, elements}) do
    {translated, state, oids} = translate_outbound_refs_list(state, elements)
    {{:sequence, translated}, state, oids}
  end

  defp translate_outbound_refs(state, {:set, elements}) do
    {translated, state, oids} = translate_outbound_refs_list(state, MapSet.to_list(elements))
    {{:set, MapSet.new(translated)}, state, oids}
  end

  defp translate_outbound_refs(state, {:dictionary, entries}) do
    {translated, state, oids} =
      Enum.reduce(entries, {[], state, []}, fn {k, v}, {acc, s, acc_oids} ->
        {tk, s, oids_k} = translate_outbound_refs(s, k)
        {tv, s, oids_v} = translate_outbound_refs(s, v)
        {[{tk, tv} | acc], s, acc_oids ++ oids_k ++ oids_v}
      end)

    {{:dictionary, Map.new(translated)}, state, oids}
  end

  defp translate_outbound_refs(state, value), do: {value, state, []}

  defp translate_outbound_refs_list(state, list) do
    {translated, state, oids} =
      Enum.reduce(list, {[], state, []}, fn elem, {acc, s, acc_oids} ->
        {t, s, elem_oids} = translate_outbound_refs(s, elem)
        {[t | acc], s, acc_oids ++ elem_oids}
      end)

    {Enum.reverse(translated), state, oids}
  end

  # Retract all inbound assertions when relay terminates
  defp retract_all_inbound_assertions(state) do
    Enum.each(state.inbound_wire_handles, fn {_wire_handle, local_handle} ->
      # Look up the ref this assertion was originally delivered to
      case Map.get(state.inbound_handle_refs, local_handle) do
        nil ->
          # Fallback to dataspace_ref if not found (shouldn't happen in normal operation)
          Logger.warning(
            "[#{state.connection_id}] No tracked ref for handle #{inspect(local_handle)}, using dataspace_ref"
          )

          Actor.deliver(
            state.actor_pid,
            state.dataspace_ref,
            Event.retract(state.dataspace_ref, local_handle)
          )

        target_ref ->
          Actor.deliver(
            state.actor_pid,
            target_ref,
            Event.retract(target_ref, local_handle)
          )
      end
    end)
  end

  # Socket helpers

  defp set_socket_active(%{socket: socket, transport: :gen_tcp, connection_id: conn_id}) do
    case :inet.setopts(socket, active: :once) do
      :ok ->
        :ok

      {:error, reason} ->
        Logger.error("[#{conn_id}] Failed to set socket active: #{inspect(reason)}")
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
          :ok ->
            # Reset idle timer on successful outbound send
            {:ok, reset_idle_timer(state)}

          {:error, reason} ->
            {:error, reason}
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
