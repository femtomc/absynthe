defmodule Absynthe.Core.Actor do
  @moduledoc """
  GenServer-based actor implementation for the Syndicated Actor Model.

  ## Overview

  The Actor is the fundamental unit of computation in the Syndicated Actor Model.
  Each actor is an isolated GenServer process that:

  - Hosts a collection of entities (computational objects)
  - Processes events through turns (transactions)
  - Manages outbound assertions to other actors
  - Maintains conversational context through facets
  - Provides OTP supervision integration

  ## Architecture

  An actor organizes entities into facets, which represent conversational scopes.
  Each facet contains zero or more entities and may have child facets, forming
  a tree structure rooted at the actor's root facet.

  ```
  Actor
  └─ Root Facet
     ├─ Entity 1
     ├─ Entity 2
     └─ Child Facet
        ├─ Entity 3
        └─ Entity 4
  ```

  When a facet terminates (either explicitly or due to an error), all its
  entities and child facets are also terminated, ensuring clean resource
  cleanup through "fate-sharing."

  ## Turn-Based Execution

  All entity operations occur within a Turn, which is a transaction-like
  execution context. A turn:

  1. Begins when an event (message, assertion, retraction) arrives
  2. Dispatches to the target entity via the Entity protocol
  3. Accumulates pending actions (assertions, messages, spawns)
  4. On success: commits by executing all pending actions
  5. On failure: rolls back by discarding all pending actions

  This ensures that actors maintain consistency and that partial updates
  never escape to the rest of the system.

  ## Actor State

  The actor maintains the following state:

  - `id` - Unique identifier for this actor (typically a PID or registered name)
  - `next_entity_id` - Monotonic counter for generating entity IDs
  - `next_handle_id` - Monotonic counter for generating assertion handles
  - `root_facet_id` - ID of the root facet (entry point for the actor)
  - `facets` - Map of facet_id => facet data structure
  - `entities` - Map of entity_id => {facet_id, entity_struct}
  - `outbound_assertions` - Map of handle => {ref, assertion} for retractions

  ## Entity Management

  Entities are spawned within facets using `spawn_entity/3`. Each entity:

  - Receives a unique entity_id
  - Is associated with exactly one facet (its parent)
  - Implements the `Absynthe.Core.Entity` protocol
  - Can be referenced via `Absynthe.Core.Ref` structs

  ## Event Delivery

  Events are delivered to entities through one of four mechanisms:

  1. **Assert** - Publish an assertion that persists until retracted
  2. **Retract** - Remove a previously published assertion
  3. **Message** - Send a one-shot, stateless message
  4. **Sync** - Establish a synchronization barrier

  All events are delivered asynchronously via `handle_cast/2`, ensuring that
  actors don't block each other and maintaining isolation.

  ## Lifecycle

  A typical actor lifecycle:

  1. **Start** - Call `start_link/1` with initialization options
  2. **Initialize** - `init/1` creates the root facet and initial entities
  3. **Process Events** - Handle messages, assertions, and retractions
  4. **Spawn Entities** - Dynamically create new entities as needed
  5. **Terminate** - Clean shutdown, retract all assertions, notify peers

  ## Examples

  Starting an actor with a simple entity:

      defmodule MyEntity do
        defstruct [:counter]
      end

      defimpl Absynthe.Core.Entity, for: MyEntity do
        def on_message(entity, {:increment, amount}, turn) do
          new_entity = %{entity | counter: entity.counter + amount}
          IO.puts("Counter is now: \#{new_entity.counter}")
          {new_entity, turn}
        end

        def on_publish(entity, _assertion, _handle, turn), do: {entity, turn}
        def on_retract(entity, _handle, turn), do: {entity, turn}
        def on_sync(entity, _peer, turn), do: {entity, turn}
      end

      # Start the actor
      {:ok, actor} = Absynthe.Core.Actor.start_link(id: :my_actor)

      # Spawn an entity in the root facet
      entity = %MyEntity{counter: 0}
      {:ok, ref} = Absynthe.Core.Actor.spawn_entity(actor, :root, entity)

      # Send a message to the entity
      Absynthe.Core.Actor.send_message(actor, ref, {:increment, 5})

  ## Design Decisions

  ### Why GenServer?

  GenServers provide:
  - Built-in OTP supervision and fault tolerance
  - Sequential message processing (simplifies consistency)
  - Standardized process lifecycle management
  - Integration with standard Elixir monitoring tools

  ### Why Turn-Based Execution?

  Turns ensure that:
  - Entity state changes are atomic (all-or-nothing)
  - Side effects are properly sequenced
  - Errors don't leave the system in an inconsistent state
  - Debugging is easier (clear transaction boundaries)

  ### Why Facets?

  Facets provide:
  - Scoped resource management (cleanup on termination)
  - Hierarchical organization of entities
  - Fate-sharing semantics (parent failure cascades)
  - Natural mapping to conversation structure

  ## Future Extensions

  The current implementation is local-only. Future versions may add:

  - Network transparency (remote actor communication)
  - Persistent state (actor snapshots and replay)
  - Distributed dataspace (multi-node coordination)
  - Capability attenuation (fine-grained access control)

  ## See Also

  - `Absynthe.Core.Entity` - Protocol for entity event handling
  - `Absynthe.Core.Ref` - Entity references and capabilities
  - `Absynthe.Core.Turn` - Transaction context for entity operations
  - `Absynthe.Core.Facet` - Conversational scope management
  - `Absynthe.Protocol.Event` - Event type definitions
  """

  use GenServer
  require Logger

  alias Absynthe.Core.Ref
  alias Absynthe.Core.Entity
  alias Absynthe.Core.Turn
  alias Absynthe.Core.Rewrite
  alias Absynthe.Protocol.Event
  alias Absynthe.Assertions.Handle

  # Import Event sub-modules for pattern matching
  alias Event.{Assert, Retract, Message, Sync}

  # Type Definitions

  @typedoc "Unique identifier for an actor (typically PID or atom)"
  @type actor_id :: term()

  @typedoc "Unique identifier for an entity within an actor"
  @type entity_id :: term()

  @typedoc "Unique identifier for a facet within an actor"
  @type facet_id :: term()

  @typedoc """
  Actor state structure.

  Contains all the runtime state for an actor instance, including
  entity registry, facet hierarchy, and outbound assertion tracking.
  """
  @type state :: %{
          id: actor_id(),
          next_entity_id: non_neg_integer(),
          next_handle_id: non_neg_integer(),
          root_facet_id: facet_id(),
          facets: %{facet_id() => facet_state()},
          entities: %{entity_id() => {facet_id(), term()}},
          outbound_assertions: %{Handle.t() => {Ref.t(), term()}},
          # Maps each facet to its owned assertion handles for automatic cleanup
          facet_handles: %{facet_id() => MapSet.t(Handle.t())},
          # Tracks inbound assertions from remote actors for crash cleanup
          # Maps handle -> {ref, assertion, owner_pid}
          inbound_assertions: %{Handle.t() => {Ref.t(), term(), pid()}},
          # Maps owner_pid -> MapSet of handles from that owner
          owner_handles: %{pid() => MapSet.t(Handle.t())},
          # Maps pid -> monitor_ref for demonitoring when no longer needed
          pid_monitors: %{pid() => reference()}
        }

  @typedoc """
  Facet state structure.

  A facet represents a conversational scope containing entities and
  potentially child facets. This is a simplified representation;
  the full Facet module will provide richer functionality.
  """
  @type facet_state :: %{
          id: facet_id(),
          parent_id: facet_id() | nil,
          children: MapSet.t(facet_id()),
          entities: MapSet.t(entity_id())
        }

  # Client API

  @doc """
  Starts a new actor process.

  ## Options

  - `:id` - The actor's unique identifier (defaults to self())
  - `:name` - Optional registered name for the GenServer
  - `:root_facet_id` - ID for the root facet (defaults to :root)

  ## Examples

      # Start an anonymous actor
      {:ok, actor} = Absynthe.Core.Actor.start_link(id: :my_actor)

      # Start a named actor
      {:ok, actor} = Absynthe.Core.Actor.start_link(
        id: :my_actor,
        name: MyApp.MyActor
      )

  ## Returns

  - `{:ok, pid}` - Successfully started actor
  - `{:error, reason}` - Failed to start
  """
  @spec start_link(Keyword.t()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    {gen_opts, init_opts} = Keyword.split(opts, [:name])
    GenServer.start_link(__MODULE__, init_opts, gen_opts)
  end

  @doc """
  Spawns a new entity within the specified facet.

  Creates a new entity and associates it with the given facet. The entity
  must implement the `Absynthe.Core.Entity` protocol to handle events.

  ## Parameters

  - `actor` - The actor PID or registered name
  - `facet_id` - The ID of the facet to spawn the entity in
  - `entity` - The entity struct implementing the Entity protocol

  ## Examples

      entity = %MyEntity{counter: 0}
      {:ok, ref} = Absynthe.Core.Actor.spawn_entity(actor, :root, entity)

  ## Returns

  - `{:ok, ref}` - Successfully spawned, returns entity reference
  - `{:error, reason}` - Failed to spawn
  """
  @spec spawn_entity(GenServer.server(), facet_id(), term()) :: {:ok, Ref.t()} | {:error, term()}
  def spawn_entity(actor, facet_id, entity) do
    GenServer.call(actor, {:spawn_entity, facet_id, entity})
  end

  @doc """
  Delivers an event to an entity.

  This is the low-level event delivery function. Most users should use
  the higher-level functions like `send_message/3`, `assert/3`, etc.

  ## Parameters

  - `actor` - The actor PID or registered name
  - `ref` - The entity reference
  - `event` - The event to deliver

  ## Examples

      event = Event.message(ref, {:string, "Hello"})
      :ok = Absynthe.Core.Actor.deliver(actor, ref, event)

  ## Returns

  - `:ok` - Event queued for delivery
  """
  @spec deliver(GenServer.server(), Ref.t(), Event.t()) :: :ok
  def deliver(actor, ref, event) do
    GenServer.cast(actor, {:deliver, ref, event})
  end

  @doc """
  Sends a message to an entity.

  Messages are ephemeral, one-shot communications that don't persist
  in the dataspace. They're delivered exactly once to the target entity.

  ## Parameters

  - `actor` - The actor PID or registered name
  - `ref` - The entity reference
  - `message` - The message body (Preserves value)

  ## Examples

      ref = %Absynthe.Core.Ref{actor_id: :my_actor, entity_id: 1}
      Absynthe.Core.Actor.send_message(actor, ref, {:string, "ping"})

  ## Returns

  - `:ok` - Message queued for delivery
  """
  @spec send_message(GenServer.server(), Ref.t(), term()) :: :ok
  def send_message(actor, ref, message) do
    event = Event.message(ref, message)
    deliver(actor, ref, event)
  end

  @doc """
  Asserts a value to an entity reference.

  Assertions are persistent facts that remain until explicitly retracted.
  Returns a handle that can be used later to retract this specific assertion.

  ## Parameters

  - `actor` - The actor PID or registered name
  - `ref` - The entity reference to assert to
  - `assertion` - The assertion value (Preserves value)

  ## Examples

      ref = %Absynthe.Core.Ref{actor_id: :dataspace, entity_id: 1}
      {:ok, handle} = Absynthe.Core.Actor.assert(actor, ref, {:status, :online})

      # Later, retract it
      :ok = Absynthe.Core.Actor.retract(actor, handle)

  ## Returns

  - `{:ok, handle}` - Assertion made, returns handle for retraction
  """
  @spec assert(GenServer.server(), Ref.t(), term()) :: {:ok, Handle.t()}
  def assert(actor, ref, assertion) do
    GenServer.call(actor, {:assert, ref, assertion})
  end

  @doc """
  Retracts a previously made assertion.

  Removes the assertion identified by the given handle from the system.
  The handle must have been returned by a previous call to `assert/3`.

  ## Parameters

  - `actor` - The actor PID or registered name
  - `handle` - The handle identifying the assertion to retract

  ## Examples

      {:ok, handle} = Absynthe.Core.Actor.assert(actor, ref, value)
      :ok = Absynthe.Core.Actor.retract(actor, handle)

  ## Returns

  - `:ok` - Assertion retracted
  - `{:error, :not_found}` - Handle not found (already retracted?)
  """
  @spec retract(GenServer.server(), Handle.t()) :: :ok | {:error, term()}
  def retract(actor, handle) do
    GenServer.call(actor, {:retract, handle})
  end

  @doc """
  Sends a synchronization event to an entity.

  Synchronization ensures that all events sent before the sync have been
  processed before the sync event itself is delivered.

  ## Parameters

  - `actor` - The actor PID or registered name
  - `ref` - The entity reference to sync with
  - `peer` - The peer reference to notify when sync is complete

  ## Examples

      Absynthe.Core.Actor.sync(actor, target_ref, reply_ref)

  ## Returns

  - `:ok` - Sync event queued
  """
  @spec sync(GenServer.server(), Ref.t(), Ref.t()) :: :ok
  def sync(actor, ref, peer) do
    event = Event.sync(ref, peer)
    deliver(actor, ref, event)
  end

  @doc """
  Creates a new facet as a child of the specified parent facet.

  Facets provide scoped resource management - when a facet terminates,
  all its entities and child facets are cleaned up automatically.

  ## Parameters

  - `actor` - The actor PID or registered name
  - `parent_id` - The ID of the parent facet
  - `facet_id` - The ID for the new facet (must be unique)

  ## Examples

      :ok = Absynthe.Core.Actor.create_facet(actor, :root, :conversation_1)

  ## Returns

  - `:ok` - Facet created successfully
  - `{:error, reason}` - Failed to create facet
  """
  @spec create_facet(GenServer.server(), facet_id(), facet_id()) ::
          :ok | {:error, term()}
  def create_facet(actor, parent_id, facet_id) do
    GenServer.call(actor, {:create_facet, parent_id, facet_id})
  end

  @doc """
  Terminates a facet and all its entities and child facets.

  This implements fate-sharing: when a facet is terminated, everything
  it contains is also terminated and cleaned up.

  ## Parameters

  - `actor` - The actor PID or registered name
  - `facet_id` - The ID of the facet to terminate

  ## Examples

      :ok = Absynthe.Core.Actor.terminate_facet(actor, :conversation_1)

  ## Returns

  - `:ok` - Facet and its contents terminated
  - `{:error, reason}` - Failed to terminate
  """
  @spec terminate_facet(GenServer.server(), facet_id()) :: :ok | {:error, term()}
  def terminate_facet(actor, facet_id) do
    GenServer.call(actor, {:terminate_facet, facet_id})
  end

  # GenServer Callbacks

  @impl GenServer
  def init(opts) do
    # Extract initialization options
    actor_id = Keyword.get(opts, :id, self())
    root_facet_id = Keyword.get(opts, :root_facet_id, :root)

    # Create the root facet
    root_facet = %{
      id: root_facet_id,
      parent_id: nil,
      children: MapSet.new(),
      entities: MapSet.new()
    }

    # Initialize actor state
    state = %{
      id: actor_id,
      next_entity_id: 0,
      next_handle_id: 0,
      root_facet_id: root_facet_id,
      facets: %{root_facet_id => root_facet},
      entities: %{},
      outbound_assertions: %{},
      facet_handles: %{root_facet_id => MapSet.new()},
      # Crash cleanup tracking
      inbound_assertions: %{},
      owner_handles: %{},
      pid_monitors: %{}
    }

    Logger.debug("Actor #{inspect(actor_id)} initialized with root facet #{root_facet_id}")

    {:ok, state}
  end

  @impl GenServer
  def handle_call({:spawn_entity, facet_id, entity}, _from, state) do
    case spawn_entity_impl(state, facet_id, entity) do
      {:ok, ref, new_state} ->
        {:reply, {:ok, ref}, new_state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl GenServer
  def handle_call({:assert, ref, assertion}, _from, state) do
    # Check attenuation BEFORE tracking - reject if attenuation fails
    attenuation = Ref.attenuation(ref)

    case Rewrite.apply_attenuation(assertion, attenuation || []) do
      {:ok, rewritten_assertion} ->
        # Generate a new handle for this assertion (namespaced by actor_id for global uniqueness)
        handle = Handle.new(state.id, state.next_handle_id)
        new_state = %{state | next_handle_id: state.next_handle_id + 1}

        # Use the underlying ref (without attenuation) for delivery since we already applied it
        # This prevents double-checking attenuation at deliver_event_impl
        underlying_ref = Ref.without_attenuation(ref)

        # Track the assertion handle with the root facet
        # (client-initiated assertions don't have a specific facet context)
        # Track with the underlying ref and REWRITTEN assertion for consistency
        new_state =
          track_assertion_handle(
            new_state,
            state.root_facet_id,
            handle,
            underlying_ref,
            rewritten_assertion
          )

        # Create the assert event with rewritten assertion and underlying ref
        event = Event.assert(underlying_ref, rewritten_assertion, handle)

        # Deliver asynchronously to align with documented behavior
        # deliver_to_ref handles both local and remote delivery
        new_state = deliver_to_ref(new_state, underlying_ref, event, :async)

        {:reply, {:ok, handle}, new_state}

      :rejected ->
        {:reply, {:error, :attenuation_rejected}, state}
    end
  end

  @impl GenServer
  def handle_call({:retract, handle}, _from, state) do
    case Map.fetch(state.outbound_assertions, handle) do
      {:ok, {ref, _assertion}} ->
        # Remove from tracking (outbound_assertions and facet_handles)
        new_state = untrack_assertion_handle(state, handle)

        # Create the retract event
        event = Event.retract(ref, handle)

        # Deliver asynchronously to align with documented behavior
        # deliver_to_ref handles both local and remote delivery
        new_state = deliver_to_ref(new_state, ref, event, :async)

        {:reply, :ok, new_state}

      :error ->
        {:reply, {:error, :not_found}, state}
    end
  end

  @impl GenServer
  def handle_call({:create_facet, parent_id, facet_id}, _from, state) do
    case create_facet_impl(state, parent_id, facet_id) do
      {:ok, new_state} ->
        {:reply, :ok, new_state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl GenServer
  def handle_call({:terminate_facet, facet_id}, _from, state) do
    case terminate_facet_impl(state, facet_id) do
      {:ok, new_state} ->
        {:reply, :ok, new_state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl GenServer
  def handle_cast({:deliver, ref, event}, state) do
    # Route to correct actor - may be local or remote
    new_state = deliver_action_to_ref(state, ref, event)
    {:noreply, new_state}
  end

  @impl GenServer
  def handle_cast({:deliver_from, ref, event, sender_pid}, state) do
    # Remote delivery with sender tracking for crash cleanup
    new_state = deliver_event_impl(state, ref, event, sender_pid)
    {:noreply, new_state}
  end

  # Implementation Helpers

  @doc false
  defp spawn_entity_impl(state, facet_id, entity) do
    # Verify the facet exists
    case Map.fetch(state.facets, facet_id) do
      {:ok, facet} ->
        # Reassign ownership of any fields in the entity to this actor process.
        # This ensures cleanup works correctly when the actor terminates,
        # even if the entity was created in a different process.
        Absynthe.Dataflow.Field.reassign_all_owners(entity, self())

        # Generate new entity ID
        entity_id = state.next_entity_id

        # Create the entity reference
        ref = Ref.new(state.id, entity_id)

        # Update facet to include this entity
        updated_facet = %{facet | entities: MapSet.put(facet.entities, entity_id)}

        # Update state
        new_state = %{
          state
          | next_entity_id: entity_id + 1,
            entities: Map.put(state.entities, entity_id, {facet_id, entity}),
            facets: Map.put(state.facets, facet_id, updated_facet)
        }

        Logger.debug(
          "Spawned entity #{entity_id} in facet #{facet_id} of actor #{inspect(state.id)}"
        )

        {:ok, ref, new_state}

      :error ->
        {:error, :facet_not_found}
    end
  end

  @doc false
  defp create_facet_impl(state, parent_id, facet_id) do
    cond do
      # Facet already exists
      Map.has_key?(state.facets, facet_id) ->
        {:error, :facet_already_exists}

      # Parent doesn't exist
      not Map.has_key?(state.facets, parent_id) ->
        {:error, :parent_facet_not_found}

      # Create the new facet
      true ->
        new_facet = %{
          id: facet_id,
          parent_id: parent_id,
          children: MapSet.new(),
          entities: MapSet.new()
        }

        # Update parent to reference this child
        parent = Map.fetch!(state.facets, parent_id)
        updated_parent = %{parent | children: MapSet.put(parent.children, facet_id)}

        new_state = %{
          state
          | facets:
              state.facets
              |> Map.put(facet_id, new_facet)
              |> Map.put(parent_id, updated_parent),
            facet_handles: Map.put(state.facet_handles, facet_id, MapSet.new())
        }

        Logger.debug("Created facet #{facet_id} under parent #{parent_id}")

        {:ok, new_state}
    end
  end

  @doc false
  defp terminate_facet_impl(state, facet_id) do
    # Can't terminate the root facet
    if facet_id == state.root_facet_id do
      {:error, :cannot_terminate_root_facet}
    else
      case Map.fetch(state.facets, facet_id) do
        {:ok, facet} ->
          # Recursively terminate all child facets
          new_state =
            Enum.reduce(facet.children, state, fn child_id, acc_state ->
              {:ok, updated_state} = terminate_facet_impl(acc_state, child_id)
              updated_state
            end)

          # Retract all assertions owned by this facet
          new_state = retract_facet_handles(new_state, facet_id)

          # Remove all entities in this facet
          new_state =
            Enum.reduce(facet.entities, new_state, fn entity_id, acc_state ->
              %{acc_state | entities: Map.delete(acc_state.entities, entity_id)}
            end)

          # Remove facet from parent's children
          new_state =
            if facet.parent_id do
              parent = Map.fetch!(new_state.facets, facet.parent_id)
              updated_parent = %{parent | children: MapSet.delete(parent.children, facet_id)}
              %{new_state | facets: Map.put(new_state.facets, facet.parent_id, updated_parent)}
            else
              new_state
            end

          # Remove the facet itself and its handle tracking
          new_state = %{
            new_state
            | facets: Map.delete(new_state.facets, facet_id),
              facet_handles: Map.delete(new_state.facet_handles, facet_id)
          }

          Logger.debug("Terminated facet #{facet_id}")

          {:ok, new_state}

        :error ->
          {:error, :facet_not_found}
      end
    end
  end

  # Retract all handles owned by a facet
  @doc false
  defp retract_facet_handles(state, facet_id) do
    handles = Map.get(state.facet_handles, facet_id, MapSet.new())

    Enum.reduce(handles, state, fn handle, acc_state ->
      case Map.fetch(acc_state.outbound_assertions, handle) do
        {:ok, {ref, _assertion}} ->
          # Create and deliver the retract event
          event = Event.retract(ref, handle)
          acc_state = deliver_to_ref(acc_state, ref, event)

          # Remove from outbound assertions
          %{acc_state | outbound_assertions: Map.delete(acc_state.outbound_assertions, handle)}

        :error ->
          # Handle not found, skip (may have been manually retracted)
          acc_state
      end
    end)
  end

  @doc false
  # sender_pid is nil for local deliveries, or the PID of the remote actor for remote deliveries
  defp deliver_event_impl(state, ref, event, sender_pid) do
    entity_id = Ref.entity_id(ref)

    # Apply attenuation to the event (may rewrite or reject)
    case apply_event_attenuation(ref, event) do
      {:ok, rewritten_event} ->
        case Map.fetch(state.entities, entity_id) do
          {:ok, {facet_id, entity}} ->
            # Track inbound assertions from remote actors for crash cleanup
            state = maybe_track_inbound_assertion(state, rewritten_event, ref, sender_pid)

            # Execute a turn for this event
            result = execute_turn(state, facet_id, entity_id, entity, rewritten_event)

            case result do
              {:ok, _updated_entity, new_state} ->
                # Entity state is already updated in execute_turn before action processing
                new_state

              {:error, reason} ->
                Logger.error("Turn failed for entity #{entity_id}: #{inspect(reason)}")
                # On error, state remains unchanged (rollback)
                state
            end

          :error ->
            Logger.warning("Entity #{entity_id} not found for event: #{inspect(event)}")
            state
        end

      :rejected ->
        # Event rejected by attenuation - drop silently
        Logger.debug("Event rejected by attenuation on ref #{inspect(ref)}")
        state
    end
  end

  # Apply attenuation to an event, potentially rewriting the value
  # Returns {:ok, event} (possibly rewritten) or :rejected
  defp apply_event_attenuation(ref, %Assert{assertion: assertion} = event) do
    attenuation = Ref.attenuation(ref)

    case Rewrite.apply_attenuation(assertion, attenuation || []) do
      {:ok, rewritten_assertion} ->
        {:ok, %Assert{event | assertion: rewritten_assertion}}

      :rejected ->
        :rejected
    end
  end

  defp apply_event_attenuation(ref, %Message{body: body} = event) do
    attenuation = Ref.attenuation(ref)

    case Rewrite.apply_attenuation(body, attenuation || []) do
      {:ok, rewritten_body} ->
        {:ok, %Message{event | body: rewritten_body}}

      :rejected ->
        :rejected
    end
  end

  # Retract and Sync events pass through - they don't carry values that need filtering
  defp apply_event_attenuation(_ref, %Retract{} = event), do: {:ok, event}
  defp apply_event_attenuation(_ref, %Sync{} = event), do: {:ok, event}

  @doc false
  defp execute_turn(state, facet_id, entity_id, entity, event) do
    # Create a turn context using the Turn module
    turn = Turn.new(state.id, facet_id)

    try do
      # Dispatch to entity based on event type
      {updated_entity, updated_turn} =
        case event do
          %Assert{assertion: assertion, handle: handle} ->
            Entity.on_publish(entity, assertion, handle, turn)

          %Retract{handle: handle} ->
            Entity.on_retract(entity, handle, turn)

          %Message{body: message} ->
            Entity.on_message(entity, message, turn)

          %Sync{peer: peer} ->
            Entity.on_sync(entity, peer, turn)
        end

      # IMPORTANT: Update entity state BEFORE processing actions.
      # This ensures nested message deliveries see the updated entity state.
      state_with_updated_entity = %{
        state
        | entities: Map.put(state.entities, entity_id, {facet_id, updated_entity})
      }

      # Commit the turn - execute pending actions (may trigger nested turns)
      new_state = commit_turn(state_with_updated_entity, updated_turn)

      {:ok, updated_entity, new_state}
    rescue
      error ->
        Logger.error("Exception during turn: #{inspect(error)}")
        {:error, {:exception, error}}
    end
  end

  @doc false
  defp commit_turn(state, turn) do
    # Commit the turn and get the actions to execute
    {_committed_turn, actions} = Turn.commit(turn)

    Logger.debug("Committing turn with #{length(actions)} pending actions")

    # Get the facet_id from the turn for tracking assertion ownership
    facet_id = Turn.facet_id(turn)

    # Process each pending action
    Enum.reduce(actions, state, fn action, acc_state ->
      process_action(acc_state, action, facet_id)
    end)
  end

  @doc false
  defp process_action(state, {:field_update, field_id, new_value}, _facet_id) do
    # Execute dataflow field updates
    Absynthe.Dataflow.Field.execute_field_update(field_id, new_value)
    state
  end

  # Handle Assert actions - deliver to target entity
  # Track the assertion if it was created by this actor (i.e., handle not already tracked).
  # Notification assertions (from dataspace to observers) reuse the original handle
  # which won't be in our outbound_assertions, so we detect and track new assertions.
  defp process_action(
         state,
         %Assert{ref: ref, handle: handle, assertion: assertion} = event,
         facet_id
       ) do
    # Check if this handle is already tracked - if not, this is a new assertion
    # that should be tracked for cleanup when the facet terminates
    state =
      if not Map.has_key?(state.outbound_assertions, handle) do
        # This is a new assertion created during this turn - track it
        state
        |> track_assertion_handle(facet_id, handle, ref, assertion)
      else
        state
      end

    deliver_action_to_ref(state, ref, event)
  end

  # Handle Retract actions - deliver to target entity
  # Remove from tracking if this was a turn-initiated retraction of our own assertion
  defp process_action(state, %Retract{ref: ref, handle: handle} = event, _facet_id) do
    # Remove from outbound assertions and facet handles if we own this handle
    state = untrack_assertion_handle(state, handle)
    deliver_action_to_ref(state, ref, event)
  end

  # Handle Message actions - deliver to target entity
  defp process_action(state, %Message{ref: ref} = event, _facet_id) do
    deliver_action_to_ref(state, ref, event)
  end

  # Handle Sync actions - deliver to target entity
  defp process_action(state, %Sync{ref: ref} = event, _facet_id) do
    deliver_action_to_ref(state, ref, event)
  end

  # Handle Spawn actions - spawn entity in the specified facet
  # Spawn failures raise an error to abort the entire turn, maintaining atomicity
  defp process_action(
         state,
         %Event.Spawn{facet_id: facet_id, entity: entity, callback: callback},
         _turn_facet_id
       ) do
    case spawn_entity_impl(state, facet_id, entity) do
      {:ok, ref, new_state} ->
        # Invoke callback if provided, with error handling
        # Callback errors are logged but don't prevent the spawn from completing
        if callback do
          try do
            callback.(ref)
          rescue
            error ->
              Logger.error(
                "Spawn callback raised error: #{inspect(error)}\n#{Exception.format_stacktrace()}"
              )
          end
        end

        new_state

      {:error, reason} ->
        # Spawn failure aborts the turn to maintain atomicity
        # The try/rescue in deliver_event_impl will catch this and log it
        raise "Failed to spawn entity in facet #{facet_id}: #{inspect(reason)}"
    end
  end

  defp process_action(state, action, _facet_id) do
    # Unknown action type - log warning
    Logger.warning("Unknown action type: #{inspect(action)}")
    state
  end

  # Track an assertion handle as belonging to a facet
  defp track_assertion_handle(state, facet_id, handle, ref, assertion) do
    # Add to outbound assertions
    state = put_in(state.outbound_assertions[handle], {ref, assertion})

    # Add to facet's handles (create the set if it doesn't exist)
    facet_handles = Map.get(state.facet_handles, facet_id, MapSet.new())
    facet_handles = MapSet.put(facet_handles, handle)
    %{state | facet_handles: Map.put(state.facet_handles, facet_id, facet_handles)}
  end

  # Remove an assertion handle from tracking
  defp untrack_assertion_handle(state, handle) do
    case Map.fetch(state.outbound_assertions, handle) do
      {:ok, _} ->
        # Remove from outbound assertions
        state = %{state | outbound_assertions: Map.delete(state.outbound_assertions, handle)}

        # Remove from facet handles (check all facets since we don't know which one owns it)
        facet_handles =
          Map.new(state.facet_handles, fn {fid, handles} ->
            {fid, MapSet.delete(handles, handle)}
          end)

        %{state | facet_handles: facet_handles}

      :error ->
        # Not our handle, nothing to do
        state
    end
  end

  # Deliver an event to a target ref (local or remote)
  # Used for internal operations like facet teardown that require synchronous local delivery
  defp deliver_to_ref(state, ref, event) do
    deliver_to_ref(state, ref, event, :sync)
  end

  # Deliver an event to a target ref with specified mode
  # - mode: :sync for synchronous local delivery (teardown), :async for async (client ops)
  defp deliver_to_ref(state, ref, event, mode) do
    case Ref.local?(ref, state.id) do
      true ->
        case mode do
          :sync ->
            # Synchronous local delivery for internal operations (facet teardown)
            # nil sender_pid - we own these assertions, no crash tracking needed
            deliver_event_impl(state, ref, event, nil)

          :async ->
            # Async local delivery for client-initiated operations
            GenServer.cast(self(), {:deliver, ref, event})
            state
        end

      false ->
        # Remote delivery - always async via cast, include sender PID for crash tracking
        remote_actor_id = Ref.actor_id(ref)

        case resolve_actor(remote_actor_id) do
          {:ok, actor_pid} ->
            # Use :deliver_from to include sender PID for crash cleanup
            GenServer.cast(actor_pid, {:deliver_from, ref, event, self()})
            state

          :error ->
            Logger.warning(
              "Cannot deliver to remote actor #{inspect(remote_actor_id)}: not found"
            )

            state
        end
    end
  end

  # Deliver an action/event to a target ref (used during turn commit)
  defp deliver_action_to_ref(state, ref, event) do
    case Ref.local?(ref, state.id) do
      true ->
        # Local delivery - process directly
        # nil sender_pid - we own these assertions, no crash tracking needed
        deliver_event_impl(state, ref, event, nil)

      false ->
        # Remote delivery - send to remote actor with sender PID for crash tracking
        remote_actor_id = Ref.actor_id(ref)

        case resolve_actor(remote_actor_id) do
          {:ok, actor_pid} ->
            # Use :deliver_from to include sender PID for crash cleanup
            GenServer.cast(actor_pid, {:deliver_from, ref, event, self()})
            state

          :error ->
            Logger.warning(
              "Cannot deliver to remote actor #{inspect(remote_actor_id)}: not found"
            )

            state
        end
    end
  end

  # Track inbound assertions from remote actors for crash cleanup
  # Only tracks assertions (not retractions or other events), and only from remote actors
  defp maybe_track_inbound_assertion(
         state,
         %Assert{handle: handle, assertion: assertion},
         ref,
         sender_pid
       )
       when is_pid(sender_pid) do
    # Monitor the sender if not already monitored
    state =
      if Map.has_key?(state.pid_monitors, sender_pid) do
        state
      else
        monitor_ref = Process.monitor(sender_pid)
        %{state | pid_monitors: Map.put(state.pid_monitors, sender_pid, monitor_ref)}
      end

    # Track the inbound assertion
    state = %{
      state
      | inbound_assertions:
          Map.put(state.inbound_assertions, handle, {ref, assertion, sender_pid})
    }

    # Track the handle for this owner
    owner_handles = Map.get(state.owner_handles, sender_pid, MapSet.new())
    owner_handles = MapSet.put(owner_handles, handle)
    %{state | owner_handles: Map.put(state.owner_handles, sender_pid, owner_handles)}
  end

  # Track inbound retractions from remote actors - remove from tracking
  defp maybe_track_inbound_assertion(state, %Retract{handle: handle}, _ref, sender_pid)
       when is_pid(sender_pid) do
    case Map.get(state.inbound_assertions, handle) do
      nil ->
        # Not tracking this handle, nothing to do
        state

      {_ref, _assertion, owner_pid} ->
        # Remove from inbound_assertions
        state = %{state | inbound_assertions: Map.delete(state.inbound_assertions, handle)}

        # Remove from owner_handles
        owner_handles = Map.get(state.owner_handles, owner_pid, MapSet.new())
        owner_handles = MapSet.delete(owner_handles, handle)

        if MapSet.size(owner_handles) == 0 do
          # No more handles from this owner - demonitor and stop tracking
          case Map.get(state.pid_monitors, owner_pid) do
            nil ->
              :ok

            monitor_ref ->
              Process.demonitor(monitor_ref, [:flush])
          end

          state
          |> Map.put(:owner_handles, Map.delete(state.owner_handles, owner_pid))
          |> Map.put(:pid_monitors, Map.delete(state.pid_monitors, owner_pid))
        else
          %{state | owner_handles: Map.put(state.owner_handles, owner_pid, owner_handles)}
        end
    end
  end

  # For local deliveries (nil sender_pid) or other event types, no tracking needed
  defp maybe_track_inbound_assertion(state, _event, _ref, _sender_pid), do: state

  # Handle a crashed owner - retract all their assertions
  defp handle_owner_crash(state, owner_pid) do
    handles = Map.get(state.owner_handles, owner_pid, MapSet.new())

    Logger.debug(
      "Actor #{inspect(state.id)}: Owner #{inspect(owner_pid)} crashed, retracting #{MapSet.size(handles)} assertions"
    )

    # For each handle, deliver a retraction event to the entity that holds the assertion
    state =
      Enum.reduce(handles, state, fn handle, acc_state ->
        case Map.get(acc_state.inbound_assertions, handle) do
          nil ->
            # Already retracted, skip
            acc_state

          {ref, _assertion, _owner_pid} ->
            # Deliver retraction to the entity (local delivery, synchronous)
            event = Event.retract(ref, handle)
            deliver_event_impl(acc_state, ref, event, nil)
        end
      end)

    # Clean up tracking state
    state = %{state | owner_handles: Map.delete(state.owner_handles, owner_pid)}
    state = %{state | pid_monitors: Map.delete(state.pid_monitors, owner_pid)}

    # Clean up inbound_assertions for this owner
    inbound_assertions =
      Enum.reduce(handles, state.inbound_assertions, fn handle, acc ->
        Map.delete(acc, handle)
      end)

    %{state | inbound_assertions: inbound_assertions}
  end

  # Resolve an actor ID to a PID
  defp resolve_actor(actor_id) when is_pid(actor_id) do
    if Process.alive?(actor_id), do: {:ok, actor_id}, else: :error
  end

  defp resolve_actor(actor_id) when is_atom(actor_id) do
    case Process.whereis(actor_id) do
      nil -> :error
      pid -> {:ok, pid}
    end
  end

  defp resolve_actor(_), do: :error

  # Handle monitored process DOWN - auto-retract crashed actor's assertions
  @impl GenServer
  def handle_info({:DOWN, _ref, :process, pid, _reason}, state) do
    new_state = handle_owner_crash(state, pid)
    {:noreply, new_state}
  end

  # Cleanup on termination
  @impl GenServer
  def terminate(reason, state) do
    Logger.debug("Actor #{inspect(state.id)} terminating: #{inspect(reason)}")

    # Retract all outbound assertions
    # Note: We iterate all assertions and send retraction events.
    # For local entities, these may not be processed if the actor is shutting down,
    # but for remote entities/dataspaces, this ensures cleanup.
    Enum.each(state.outbound_assertions, fn {handle, {ref, _assertion}} ->
      event = Event.retract(ref, handle)

      # Only send to remote actors - local delivery won't work during termination
      if not Ref.local?(ref, state.id) do
        remote_actor_id = Ref.actor_id(ref)

        case resolve_actor(remote_actor_id) do
          {:ok, actor_pid} ->
            # Use :deliver_from so receiver can untrack the assertion before DOWN arrives
            GenServer.cast(actor_pid, {:deliver_from, ref, event, self()})

          :error ->
            :ok
        end
      end
    end)

    # Clean up dataflow fields owned by this process
    Absynthe.Dataflow.Field.cleanup()

    :ok
  end
end
