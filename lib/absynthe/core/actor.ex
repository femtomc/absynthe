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
          outbound_assertions: %{Handle.t() => {Ref.t(), term()}}
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
      outbound_assertions: %{}
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
    # Generate a new handle for this assertion
    handle = Handle.new(state.next_handle_id)
    new_state = %{state | next_handle_id: state.next_handle_id + 1}

    # Store the outbound assertion for potential retraction
    new_state = put_in(new_state.outbound_assertions[handle], {ref, assertion})

    # Create and deliver the assert event
    event = Event.assert(ref, assertion, handle)

    # If local, deliver immediately; otherwise would send to remote actor
    case Ref.local?(ref, state.id) do
      true ->
        # Deliver locally
        new_state = deliver_event_impl(new_state, ref, event)
        {:reply, {:ok, handle}, new_state}

      false ->
        # Remote delivery not yet implemented
        Logger.warning("Remote assertion not yet implemented: #{inspect(ref)}")
        {:reply, {:ok, handle}, new_state}
    end
  end

  @impl GenServer
  def handle_call({:retract, handle}, _from, state) do
    case Map.fetch(state.outbound_assertions, handle) do
      {:ok, {ref, _assertion}} ->
        # Remove from outbound assertions
        new_state = %{state | outbound_assertions: Map.delete(state.outbound_assertions, handle)}

        # Create and deliver the retract event
        event = Event.retract(ref, handle)

        case Ref.local?(ref, state.id) do
          true ->
            new_state = deliver_event_impl(new_state, ref, event)
            {:reply, :ok, new_state}

          false ->
            Logger.warning("Remote retraction not yet implemented: #{inspect(ref)}")
            {:reply, :ok, new_state}
        end

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
    new_state = deliver_event_impl(state, ref, event)
    {:noreply, new_state}
  end

  # Implementation Helpers

  @doc false
  defp spawn_entity_impl(state, facet_id, entity) do
    # Verify the facet exists
    case Map.fetch(state.facets, facet_id) do
      {:ok, facet} ->
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
              |> Map.put(parent_id, updated_parent)
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

          # Remove the facet itself
          new_state = %{new_state | facets: Map.delete(new_state.facets, facet_id)}

          Logger.debug("Terminated facet #{facet_id}")

          {:ok, new_state}

        :error ->
          {:error, :facet_not_found}
      end
    end
  end

  @doc false
  defp deliver_event_impl(state, ref, event) do
    entity_id = Ref.entity_id(ref)

    case Map.fetch(state.entities, entity_id) do
      {:ok, {facet_id, entity}} ->
        # Execute a turn for this event
        result = execute_turn(state, facet_id, entity_id, entity, event)

        case result do
          {:ok, updated_entity, new_state} ->
            # Update the entity in state
            %{new_state | entities: Map.put(new_state.entities, entity_id, {facet_id, updated_entity})}

          {:error, reason} ->
            Logger.error("Turn failed for entity #{entity_id}: #{inspect(reason)}")
            # On error, state remains unchanged (rollback)
            state
        end

      :error ->
        Logger.warning("Entity #{entity_id} not found for event: #{inspect(event)}")
        state
    end
  end

  @doc false
  defp execute_turn(state, facet_id, _entity_id, entity, event) do
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

      # Commit the turn - execute pending actions
      new_state = commit_turn(state, updated_turn)

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

    # Process each pending action
    # TODO: Implement full action processing for spawn, assert, retract, message, sync
    Enum.reduce(actions, state, fn action, acc_state ->
      process_action(acc_state, action)
    end)
  end

  @doc false
  defp process_action(state, action) do
    # Stub for processing individual actions
    # Will be implemented when Turn and Action types are defined
    Logger.debug("Processing action: #{inspect(action)}")
    state
  end
end
