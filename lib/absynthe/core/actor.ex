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

  Event delivery follows the syndicate-rs pattern: all delivery is asynchronous with
  a liveness check at delivery time. This design provides:

  - **Correct ordering**: Events are queued via self-cast (local) or GenServer.cast
    (remote), ensuring FIFO ordering via the actor's mailbox
  - **Liveness check**: Assert events are dropped if their handle was already retracted
    (e.g., because the owning facet terminated). This prevents stale assertions from
    arriving after their corresponding retractions.
  - **Flow control**: All delivery paths apply borrow/repay accounting

  Note: The client API functions `assert/4`, `retract/3`, and `update_assertion/5`
  use `GenServer.call` to synchronously validate attenuation and track handles,
  but the actual event delivery to entities is asynchronous with liveness checks.
  Functions `send_message/3` and `sync/3` are fully asynchronous.

  **Important**: Local turn-initiated actions are processed in subsequent turns,
  not synchronously during commit. This keeps call stacks shallow and prevents
  observer fan-out from monopolizing the actor, but means observers see effects
  in subsequent turns rather than atomically within the initiating turn.

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
        # on_sync uses default: sends {:symbol, "synced"} to peer
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
  alias Absynthe.FlowControl
  alias Absynthe.FlowControl.Account

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
          pid_monitors: %{pid() => reference()},
          # Per-actor flow control account for internal cascades
          # When present, each action enqueued during turn commit borrows from this account
          flow_control_account: Account.t() | nil
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
          entities: MapSet.t(entity_id()),
          # Inertness tracking
          inert_check_preventers: non_neg_integer(),
          linked_tasks: MapSet.t(reference())
        }

  # Client API

  @doc """
  Starts a new actor process.

  ## Options

  - `:id` - The actor's unique identifier (defaults to self())
  - `:name` - Optional registered name for the GenServer
  - `:root_facet_id` - ID for the root facet (defaults to :root)
  - `:flow_control` - Per-actor flow control instrumentation (see Flow Control below)

  ## Flow Control

  Flow control provides instrumentation for monitoring internal event cascades.
  When enabled, each action (assert, retract, message, etc.) incurs a "debt" that
  is repaid when the event is processed.

  **Note**: Flow control tracks debt and emits telemetry but does not automatically
  throttle or block event processing. To implement actual backpressure, use the
  `pause_callback` and `resume_callback` options to integrate with your own
  throttling mechanism (e.g., pausing socket reads in relays).

  Options:

  - `false` or omitted - No flow control (default)
  - `true` - Enable with default settings (limit: 1000)
  - `[limit: n, ...]` - Custom configuration passed to `Absynthe.FlowControl.Account.new/1`

  When debt exceeds the high water mark, the account enters a "paused" state and
  emits telemetry events on `[:absynthe, :flow_control, :account, :pause]` and
  `[:absynthe, :flow_control, :actor, :overload]`. When debt falls below the low
  water mark, a resume event is emitted on `[:absynthe, :flow_control, :account, :resume]`.
  Use `flow_control_stats/1` to inspect the current state.

  ## Examples

      # Start an anonymous actor
      {:ok, actor} = Absynthe.Core.Actor.start_link(id: :my_actor)

      # Start a named actor
      {:ok, actor} = Absynthe.Core.Actor.start_link(
        id: :my_actor,
        name: MyApp.MyActor
      )

      # Start with default flow control
      {:ok, actor} = Absynthe.Core.Actor.start_link(
        id: :my_actor,
        flow_control: true
      )

      # Start with custom flow control settings
      {:ok, actor} = Absynthe.Core.Actor.start_link(
        id: :my_actor,
        flow_control: [limit: 500, high_water_mark: 400, low_water_mark: 100]
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
  Delivers an event with flow control acknowledgment.

  This variant is used by relays to enable ack-based flow control. After the
  turn completes (event is fully processed), the actor will send a
  `{:flow_control_loan_ack, loan_id}` message back to the relay_pid.

  ## Parameters

  - `actor` - The actor PID or registered name
  - `ref` - The entity reference
  - `event` - The event to deliver
  - `relay_pid` - The relay process to send the ack to
  - `loan_id` - The loan ID to include in the ack

  ## Returns

  - `:ok` - Event queued for delivery
  """
  @spec deliver_with_flow_control(
          GenServer.server(),
          Ref.t(),
          Event.t(),
          pid(),
          non_neg_integer()
        ) :: :ok
  def deliver_with_flow_control(actor, ref, event, relay_pid, loan_id) do
    GenServer.cast(actor, {:deliver_with_flow_control, ref, event, relay_pid, loan_id})
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

  This function blocks the caller to validate attenuation and generate a handle,
  but the actual event delivery to the target entity is asynchronous.

  ## Parameters

  - `actor` - The actor PID or registered name
  - `ref` - The entity reference to assert to
  - `assertion` - The assertion value (Preserves value)

  ## Examples

      ref = %Absynthe.Core.Ref{actor_id: :dataspace, entity_id: 1}
      {:ok, handle} = Absynthe.Core.Actor.assert(actor, ref, {:status, :online})

      # Later, retract it
      :ok = Absynthe.Core.Actor.retract(actor, handle)

  ## Options

  - `:facet_id` - The facet to associate this assertion with (default: root facet).
    When the specified facet terminates, the assertion is automatically retracted.
    This enables fate-sharing between facets and their assertions.

  ## Returns

  - `{:ok, handle}` - Handle allocated and delivery enqueued
  - `{:error, :attenuation_rejected}` - Assertion blocked by ref attenuation
  - `{:error, :facet_not_found}` - Specified facet_id does not exist

  ## Delivery Semantics

  All delivery is asynchronous, following the syndicate-rs pattern. For local refs,
  events are enqueued via self-cast to the actor's mailbox. For remote refs, events
  are sent via GenServer.cast. This ensures shallow call stacks and fair scheduling
  during observer fan-out. Ordering is preserved via mailbox FIFO semantics.
  """
  @spec assert(GenServer.server(), Ref.t(), term(), keyword()) ::
          {:ok, Handle.t()} | {:error, term()}
  def assert(actor, ref, assertion, opts \\ []) do
    facet_id = Keyword.get(opts, :facet_id)
    GenServer.call(actor, {:assert, ref, assertion, facet_id})
  end

  @doc """
  Retracts a previously made assertion.

  Removes the assertion identified by the given handle from the system.
  The handle must have been returned by a previous call to `assert/3`.

  This function blocks the caller to validate and untrack the handle,
  but the actual retraction event delivery to the target entity is asynchronous.

  ## Parameters

  - `actor` - The actor PID or registered name
  - `handle` - The handle identifying the assertion to retract

  ## Examples

      {:ok, handle} = Absynthe.Core.Actor.assert(actor, ref, value)
      :ok = Absynthe.Core.Actor.retract(actor, handle)

  ## Returns

  - `:ok` - Handle untracked and retraction enqueued
  - `{:error, :not_found}` - Handle not found (already retracted?)
  """
  @spec retract(GenServer.server(), Handle.t()) :: :ok | {:error, term()}
  def retract(actor, handle) do
    GenServer.call(actor, {:retract, handle})
  end

  @doc """
  Atomically updates an assertion by retracting the old value and asserting a new one.

  This is the key operation for maintaining consistent state in the Syndicated Actor Model.
  Instead of manually tracking handles and doing separate retract/assert operations,
  `update_assertion/5` provides atomic state-change notification (SCN) semantics.

  When the old handle is `nil` or no new assertion is provided, this becomes a simple
  assert or retract operation respectively.

  This function blocks the caller to validate attenuation and manage handles atomically,
  but the actual event delivery to target entities is asynchronous (see `assert/4` for details).

  ## Parameters

  - `actor` - The actor PID or registered name
  - `handle` - The current handle to retract (or `nil` if this is a new assertion)
  - `ref` - The entity reference to assert to
  - `assertion` - The new assertion value (or `nil` to just retract)
  - `opts` - Keyword options (see Options below)

  ## Options

  - `:facet_id` - The facet to associate the new assertion with (default: root facet).
    When the specified facet terminates, the assertion is automatically retracted.

  ## Examples

      # Initial assertion (no previous handle)
      {:ok, handle} = Actor.update_assertion(actor, nil, ref, {:status, :online})

      # Update to new value (atomically retracts old, asserts new)
      {:ok, new_handle} = Actor.update_assertion(actor, handle, ref, {:status, :busy})

      # Retract completely (no new assertion)
      :ok = Actor.update_assertion(actor, handle, ref, nil)

      # Update with facet ownership
      {:ok, handle} = Actor.update_assertion(actor, nil, ref, value, facet_id: :my_facet)

  ## Returns

  - `{:ok, handle}` - New handle allocated and delivery enqueued
  - `:ok` - Only retraction performed (no new assertion)
  - `{:error, :attenuation_rejected}` - New assertion rejected by ref attenuation
  - `{:error, :not_found}` - Old handle not found (already retracted?)
  - `{:error, :facet_not_found}` - Specified facet_id does not exist

  ## Atomicity

  The retraction and assertion happen in a single GenServer call, ensuring that
  observers see the state change as a single coherent update rather than
  separate remove/add events that could expose intermediate states.
  """
  @spec update_assertion(GenServer.server(), Handle.t() | nil, Ref.t(), term() | nil, keyword()) ::
          {:ok, Handle.t()} | :ok | {:error, term()}
  def update_assertion(actor, handle, ref, assertion, opts \\ []) do
    facet_id = Keyword.get(opts, :facet_id)
    GenServer.call(actor, {:update_assertion, handle, ref, assertion, facet_id})
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
  Returns flow control statistics for the actor.

  If flow control is not enabled for this actor, returns `nil`.

  ## Parameters

  - `actor` - The actor PID or registered name

  ## Examples

      stats = Absynthe.Core.Actor.flow_control_stats(actor)
      # %{debt: 50, limit: 1000, paused: false, utilization: 5.0, ...}

  ## Returns

  - `map()` - Flow control statistics
  - `nil` - Flow control not enabled
  """
  @spec flow_control_stats(GenServer.server()) :: map() | nil
  def flow_control_stats(actor) do
    GenServer.call(actor, :flow_control_stats)
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

  @doc """
  Prevents inertness checks from auto-terminating a facet.

  This is used during facet setup to prevent premature garbage collection
  while the facet is still being initialized (e.g., while wiring up assertions
  or spawning entities).

  Returns a token that must be passed to `allow_inert_check/3` to re-enable
  inertness checking.

  ## Parameters

  - `actor` - The actor PID or registered name
  - `facet_id` - The ID of the facet to prevent inertness checks for

  ## Examples

      {:ok, token} = Absynthe.Core.Actor.prevent_inert_check(actor, :my_facet)
      # ... do setup work ...
      :ok = Absynthe.Core.Actor.allow_inert_check(actor, :my_facet, token)

  ## Returns

  - `{:ok, token}` - Token for later allowing inert checks
  - `{:error, :facet_not_found}` - Facet doesn't exist
  """
  @spec prevent_inert_check(GenServer.server(), facet_id()) ::
          {:ok, reference()} | {:error, term()}
  def prevent_inert_check(actor, facet_id) do
    GenServer.call(actor, {:prevent_inert_check, facet_id})
  end

  @doc """
  Re-enables inertness checks for a facet after `prevent_inert_check/2`.

  This decrements the facet's preventer counter. When the counter reaches zero,
  the facet becomes eligible for automatic inertness termination.

  Note: The token parameter is required for API symmetry with `prevent_inert_check/2`
  but is not validated. Callers are responsible for ensuring balanced
  prevent/allow calls.

  ## Parameters

  - `actor` - The actor PID or registered name
  - `facet_id` - The ID of the facet to allow inertness checks for
  - `token` - The token returned from `prevent_inert_check/2` (kept for API symmetry)

  ## Examples

      {:ok, token} = Absynthe.Core.Actor.prevent_inert_check(actor, :my_facet)
      :ok = Absynthe.Core.Actor.allow_inert_check(actor, :my_facet, token)

  ## Returns

  - `:ok` - Inertness checks re-enabled
  - `{:error, :facet_not_found}` - Facet doesn't exist
  - `{:error, :no_preventers}` - No preventers active (unbalanced calls)
  """
  @spec allow_inert_check(GenServer.server(), facet_id(), reference()) ::
          :ok | {:error, term()}
  def allow_inert_check(actor, facet_id, token) do
    GenServer.call(actor, {:allow_inert_check, facet_id, token})
  end

  # GenServer Callbacks

  @impl GenServer
  def init(opts) do
    # Extract initialization options
    actor_id = Keyword.get(opts, :id, self())
    root_facet_id = Keyword.get(opts, :root_facet_id, :root)

    # Create flow control account if enabled
    # Options:
    # - flow_control: true - use default flow control settings
    # - flow_control: [limit: 500, ...] - custom account options
    # - flow_control: false or omitted - no internal flow control
    flow_control_account =
      case Keyword.get(opts, :flow_control) do
        nil ->
          nil

        false ->
          nil

        true ->
          Account.new(id: {:actor, actor_id})

        account_opts when is_list(account_opts) ->
          Account.new([{:id, {:actor, actor_id}} | account_opts])
      end

    # Create the root facet
    # Note: root facet starts with inert_check_preventers=1 to prevent premature termination
    # The root facet is special and should not be auto-terminated by inertness
    root_facet = %{
      id: root_facet_id,
      parent_id: nil,
      children: MapSet.new(),
      entities: MapSet.new(),
      # Inertness tracking
      inert_check_preventers: 1,
      linked_tasks: MapSet.new()
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
      pid_monitors: %{},
      # Per-actor flow control for internal cascades
      flow_control_account: flow_control_account
    }

    Logger.debug("Actor #{inspect(actor_id)} initialized with root facet #{root_facet_id}")

    {:ok, state}
  end

  @impl GenServer
  def handle_call(:flow_control_stats, _from, state) do
    stats =
      case state.flow_control_account do
        nil ->
          nil

        account ->
          {:message_queue_len, mailbox_len} = Process.info(self(), :message_queue_len)

          Account.stats(account)
          |> Map.put(:mailbox_length, mailbox_len)
      end

    {:reply, stats, state}
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
  def handle_call({:assert, ref, assertion, facet_id}, _from, state) do
    # Determine which facet to track this assertion with
    # If facet_id is nil, use root_facet_id (backwards compatible behavior)
    # If facet_id is provided, verify it exists
    # Note: Use explicit nil check to handle falsy facet_ids like false or 0 correctly
    target_facet_id = if facet_id == nil, do: state.root_facet_id, else: facet_id

    # Validate the target facet exists (unless it's the root facet)
    if facet_id != nil and not Map.has_key?(state.facets, target_facet_id) do
      {:reply, {:error, :facet_not_found}, state}
    else
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

          # Track the assertion handle with the specified facet
          # This enables fate-sharing: when the facet terminates, this assertion is retracted
          # Track with the underlying ref and REWRITTEN assertion for consistency
          new_state =
            track_assertion_handle(
              new_state,
              target_facet_id,
              handle,
              underlying_ref,
              rewritten_assertion
            )

          # Create the assert event with rewritten assertion and underlying ref
          event = Event.assert(underlying_ref, rewritten_assertion, handle)

          # Deliver asynchronously. Ordering is preserved because both asserts and retracts
          # use the same mailbox (via self-cast for local, cast for remote). A liveness
          # check at delivery time drops Assert events if the handle was already retracted.
          # This follows the syndicate-rs pattern: async delivery + liveness checks.
          new_state = deliver_to_ref(new_state, underlying_ref, event, :async)

          {:reply, {:ok, handle}, new_state}

        :rejected ->
          {:reply, {:error, :attenuation_rejected}, state}
      end
    end
  end

  @impl GenServer
  def handle_call({:retract, handle}, _from, state) do
    case Map.fetch(state.outbound_assertions, handle) do
      {:ok, {ref, _assertion}} ->
        # Remove from tracking (outbound_assertions and facet_handles)
        {new_state, owning_facet_id} = untrack_assertion_handle(state, handle)

        # Create the retract event
        event = Event.retract(ref, handle)

        # Deliver asynchronously to align with documented behavior
        # deliver_to_ref handles both local and remote delivery
        new_state = deliver_to_ref(new_state, ref, event, :async)

        # Check if the owning facet became inert after losing this handle
        new_state =
          if owning_facet_id do
            stop_inert_facets(new_state, owning_facet_id)
          else
            new_state
          end

        {:reply, :ok, new_state}

      :error ->
        {:reply, {:error, :not_found}, state}
    end
  end

  @impl GenServer
  def handle_call({:update_assertion, old_handle, ref, new_assertion, facet_id}, _from, state) do
    # Atomic update: retract old (if present) then assert new (if present)
    # This provides SCN-style state-change semantics
    #
    # IMPORTANT: We must check attenuation BEFORE retracting to ensure atomicity.
    # If the new assertion would be rejected, the old assertion should remain intact.

    # Determine which facet to track the new assertion with
    # Note: Use explicit nil check to handle falsy facet_ids like false or 0 correctly
    target_facet_id = if facet_id == nil, do: state.root_facet_id, else: facet_id

    # Validate the target facet exists (if specified)
    if facet_id != nil and not Map.has_key?(state.facets, target_facet_id) do
      {:reply, {:error, :facet_not_found}, state}
    else
      # Step 1: Validate the old handle exists (if provided)
      old_handle_info =
        case old_handle do
          nil ->
            {:ok, nil}

          handle ->
            case Map.fetch(state.outbound_assertions, handle) do
              {:ok, {old_ref, _assertion}} -> {:ok, {handle, old_ref}}
              :error -> {:error, :not_found}
            end
        end

      # Step 2: Check attenuation on the new assertion BEFORE modifying state
      # This ensures we don't retract the old assertion if the new one would be rejected
      attenuation_result =
        case new_assertion do
          nil ->
            {:ok, nil}

          assertion ->
            attenuation = Ref.attenuation(ref)

            case Rewrite.apply_attenuation(assertion, attenuation || []) do
              {:ok, rewritten} -> {:ok, rewritten}
              :rejected -> {:error, :attenuation_rejected}
            end
        end

      # Step 3: Now apply the operations only if all validations passed
      case {old_handle_info, attenuation_result} do
        {{:error, reason}, _} ->
          # Old handle not found
          {:reply, {:error, reason}, state}

        {_, {:error, reason}} ->
          # Attenuation rejected - do NOT retract the old assertion
          {:reply, {:error, reason}, state}

        {{:ok, old_info}, {:ok, rewritten_assertion}} ->
          # All validations passed - now perform the atomic update

          # Step 3a: Retract old assertion (if handle provided)
          # Deliver asynchronously - ordering is preserved via mailbox FIFO
          {state, owning_facet_id} =
            case old_info do
              nil ->
                {state, nil}

              {handle, old_ref} ->
                {new_state, owner_fid} = untrack_assertion_handle(state, handle)
                event = Event.retract(old_ref, handle)
                {deliver_to_ref(new_state, old_ref, event, :async), owner_fid}
            end

          # Step 3b: Assert new value (if provided)
          case rewritten_assertion do
            nil ->
              # Just retraction, no new assertion
              # Check if the owning facet became inert after losing this handle
              state =
                if owning_facet_id do
                  stop_inert_facets(state, owning_facet_id)
                else
                  state
                end

              {:reply, :ok, state}

            rewritten ->
              # Generate new handle
              new_handle = Handle.new(state.id, state.next_handle_id)
              state = %{state | next_handle_id: state.next_handle_id + 1}

              # Use underlying ref for delivery
              underlying_ref = Ref.without_attenuation(ref)

              # Track the new assertion with the specified facet
              state =
                track_assertion_handle(
                  state,
                  target_facet_id,
                  new_handle,
                  underlying_ref,
                  rewritten
                )

              # Create and deliver the assert event
              # Deliver asynchronously - liveness check at delivery time handles races
              event = Event.assert(underlying_ref, rewritten, new_handle)
              state = deliver_to_ref(state, underlying_ref, event, :async)

              {:reply, {:ok, new_handle}, state}
          end
      end
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
  def handle_call({:prevent_inert_check, facet_id}, _from, state) do
    case Map.fetch(state.facets, facet_id) do
      {:ok, facet} ->
        token = make_ref()
        updated_facet = %{facet | inert_check_preventers: facet.inert_check_preventers + 1}
        new_state = %{state | facets: Map.put(state.facets, facet_id, updated_facet)}
        {:reply, {:ok, token}, new_state}

      :error ->
        {:reply, {:error, :facet_not_found}, state}
    end
  end

  @impl GenServer
  def handle_call({:allow_inert_check, facet_id, _token}, _from, state) do
    case Map.fetch(state.facets, facet_id) do
      {:ok, facet} ->
        if facet.inert_check_preventers > 0 do
          updated_facet = %{facet | inert_check_preventers: facet.inert_check_preventers - 1}
          new_state = %{state | facets: Map.put(state.facets, facet_id, updated_facet)}

          # After allowing inert check, check if the facet is now inert
          new_state = stop_inert_facets(new_state, facet_id)

          {:reply, :ok, new_state}
        else
          {:reply, {:error, :no_preventers}, state}
        end

      :error ->
        {:reply, {:error, :facet_not_found}, state}
    end
  end

  @impl GenServer
  def handle_cast({:deliver, ref, event}, state) do
    # External delivery - from Actor.deliver/3 or Actor.send_message/3
    # Deliver the event - either locally or to remote actor
    # For local refs, we process directly with flow control tracking
    # For remote refs, we forward to the remote actor (which tracks its own load)
    #
    # LIVENESS CHECK (syndicate-rs pattern): For Assert events, check if the handle
    # is still tracked in outbound_assertions. If not (facet terminated and retracted),
    # drop the event - the entity will receive the retraction instead.
    new_state =
      case Ref.local?(ref, state.id) do
        true ->
          # Liveness check for Assert events (syndicate-rs pattern)
          if assert_handle_alive?(event, state) do
            # Local delivery - borrow debt, process, then repay
            # This tracks load from external API calls
            state = borrow_for_incoming_event(state, event)
            state = check_flow_control_overload(state)
            state = deliver_event_impl(state, ref, event, nil)
            repay_action_cost(state, event)
          else
            # Handle was retracted (facet terminated), drop this assert
            Logger.debug("Dropping Assert for retracted handle (liveness check)")
            state
          end

        false ->
          # Remote delivery - forward to the remote actor
          # Apply same liveness check as local to prevent stale asserts reaching remotes
          if assert_handle_alive?(event, state) do
            remote_actor_id = Ref.actor_id(ref)

            case resolve_actor(remote_actor_id) do
              {:ok, actor_pid} ->
                GenServer.cast(actor_pid, {:deliver_from, ref, event, self()})
                state

              :error ->
                Logger.warning(
                  "Cannot deliver to remote actor #{inspect(remote_actor_id)}: not found"
                )

                state
            end
          else
            # Handle was retracted, drop this assert (retract will reach remote)
            Logger.debug("Dropping remote Assert for retracted handle (liveness check)")
            state
          end
      end

    {:noreply, new_state}
  end

  @impl GenServer
  def handle_cast({:deliver_internal, ref, event}, state) do
    # Internal delivery from self-cast during commit_turn
    # We borrowed for these in borrow_for_actions, so we repay here
    #
    # LIVENESS CHECK (syndicate-rs pattern): same as :deliver - drop Assert if handle retracted
    new_state =
      if assert_handle_alive?(event, state) do
        state = check_flow_control_overload(state)

        # Local delivery - process the event directly
        state = deliver_event_impl(state, ref, event, nil)

        # Flow control: repay debt after turn completion
        # This corresponds to the borrow done in commit_turn
        repay_action_cost(state, event)
      else
        # Handle was retracted, drop this assert but still repay the borrowed debt
        Logger.debug("Dropping internal Assert for retracted handle (liveness check)")
        repay_action_cost(state, event)
      end

    {:noreply, new_state}
  end

  @impl GenServer
  def handle_cast({:deliver_from, ref, event, sender_pid}, state) do
    # Delivery from another local actor with sender tracking for crash cleanup
    # Flow control: borrow debt for incoming events to track receiver load
    state = borrow_for_incoming_event(state, event)
    state = check_flow_control_overload(state)

    new_state = deliver_event_impl(state, ref, event, sender_pid)

    # Flow control: repay debt after processing
    new_state = repay_action_cost(new_state, event)
    {:noreply, new_state}
  end

  @impl GenServer
  def handle_cast({:deliver_with_flow_control, ref, event, relay_pid, loan_id}, state) do
    # Deliver event with flow control ack - send ack back after turn completes
    new_state = deliver_event_with_flow_control(state, ref, event, relay_pid, loan_id)
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
          entities: MapSet.new(),
          # Inertness tracking
          inert_check_preventers: 0,
          linked_tasks: MapSet.new()
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
  # Following syndicate-rs pattern: async delivery for all events.
  # Ordering is preserved via mailbox FIFO - retracts queued after asserts
  # will be processed after asserts.
  @doc false
  defp retract_facet_handles(state, facet_id) do
    handles = Map.get(state.facet_handles, facet_id, MapSet.new())

    Enum.reduce(handles, state, fn handle, acc_state ->
      case Map.fetch(acc_state.outbound_assertions, handle) do
        {:ok, {ref, _assertion}} ->
          # Create and deliver the retract event asynchronously
          event = Event.retract(ref, handle)
          acc_state = deliver_to_ref(acc_state, ref, event, :async)

          # Remove from outbound assertions after queuing the retract delivery.
          # Since delivery is async (via cast), by the time any pending assert
          # deliveries are processed, this removal will have taken effect.
          # The liveness check in handle_cast sees the updated state.
          %{acc_state | outbound_assertions: Map.delete(acc_state.outbound_assertions, handle)}

        :error ->
          # Handle not found, skip (may have been manually retracted)
          acc_state
      end
    end)
  end

  # Liveness check for Assert events (syndicate-rs pattern)
  #
  # Returns true if the event should be delivered:
  # - For Assert events with handles WE OWN: only if the handle is still tracked
  # - For Assert events with FOREIGN handles (from remote actors): always true
  # - For all other events (Retract, Message, Sync): always true
  #
  # This prevents delivering OUR Assert events for handles that have already been
  # retracted (e.g., because the owning facet terminated). The entity will
  # receive the Retract event instead, maintaining consistent state.
  #
  # Foreign handles (from remote actors via relay or Actor.deliver) must pass
  # through unconditionally - we don't track them in outbound_assertions.
  #
  # ## Important Ordering Invariants
  #
  # Foreign handles that entities (e.g., dataspaces) create in their turns ARE
  # tracked in outbound_assertions, but their actor_id differs from ours.
  # This is safe because:
  #
  # 1. **Action-before-termination**: commit_turn processes ALL actions before
  #    stop_inert_facets checks for termination, so foreign-handle asserts are
  #    tracked and queued BEFORE any facet termination.
  #
  # 2. **FIFO mailbox**: Async deliveries via self-cast preserve order. Assert
  #    is queued first, retract is queued after termination.
  #
  # 3. **Correct semantics**: Observers see assert then retract, which is the
  #    correct syndicate behavior - the assertion existed and was retracted.
  #
  # The foreign handle's owner actor is responsible for its lifecycle and
  # retract ordering on their end.
  @doc false
  defp assert_handle_alive?(%Event.Assert{handle: handle}, state) do
    # Only apply liveness check to handles we own (created by this actor)
    # Foreign handles from remote actors always pass through
    if Handle.actor_id(handle) == state.id do
      # Our handle - check if still tracked (not retracted)
      Map.has_key?(state.outbound_assertions, handle)
    else
      # Foreign handle - always allow
      true
    end
  end

  defp assert_handle_alive?(_event, _state), do: true

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

  @doc false
  # Deliver event with flow control ack - sends ack back to relay after turn completes
  defp deliver_event_with_flow_control(state, ref, event, relay_pid, loan_id) do
    entity_id = Ref.entity_id(ref)

    # Apply attenuation to the event (may rewrite or reject)
    case apply_event_attenuation(ref, event) do
      {:ok, rewritten_event} ->
        case Map.fetch(state.entities, entity_id) do
          {:ok, {facet_id, entity}} ->
            # Track inbound assertions from remote actors for crash cleanup
            # Use relay_pid as the sender for crash tracking
            state = maybe_track_inbound_assertion(state, rewritten_event, ref, relay_pid)

            # Execute a turn for this event
            result = execute_turn(state, facet_id, entity_id, entity, rewritten_event)

            # Send flow control ack back to relay after turn completes (success or failure)
            # This ensures debt is repaid even if the turn fails
            send(relay_pid, {:flow_control_loan_ack, loan_id})

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
            # Entity not found - still send ack to prevent debt accumulation
            Logger.warning("Entity #{entity_id} not found for event: #{inspect(event)}")
            send(relay_pid, {:flow_control_loan_ack, loan_id})
            state
        end

      :rejected ->
        # Event rejected by attenuation - still send ack
        Logger.debug("Event rejected by attenuation on ref #{inspect(ref)}")
        send(relay_pid, {:flow_control_loan_ack, loan_id})
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

    # Flow control: borrow from account for each action's cost
    # This charges fan-out to the originating turn
    state = borrow_for_actions(state, actions)

    # Process each pending action
    state =
      Enum.reduce(actions, state, fn action, acc_state ->
        process_action(acc_state, action, facet_id)
      end)

    # After processing actions, check for inert facets and terminate them
    # This implements the SAM inertness detection and auto-teardown
    stop_inert_facets(state, facet_id)
  end

  # Check if actor is overloaded based on flow control and mailbox length
  # Emits telemetry when paused, allowing external monitoring and intervention
  defp check_flow_control_overload(%{flow_control_account: nil} = state), do: state

  defp check_flow_control_overload(%{flow_control_account: account} = state) do
    if account.paused do
      # Get mailbox length for telemetry
      {:message_queue_len, mailbox_len} = Process.info(self(), :message_queue_len)

      :telemetry.execute(
        [:absynthe, :flow_control, :actor, :overload],
        %{debt: account.debt, mailbox_length: mailbox_len},
        %{actor_id: state.id, account_id: account.id}
      )

      # Log warning if mailbox is also growing
      if mailbox_len > account.limit do
        Logger.warning(
          "Actor #{inspect(state.id)} overloaded: debt=#{account.debt}, mailbox=#{mailbox_len}"
        )
      end
    end

    state
  end

  # Borrow from flow control account for the cost of all actions
  # This is preemptive - we charge the originating turn for the cascade it causes
  #
  # Note: We directly manipulate debt instead of using Account.force_borrow/2
  # because we track aggregate debt, not individual loans. This avoids growing
  # the loaned_items map unboundedly.
  defp borrow_for_actions(%{flow_control_account: nil} = state, _actions), do: state

  defp borrow_for_actions(%{flow_control_account: account} = state, actions) do
    # Calculate total cost of all actions
    total_cost = Enum.reduce(actions, 0, fn action, acc -> acc + action_cost(action) end)

    if total_cost > 0 do
      # Directly increment debt (no loan tracking needed for aggregate debt)
      new_debt = account.debt + total_cost
      new_account = %{account | debt: new_debt}

      # Check if we should pause (debt exceeded high water mark)
      new_account =
        if not account.paused and new_debt >= account.high_water_mark do
          :telemetry.execute(
            [:absynthe, :flow_control, :account, :pause],
            %{debt: new_debt, high_water: account.high_water_mark},
            %{account_id: account.id}
          )

          paused_account = %{new_account | paused: true}

          if account.pause_callback do
            account.pause_callback.(paused_account)
          end

          paused_account
        else
          new_account
        end

      %{state | flow_control_account: new_account}
    else
      state
    end
  end

  # Borrow from flow control account for an incoming event from another local actor
  # This tracks receiver load for inter-actor communication
  defp borrow_for_incoming_event(%{flow_control_account: nil} = state, _event), do: state

  defp borrow_for_incoming_event(%{flow_control_account: account} = state, event) do
    cost = action_cost(event)

    if cost > 0 do
      # Directly increment debt (no loan tracking needed for aggregate debt)
      new_debt = account.debt + cost
      new_account = %{account | debt: new_debt}

      # Check if we should pause (debt exceeded high water mark)
      new_account =
        if not account.paused and new_debt >= account.high_water_mark do
          :telemetry.execute(
            [:absynthe, :flow_control, :account, :pause],
            %{debt: new_debt, high_water: account.high_water_mark},
            %{account_id: account.id}
          )

          paused_account = %{new_account | paused: true}

          if account.pause_callback do
            account.pause_callback.(paused_account)
          end

          paused_account
        else
          new_account
        end

      %{state | flow_control_account: new_account}
    else
      state
    end
  end

  # Calculate the cost of an action for flow control
  # Delegates to FlowControl.event_cost/1 for protocol events
  defp action_cost(%Assert{} = event), do: FlowControl.event_cost(event)
  defp action_cost(%Retract{} = event), do: FlowControl.event_cost(event)
  defp action_cost(%Message{} = event), do: FlowControl.event_cost(event)
  defp action_cost(%Sync{} = event), do: FlowControl.event_cost(event)
  defp action_cost(%Event.Spawn{}), do: 2
  defp action_cost({:field_update, _, _}), do: 1
  defp action_cost(_), do: 1

  # Repay flow control debt after processing an event
  # This balances the borrow done in commit_turn when the action was enqueued
  defp repay_action_cost(%{flow_control_account: nil} = state, _event), do: state

  defp repay_action_cost(%{flow_control_account: account} = state, event) do
    cost = action_cost(event)

    if cost > 0 do
      # Directly reduce debt and check resume threshold
      # This is simpler than using the loan mechanism since we track aggregate debt
      new_debt = max(0, account.debt - cost)
      new_account = %{account | debt: new_debt}

      # Check if we should resume (debt fell below low water mark)
      new_account =
        if account.paused and new_debt < account.low_water_mark do
          :telemetry.execute(
            [:absynthe, :flow_control, :account, :resume],
            %{debt: new_debt, low_water: account.low_water_mark},
            %{account_id: account.id}
          )

          resumed_account = %{new_account | paused: false}

          if account.resume_callback do
            account.resume_callback.(resumed_account)
          end

          resumed_account
        else
          new_account
        end

      %{state | flow_control_account: new_account}
    else
      state
    end
  end

  @doc false
  defp process_action(state, {:field_update, field_id, new_value} = action, _facet_id) do
    # Execute dataflow field updates
    Absynthe.Dataflow.Field.execute_field_update(field_id, new_value)
    # Field updates are synchronous - repay immediately
    repay_action_cost(state, action)
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
    # Note: inertness check happens after commit_turn via stop_inert_facets
    {state, _owning_facet_id} = untrack_assertion_handle(state, handle)
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
         %Event.Spawn{facet_id: facet_id, entity: entity, callback: callback} = action,
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

        # Spawns are synchronous - repay immediately
        repay_action_cost(new_state, action)

      {:error, reason} ->
        # Spawn failure aborts the turn to maintain atomicity
        # The try/rescue in deliver_event_impl will catch this and log it
        raise "Failed to spawn entity in facet #{facet_id}: #{inspect(reason)}"
    end
  end

  defp process_action(state, action, _facet_id) do
    # Unknown action type - log warning
    Logger.warning("Unknown action type: #{inspect(action)}")
    # Repay for unknown actions to prevent debt leak
    repay_action_cost(state, action)
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
  # Returns {state, owning_facet_id | nil} where owning_facet_id is the facet that owned
  # the handle, allowing the caller to trigger inertness checks if needed.
  defp untrack_assertion_handle(state, handle) do
    case Map.fetch(state.outbound_assertions, handle) do
      {:ok, _} ->
        # Remove from outbound assertions
        state = %{state | outbound_assertions: Map.delete(state.outbound_assertions, handle)}

        # Find which facet owns this handle and remove it
        {facet_handles, owning_facet_id} =
          Enum.reduce(state.facet_handles, {%{}, nil}, fn {fid, handles}, {acc_map, owner} ->
            if MapSet.member?(handles, handle) do
              {Map.put(acc_map, fid, MapSet.delete(handles, handle)), fid}
            else
              {Map.put(acc_map, fid, handles), owner}
            end
          end)

        {%{state | facet_handles: facet_handles}, owning_facet_id}

      :error ->
        # Not our handle, nothing to do
        {state, nil}
    end
  end

  # Inertness Detection and Auto-Teardown
  #
  # A facet is "inert" when it has no work left to do:
  # - No children facets
  # - No outbound assertion handles
  # - No linked tasks
  # - No inert-check preventers active
  #
  # Inert facets are automatically terminated to clean up unused conversational
  # scopes. When a facet is terminated due to inertness, we check if its parent
  # also becomes inert, cascading up the tree.

  # Check if a specific facet is inert
  defp facet_is_inert?(state, facet_id, facet) do
    # Get the handles for this facet
    handles = Map.get(state.facet_handles, facet_id, MapSet.new())

    # A facet is inert if:
    # - It has no children
    # - It has no outbound handles
    # - It has no linked tasks
    # - It has no inert-check preventers
    MapSet.size(facet.children) == 0 and
      MapSet.size(handles) == 0 and
      MapSet.size(facet.linked_tasks) == 0 and
      facet.inert_check_preventers == 0
  end

  # Stop inert facets starting from a given facet, cascading to parents
  defp stop_inert_facets(state, facet_id) do
    case Map.fetch(state.facets, facet_id) do
      {:ok, facet} ->
        if facet_is_inert?(state, facet_id, facet) do
          # Don't terminate the root facet via inertness
          if facet_id == state.root_facet_id do
            state
          else
            # Terminate this facet
            Logger.debug("Facet #{inspect(facet_id)} is inert, auto-terminating")

            case terminate_facet_impl(state, facet_id) do
              {:ok, new_state} ->
                # Check if parent is now inert
                if facet.parent_id do
                  stop_inert_facets(new_state, facet.parent_id)
                else
                  new_state
                end

              {:error, _reason} ->
                # Failed to terminate, just return current state
                state
            end
          end
        else
          state
        end

      :error ->
        # Facet doesn't exist, nothing to do
        state
    end
  end

  # Deliver an event to a target ref (local or remote)
  # NOTE: This 3-arg version defaults to :sync but is currently unused.
  # All code paths now use explicit :async mode following the syndicate-rs pattern.
  defp deliver_to_ref(state, ref, event) do
    deliver_to_ref(state, ref, event, :sync)
  end

  # Deliver an event to a target ref with specified mode
  #
  # Following the syndicate-rs pattern, all delivery is now asynchronous:
  # - :async mode queues events via self-cast for local refs, GenServer.cast for remote
  # - :sync mode is kept for backwards compatibility but currently unused
  #
  # The async pattern ensures ordering via mailbox FIFO, with a liveness check
  # at delivery time that drops Assert events if their handle was already retracted.
  defp deliver_to_ref(state, ref, event, mode) do
    case Ref.local?(ref, state.id) do
      true ->
        case mode do
          :sync ->
            # Synchronous local delivery - currently unused, kept for backwards compat
            # Apply flow control hooks to maintain proper accounting
            state = borrow_for_incoming_event(state, event)
            state = check_flow_control_overload(state)
            # nil sender_pid - we own these assertions, no crash tracking needed
            state = deliver_event_impl(state, ref, event, nil)
            repay_action_cost(state, event)

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
  #
  # Local delivery is ASYNCHRONOUS - events are enqueued via self-cast rather than
  # processed immediately. This keeps call stacks shallow and allows the actor to
  # process mailbox work between cascades.
  #
  # Flow control debt is repaid:
  # - For local delivery: when handle_cast({:deliver, ...}) completes
  # - For remote delivery: immediately after cast (remote actor handles its own cost)
  #
  # IMPORTANT: This means turns no longer complete all local effects atomically.
  # Observers will see effects in subsequent turns. This is a trade-off for:
  # - Shallow call stacks (prevents stack overflow on large fan-out)
  # - Fair scheduling (actor doesn't monopolize scheduler during cascades)
  # - Interleaved mailbox processing (external messages can be processed between local deliveries)
  defp deliver_action_to_ref(state, ref, event) do
    case Ref.local?(ref, state.id) do
      true ->
        # Local delivery - enqueue via self-cast for async processing
        # This keeps call stacks shallow during observer fan-out
        # Uses :deliver_internal so we can distinguish from external delivers
        # Debt repayment happens in handle_cast({:deliver_internal, ...})
        GenServer.cast(self(), {:deliver_internal, ref, event})
        state

      false ->
        # Remote delivery - send to remote actor with sender PID for crash tracking
        # Repay debt immediately - the remote actor will track its own cost
        remote_actor_id = Ref.actor_id(ref)

        case resolve_actor(remote_actor_id) do
          {:ok, actor_pid} ->
            # Use :deliver_from to include sender PID for crash cleanup
            GenServer.cast(actor_pid, {:deliver_from, ref, event, self()})
            # Repay our debt - remote actor manages its own flow control
            repay_action_cost(state, event)

          :error ->
            Logger.warning(
              "Cannot deliver to remote actor #{inspect(remote_actor_id)}: not found"
            )

            # Repay even on error - the work is done from our perspective
            repay_action_cost(state, event)
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
