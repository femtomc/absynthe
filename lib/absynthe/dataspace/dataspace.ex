defmodule Absynthe.Dataspace.Dataspace do
  @moduledoc """
  Core routing mechanism for the Syndicated Actor Model.

  ## Purpose

  A Dataspace is a special entity that serves as the central coordination point
  for actors in the Syndicated Actor Model. It:

  1. Receives assertions and messages from actors
  2. Routes assertions to interested observers via pattern matching
  3. Maintains a set of current assertions using reference-counted storage
  4. Manages subscriptions through Observe assertions

  ## Architecture

  The Dataspace coordinates three key subsystems:

  - **`Absynthe.Assertions.Bag`** - Reference-counted assertion storage that tracks
    how many times each assertion has been published (assertions may be published
    multiple times and only disappear when fully retracted).

  - **`Absynthe.Dataspace.Skeleton`** - Efficient pattern-based indexing structure
    that maps assertions to interested observers and vice versa. The skeleton uses
    compiled patterns to quickly determine which observers should be notified when
    assertions are published or retracted.

  - **`Absynthe.Dataspace.Observer`** - Subscription management for entities that
    want to observe assertions matching specific patterns. Each observer is created
    from an Observe assertion and tracks the pattern and target entity.

  ## Data Flow

  ### Publishing Assertions

  When an entity publishes an assertion to the dataspace:

  1. Check if it's an Observe assertion (subscription request)
     - If yes: Create an Observer, add to skeleton, notify of existing matches
     - If no: Continue to step 2

  2. Add to Bag (reference-counted storage)
     - If new: Add to skeleton index, notify matching observers
     - If existing: Just increment reference count

  3. Return updated dataspace and turn

  ### Retracting Assertions

  When an entity retracts an assertion handle:

  1. Check if it's an Observe handle (subscription removal)
     - If yes: Remove observer from skeleton and cleanup
     - If no: Continue to step 2

  2. Remove from Bag
     - If last reference: Remove from skeleton, notify observers
     - If still referenced: Just decrement reference count

  3. Return updated dataspace and turn

  ### Message Routing

  Messages sent to a dataspace are routed to all observers whose subscription
  patterns match the message. Unlike assertions, messages are transient:
  - They are not stored in the dataspace
  - They are delivered once to all matching observers
  - Captures from pattern matching are not included in the delivered message

  This implements the Syndicate message semantics where messages are interest-gated
  broadcasts. An observer must have an active Observe assertion with a matching
  pattern to receive messages.

  ## Observe Assertions

  The Observe assertion is a special assertion type that represents a subscription.
  It has the structure:

      {:record, {{:symbol, "Observe"}, [pattern, entity_ref]}}

  Where:
  - `pattern` is a Preserves pattern value
  - `entity_ref` is a Ref to the entity that should receive notifications

  When an Observe assertion is published, the dataspace:
  1. Compiles the pattern for efficient matching
  2. Creates an Observer to track the subscription
  3. Queries existing assertions for matches
  4. Notifies the observer of all existing matches
  5. Tracks the observer for future assertion changes

  ## Expected Module Interfaces

  ### `Absynthe.Assertions.Bag`

  The Bag module must provide:

      @spec new() :: Bag.t()
      @spec add(Bag.t(), assertion, Handle.t()) :: {:new, Bag.t()} | {:existing, Bag.t()}
      @spec remove(Bag.t(), Handle.t()) :: {:removed, Bag.t()} | {:decremented, Bag.t()} | :not_found
      @spec member?(Bag.t(), assertion) :: boolean()
      @spec assertions(Bag.t()) :: [assertion]

  ### `Absynthe.Dataspace.Skeleton`

  The Skeleton uses four ETS tables for efficient indexing, plus a wildcard observer set:
  - **path_index**: Maps `{path, value}` to assertion handles
  - **assertions**: Maps handles to assertion values
  - **observers**: Maps observer IDs to `{pattern, ref}`
  - **observer_index**: Maps `{path, value_class}` to sets of `{observer_id, exact_value}`
  - **wildcard_observers** (in struct): Observer IDs with unconstrained patterns

  The module must provide:

      @spec new() :: Skeleton.t()
      @spec add_observer(Skeleton.t(), observer_id, Ref.t(), compiled_pattern) ::
        {Skeleton.t(), [{Handle.t(), assertion, captures}]}
      @spec remove_observer(Skeleton.t(), observer_id) :: Skeleton.t()
      @spec add_assertion(Skeleton.t(), Handle.t(), assertion) ::
        [{observer_id, observer_ref, assertion, captures}]
      @spec remove_assertion(Skeleton.t(), Handle.t()) ::
        [{observer_id, observer_ref}]

  **Note**: `add_observer` and `remove_observer` return updated skeleton structs that must be
  stored. ETS tables are mutated in-place for assertions, but the wildcard observer set lives
  in the struct and requires the caller to use the returned skeleton for correct notifications.

  ### `Absynthe.Dataspace.Observer`

  The Observer module must provide:

      @spec from_assertion(assertion) :: {:ok, {pattern, Ref.t()}} | :error
      @spec new(Handle.t(), compiled_pattern, Ref.t()) :: Observer.t()

  ### `Absynthe.Dataspace.Pattern`

  The Pattern module must provide:

      @spec compile(pattern) :: compiled_pattern

  ## Examples

      # Create a new dataspace
      dataspace = Absynthe.Dataspace.Dataspace.new()

      # The dataspace implements the Entity protocol
      {updated_dataspace, updated_turn} =
        Absynthe.Core.Entity.on_publish(dataspace, assertion, handle, turn)

      # Query current assertions
      assertions = Absynthe.Dataspace.Dataspace.assertions(dataspace)

      # Check if assertion exists
      exists? = Absynthe.Dataspace.Dataspace.has_assertion?(dataspace, assertion)

  ## Implementation Notes

  - The dataspace is purely functional - all operations return updated copies
  - Turn-based execution ensures proper sequencing of effects
  - Reference counting prevents premature assertion removal
  - Pattern compilation happens once per observer for efficiency
  - Observer notifications are accumulated in the Turn for atomic execution
  """

  alias Absynthe.Assertions.{Bag, Handle}
  alias Absynthe.Dataspace.{Skeleton, Observer, Pattern}
  alias Absynthe.Core.Turn
  alias Absynthe.Protocol.Event
  alias Absynthe.Preserves.Value

  @typedoc """
  The Dataspace entity structure.

  Fields:
  - `bag` - Reference-counted storage for all current assertions
  - `skeleton` - Pattern-based index mapping assertions to observers
  - `observers` - Map of observer_id to Observer structs
  - `handle_to_observer` - Reverse mapping from Observe assertion handle to observer_id
  """
  @type t :: %__MODULE__{
          bag: Bag.t(),
          skeleton: Skeleton.t(),
          observers: %{term() => Observer.t()},
          handle_to_observer: %{Handle.t() => term()}
        }

  defstruct bag: nil, skeleton: nil, observers: %{}, handle_to_observer: %{}

  # Constructor

  @doc """
  Creates a new empty dataspace.

  The dataspace is initialized with:
  - Empty Bag for assertion storage
  - Empty Skeleton for pattern indexing
  - No observers
  - No handle mappings

  ## Options

  - `:use_ets` - Whether to use ETS for storage (default: true in production)

  ## Examples

      iex> dataspace = Absynthe.Dataspace.Dataspace.new()
      iex> Absynthe.Dataspace.Dataspace.assertions(dataspace)
      []

      # For testing without ETS
      iex> dataspace = Absynthe.Dataspace.Dataspace.new(use_ets: false)
      iex> Absynthe.Dataspace.Dataspace.assertions(dataspace)
      []
  """
  @spec new(keyword()) :: t()
  def new(_opts \\ []) do
    %__MODULE__{
      bag: Bag.new(),
      skeleton: Skeleton.new(),
      observers: %{},
      handle_to_observer: %{}
    }
  end

  # Query API

  @doc """
  Returns all current assertions in the dataspace.

  This includes all assertions that have been published and not fully retracted.
  Each assertion appears once in the list regardless of its reference count.

  ## Parameters

  - `dataspace` - The dataspace to query

  ## Returns

  A list of Preserves values representing the current assertions.

  ## Examples

      iex> dataspace = Absynthe.Dataspace.Dataspace.new()
      iex> Absynthe.Dataspace.Dataspace.assertions(dataspace)
      []
  """
  @spec assertions(t()) :: [Value.t()]
  def assertions(%__MODULE__{bag: bag}) do
    Bag.assertions(bag)
  end

  @doc """
  Checks if an assertion is currently present in the dataspace.

  An assertion is present if it has been published at least once and not
  fully retracted (reference count > 0).

  ## Parameters

  - `dataspace` - The dataspace to check
  - `assertion` - The assertion value to look for

  ## Returns

  `true` if the assertion is present, `false` otherwise.

  ## Examples

      iex> dataspace = Absynthe.Dataspace.Dataspace.new()
      iex> Absynthe.Dataspace.Dataspace.has_assertion?(dataspace, {:string, "test"})
      false
  """
  @spec has_assertion?(t(), Value.t()) :: boolean()
  def has_assertion?(%__MODULE__{bag: bag}, assertion) do
    Bag.member?(bag, assertion)
  end

  @doc """
  Queries the dataspace for assertions matching a pattern.

  Returns all assertions that match the given pattern along with their
  handles and captured bindings.

  ## Parameters

  - `dataspace` - The dataspace to query
  - `pattern` - A Preserves pattern to match against

  ## Returns

  A list of tuples `{handle, assertion, captures}` where:
  - `handle` is the Handle identifying the assertion
  - `assertion` is the matched Preserves value
  - `captures` is a map of captured pattern variables

  ## Examples

      iex> dataspace = Absynthe.Dataspace.Dataspace.new()
      iex> pattern = Absynthe.Preserves.Pattern.capture(:x)
      iex> Absynthe.Dataspace.Dataspace.query(dataspace, pattern)
      []
  """
  @spec query(t(), Value.t()) :: [{Handle.t(), Value.t(), map()}]
  def query(%__MODULE__{skeleton: skeleton}, pattern) do
    compiled = Pattern.compile(pattern)
    Skeleton.query(skeleton, compiled)
  end

  # Internal Helpers

  @doc false
  @spec handle_assertion(t(), Value.t(), Handle.t(), Turn.t()) :: {t(), Turn.t()}
  def handle_assertion(dataspace, assertion, handle, turn) do
    case Observer.from_assertion(assertion) do
      {:ok, {pattern, entity_ref}} ->
        handle_observe_assertion(dataspace, pattern, entity_ref, handle, turn)

      :error ->
        handle_regular_assertion(dataspace, assertion, handle, turn)
    end
  end

  @doc false
  @spec handle_retraction(t(), Handle.t(), Turn.t()) :: {t(), Turn.t()}
  def handle_retraction(dataspace, handle, turn) do
    case Map.get(dataspace.handle_to_observer, handle) do
      nil ->
        handle_regular_retraction(dataspace, handle, turn)

      observer_id ->
        handle_observe_retraction(dataspace, observer_id, handle, turn)
    end
  end

  @doc false
  @spec handle_message(t(), Value.t(), Turn.t()) :: {t(), Turn.t()}
  def handle_message(dataspace, message, turn) do
    # Route message to all observers whose patterns match
    observer_matches = Skeleton.send_message(dataspace.skeleton, message)

    # Notify each matching observer with a message event
    turn = notify_observers_of_message(turn, observer_matches, message)

    {dataspace, turn}
  end

  # Observe Assertion Handling

  defp handle_observe_assertion(%__MODULE__{} = dataspace, pattern, entity_ref, handle, turn) do
    # Compile the pattern for efficient matching
    compiled = Pattern.compile(pattern)

    # Generate observer_id first, then create observer with consistent ID
    observer_id = generate_observer_id(handle)
    observer = Observer.new(observer_id, compiled, entity_ref)

    # Add observer to skeleton and get existing matches
    # Returns {updated_skeleton, [{assertion_handle, assertion_value, captures}]}
    # Pass observer_id (not handle) so skeleton returns consistent IDs
    {updated_skeleton, existing_matches} =
      Skeleton.add_observer(dataspace.skeleton, observer_id, entity_ref, compiled)

    # Track existing matches in the observer's active_handles
    observer =
      Enum.reduce(existing_matches, observer, fn {assertion_handle, _value, _captures}, obs ->
        Observer.add_match(obs, assertion_handle)
      end)

    # Notify observer of existing assertions
    turn = notify_observer_of_existing(turn, entity_ref, existing_matches)

    # Update dataspace with new observer and updated skeleton
    updated_dataspace = %__MODULE__{
      dataspace
      | skeleton: updated_skeleton,
        observers: Map.put(dataspace.observers, observer_id, observer),
        handle_to_observer: Map.put(dataspace.handle_to_observer, handle, observer_id)
    }

    {updated_dataspace, turn}
  end

  defp handle_observe_retraction(%__MODULE__{} = dataspace, observer_id, handle, turn) do
    # Remove observer from skeleton
    # Returns updated skeleton with wildcard_observers updated
    # Use observer_id (not handle) for consistency with add_observer
    updated_skeleton = Skeleton.remove_observer(dataspace.skeleton, observer_id)

    # Clean up observer tracking
    updated_dataspace = %__MODULE__{
      dataspace
      | skeleton: updated_skeleton,
        observers: Map.delete(dataspace.observers, observer_id),
        handle_to_observer: Map.delete(dataspace.handle_to_observer, handle)
    }

    {updated_dataspace, turn}
  end

  # Regular Assertion Handling

  defp handle_regular_assertion(%__MODULE__{} = dataspace, assertion, handle, turn) do
    case Bag.add(dataspace.bag, assertion, handle) do
      {:new, bag} ->
        # First occurrence - add to skeleton and notify observers
        # Skeleton is ETS-based, so it's mutated in place
        # Returns [{observer_id, observer_ref, assertion_value, captures}]
        observer_matches = Skeleton.add_assertion(dataspace.skeleton, handle, assertion)

        # Update observers' active_handles with the new assertion handle
        observers =
          Enum.reduce(observer_matches, dataspace.observers, fn
            {observer_id, _observer_ref, _value, _captures}, obs_map ->
              case Map.get(obs_map, observer_id) do
                nil ->
                  obs_map

                observer ->
                  Map.put(obs_map, observer_id, Observer.add_match(observer, handle))
              end
          end)

        # Notify all matching observers
        turn = notify_observers_of_publish(turn, observer_matches, assertion, handle)

        updated_dataspace = %__MODULE__{dataspace | bag: bag, observers: observers}
        {updated_dataspace, turn}

      {:existing, bag} ->
        # Already present, just update reference count
        updated_dataspace = %__MODULE__{dataspace | bag: bag}
        {updated_dataspace, turn}
    end
  end

  defp handle_regular_retraction(%__MODULE__{} = dataspace, handle, turn) do
    case Bag.remove(dataspace.bag, handle) do
      {:removed, bag, assertion_value} ->
        # Last occurrence - remove from skeleton and notify observers
        # Use remove_assertion_by_value because the skeleton may have stored
        # a different (canonical) handle for this assertion value
        # Returns {canonical_handle, [{observer_id, observer_ref}]}
        {canonical_handle, observer_info} =
          Skeleton.remove_assertion_by_value(dataspace.skeleton, assertion_value)

        # Remove the canonical handle from each observer's active_handles
        # (observers track the canonical handle, not the one being retracted)
        observers =
          if canonical_handle do
            Enum.reduce(observer_info, dataspace.observers, fn
              {observer_id, _observer_ref}, obs_map ->
                case Map.get(obs_map, observer_id) do
                  nil ->
                    obs_map

                  observer ->
                    Map.put(
                      obs_map,
                      observer_id,
                      Observer.remove_match(observer, canonical_handle)
                    )
                end
            end)
          else
            dataspace.observers
          end

        # Notify all observers that were watching this assertion
        # Use the canonical handle in notifications for consistency
        notification_handle = canonical_handle || handle
        turn = notify_observers_of_retract(turn, observer_info, notification_handle)

        updated_dataspace = %__MODULE__{dataspace | bag: bag, observers: observers}
        {updated_dataspace, turn}

      {:decremented, bag} ->
        # Still present, just update reference count
        updated_dataspace = %__MODULE__{dataspace | bag: bag}
        {updated_dataspace, turn}

      :not_found ->
        # Handle not found - this is a no-op (could log a warning)
        {dataspace, turn}
    end
  end

  # Notification Helpers

  defp notify_observer_of_existing(turn, entity_ref, matches) do
    # For each existing assertion that matches the observer's pattern,
    # send a publish notification to the observer with captured bindings
    Enum.reduce(matches, turn, fn {handle, assertion, captures}, turn_acc ->
      action = Event.assert(entity_ref, assertion, handle, captures)
      Turn.add_action(turn_acc, action)
    end)
  end

  defp notify_observers_of_publish(turn, observer_matches, assertion, handle) do
    # For each observer that matches this assertion, send a publish notification
    # with captured bindings from pattern matching
    # observer_matches is [{observer_id, observer_ref, value, captures}]
    Enum.reduce(observer_matches, turn, fn {_observer_id, observer_ref, _value, captures},
                                           turn_acc ->
      action = Event.assert(observer_ref, assertion, handle, captures)
      Turn.add_action(turn_acc, action)
    end)
  end

  defp notify_observers_of_retract(turn, observer_info, handle) do
    # For each observer that was watching this assertion, send a retract notification
    # observer_info is [{observer_id, observer_ref}]
    Enum.reduce(observer_info, turn, fn {_observer_id, observer_ref}, turn_acc ->
      action = Event.retract(observer_ref, handle)
      Turn.add_action(turn_acc, action)
    end)
  end

  defp notify_observers_of_message(turn, observer_matches, message) do
    # For each observer whose pattern matches the message, send a message notification
    # observer_matches is [{observer_id, observer_ref, captures}]
    Enum.reduce(observer_matches, turn, fn {_observer_id, observer_ref, _captures}, turn_acc ->
      action = Event.message(observer_ref, message)
      Turn.add_action(turn_acc, action)
    end)
  end

  # Utilities

  defp generate_observer_id(handle) do
    # Use the handle directly as the observer ID since handles are globally unique
    # (they include actor_id + local counter)
    {:observer, handle}
  end
end

# Entity Protocol Implementation

defimpl Absynthe.Core.Entity, for: Absynthe.Dataspace.Dataspace do
  alias Absynthe.Dataspace.Dataspace
  alias Absynthe.Core.Turn
  alias Absynthe.Protocol.Event

  @doc """
  Called when an assertion is published to the dataspace.

  This is the core routing logic. When an assertion arrives:

  1. Check if it's an Observe assertion (subscription)
     - If yes: Register the observer and notify of existing matches
     - If no: Add to storage and notify matching observers

  2. Update the dataspace state and turn

  ## Parameters

  - `dataspace` - The dataspace receiving the assertion
  - `assertion` - The Preserves value being asserted
  - `handle` - Unique handle for later retraction
  - `turn` - The current turn context

  ## Returns

  A tuple of `{updated_dataspace, updated_turn}`.

  The turn will contain actions to notify any observers that match the assertion.
  """
  def on_publish(dataspace, assertion, handle, turn) do
    Dataspace.handle_assertion(dataspace, assertion, handle, turn)
  end

  @doc """
  Called when a previously published assertion is retracted.

  Removes the assertion from storage (or decrements its reference count) and
  notifies any observers that were watching it.

  ## Parameters

  - `dataspace` - The dataspace receiving the retraction
  - `handle` - The handle of the assertion to retract
  - `turn` - The current turn context

  ## Returns

  A tuple of `{updated_dataspace, updated_turn}`.

  The turn will contain actions to notify any observers about the retraction.
  """
  def on_retract(dataspace, handle, turn) do
    Dataspace.handle_retraction(dataspace, handle, turn)
  end

  @doc """
  Called when a message is sent to the dataspace.

  Routes messages to all observers whose subscription patterns match the message.
  This implements the Syndicate message semantics where messages are transient
  events delivered to interested parties based on their subscription patterns.

  Unlike assertions, messages are not stored in the dataspace - they are delivered
  once to all matching observers and then discarded.

  ## Parameters

  - `dataspace` - The dataspace receiving the message
  - `message` - The message content (Preserves value)
  - `turn` - The current turn context

  ## Returns

  A tuple of `{dataspace, updated_turn}`.

  The turn will contain message actions to notify all observers whose patterns
  match the incoming message.
  """
  def on_message(dataspace, message, turn) do
    Dataspace.handle_message(dataspace, message, turn)
  end

  @doc """
  Called when a synchronization barrier is reached.

  The dataspace responds by sending a `Synced` message to the peer after
  all prior operations have been processed. This creates a happens-before
  relationship: once the peer receives the `Synced` message, it knows that
  the dataspace has processed the sync request within its turn.

  ## Parameters

  - `dataspace` - The dataspace being synchronized
  - `peer_ref` - Reference to the peer that initiated the sync
  - `turn` - The current turn context

  ## Returns

  A tuple of `{dataspace, updated_turn}`.

  The turn will contain a message action to notify the peer with `{:symbol, "synced"}`.
  """
  def on_sync(dataspace, peer_ref, turn) do
    # Respond to the peer with a Synced message
    # This is sent after the turn commits, ensuring the peer knows
    # all prior operations have been processed
    action = Event.message(peer_ref, {:symbol, "synced"})
    turn = Turn.add_action(turn, action)

    {dataspace, turn}
  end
end
