defmodule Absynthe.Dataspace.Observer do
  @moduledoc """
  Subscription tracking and notification delivery for the Syndicated Actor Model.

  Observers represent subscriptions to assertion patterns in a dataspace. When
  assertions matching an observer's pattern are added or removed from the
  dataspace, the observer's entity receives callbacks notifying it of these
  changes.

  ## Purpose

  In the Syndicated Actor Model, entities subscribe to patterns of interest by
  asserting an `Observe` assertion into the dataspace. The dataspace creates an
  Observer struct to track this subscription, including:

  - The pattern to match against assertions
  - The entity reference to notify when matches occur
  - The set of currently matched assertion handles

  When assertions are added or removed from the dataspace, the pattern matching
  infrastructure identifies which observers are affected, and the dataspace
  uses this module to track the match state and determine what notifications
  to send.

  ## Key Concepts

  ### Observe Assertion

  A subscription is itself an assertion: `<Observe pattern entity_ref>`. This
  means subscriptions are managed using the same assertion/retraction mechanisms
  as regular data, providing uniformity and composability.

  ### Pattern Matching

  Observers specify patterns using the `Absynthe.Preserves.Pattern` syntax,
  which supports:
  - Literal value matching
  - Wildcards (`:_`) that match anything
  - Captures (`{:capture, name}`) that extract values
  - Compound patterns for records, sequences, sets, and dictionaries

  ### Callbacks

  When pattern matches change, the observer's entity receives event callbacks:
  - `Assert` event when an assertion matching the pattern appears (on_publish)
  - `Retract` event when a matching assertion disappears (on_retract)

  ### Captures

  Pattern captures are delivered with Assert events, allowing entities to
  extract and process specific values from matched assertions.

  ## Usage Flow

  1. Entity asserts `<Observe <Person _ $age> my_ref>` into dataspace
  2. Dataspace parses the Observe assertion and creates Observer struct
  3. Dataspace compiles the pattern and registers observer in Skeleton index
  4. When `<Person "Alice" 30>` is asserted:
     - Skeleton finds matching observers via pattern index
     - Dataspace calls `Observer.add_match/2` to track the handle
     - Dataspace sends Assert event to observer's entity_ref with captures `%{age: {:integer, 30}}`
  5. When `<Person "Alice" 30>` is retracted:
     - Skeleton notifies affected observers
     - Dataspace calls `Observer.remove_match/2` to untrack the handle
     - Dataspace sends Retract event to observer's entity_ref

  ## Observe Assertion Format

  The standard format for Observe assertions in Preserves syntax:

      # <Observe pattern entity_ref>
      {:record, {{:symbol, "Observe"}, [pattern_value, embedded_ref]}}

  Where:
  - `pattern_value` is a Preserves value representing the pattern (may contain `:_` or `{:capture, name}`)
  - `embedded_ref` is an `{:embedded, ref}` value containing the entity reference

  ## Integration with Turn/Entity

  When notifying observers, the dataspace constructs events and adds them
  as actions to the current turn. Captured bindings from pattern matching
  are included in the Assert event:

      # On assertion match - captures are included in the event
      event = %Absynthe.Protocol.Event.Assert{
        ref: observer.entity_ref,
        assertion: assertion_value,
        handle: assertion_handle,
        captures: [{:integer, 30}]  # captured values in pattern order
      }
      turn = Absynthe.Core.Turn.add_action(turn, event)

      # On assertion removal
      event = %Absynthe.Protocol.Event.Retract{
        ref: observer.entity_ref,
        handle: assertion_handle
      }
      turn = Absynthe.Core.Turn.add_action(turn, event)

  ## Examples

      # Create an observer
      pattern = {:record, {{:symbol, "Person"}, [:_, {:capture, :age}]}}
      entity_ref = %Absynthe.Core.Ref{actor_id: :actor1, entity_id: :entity1}
      observer = Absynthe.Dataspace.Observer.new(:obs1, pattern, entity_ref)

      # Track matches
      handle = %Absynthe.Assertions.Handle{id: 42}
      observer = Absynthe.Dataspace.Observer.add_match(observer, handle)

      # Check if handle is matched
      Absynthe.Dataspace.Observer.matches?(observer, handle)  # => true

      # Get match count
      Absynthe.Dataspace.Observer.match_count(observer)  # => 1

      # Remove match
      observer = Absynthe.Dataspace.Observer.remove_match(observer, handle)
      Absynthe.Dataspace.Observer.match_count(observer)  # => 0

      # Convert to Observe assertion
      observe_assertion = Absynthe.Dataspace.Observer.to_assertion(observer)
      # => {:record, {{:symbol, "Observe"}, [pattern, {:embedded, entity_ref}]}}

      # Parse Observe assertion
      {:ok, {pattern, entity_ref}} = Absynthe.Dataspace.Observer.from_assertion(observe_assertion)
  """

  alias Absynthe.Preserves.Pattern
  alias Absynthe.Preserves.Value
  alias Absynthe.Core.Ref
  alias Absynthe.Assertions.Handle

  @typedoc """
  Observer struct tracking a pattern subscription.

  Fields:
  - `id` - Unique identifier for this observer (typically generated by dataspace)
  - `pattern` - The compiled pattern to match assertions against
  - `entity_ref` - Reference to the entity that should be notified of matches
  - `active_handles` - Set of assertion handles currently matched by this observer
  - `created_at` - Timestamp when the observer was created
  """
  @type t :: %__MODULE__{
          id: term(),
          pattern: Pattern.pattern(),
          entity_ref: Ref.t(),
          active_handles: MapSet.t(Handle.t()),
          created_at: DateTime.t()
        }

  @enforce_keys [:id, :pattern, :entity_ref]
  defstruct [:id, :pattern, :entity_ref, active_handles: MapSet.new(), created_at: nil]

  # Constructor

  @doc """
  Creates a new observer with the given ID, pattern, and entity reference.

  The observer begins with no active matches and a creation timestamp set to
  the current time.

  ## Parameters

  - `id` - Unique identifier for this observer
  - `pattern` - Pattern to match assertions against (using `Absynthe.Preserves.Pattern` syntax)
  - `entity_ref` - Reference to the entity that receives match notifications

  ## Returns

  A new `Observer` struct.

  ## Examples

      iex> pattern = {:capture, :x}
      iex> ref = %Absynthe.Core.Ref{actor_id: :a1, entity_id: :e1}
      iex> observer = Absynthe.Dataspace.Observer.new(:obs1, pattern, ref)
      iex> observer.id
      :obs1
      iex> observer.pattern
      {:capture, :x}
      iex> observer.entity_ref
      %Absynthe.Core.Ref{actor_id: :a1, entity_id: :e1, attenuation: nil}
      iex> Absynthe.Dataspace.Observer.match_count(observer)
      0
  """
  @spec new(term(), Pattern.pattern(), Ref.t()) :: t()
  def new(id, pattern, entity_ref) do
    %__MODULE__{
      id: id,
      pattern: pattern,
      entity_ref: entity_ref,
      active_handles: MapSet.new(),
      created_at: DateTime.utc_now()
    }
  end

  # Match Tracking

  @doc """
  Records that an assertion handle is now matched by this observer.

  If the handle is already in the active set, this is a no-op (idempotent).

  ## Parameters

  - `observer` - The observer to update
  - `handle` - The assertion handle to add to the active set

  ## Returns

  Updated `Observer` struct with the handle added to active matches.

  ## Examples

      iex> observer = Absynthe.Dataspace.Observer.new(:obs1, :_, ref)
      iex> handle = %Absynthe.Assertions.Handle{id: 1}
      iex> observer = Absynthe.Dataspace.Observer.add_match(observer, handle)
      iex> Absynthe.Dataspace.Observer.matches?(observer, handle)
      true
      iex> Absynthe.Dataspace.Observer.match_count(observer)
      1

      # Adding the same handle again is idempotent
      iex> observer = Absynthe.Dataspace.Observer.add_match(observer, handle)
      iex> Absynthe.Dataspace.Observer.match_count(observer)
      1
  """
  @spec add_match(t(), Handle.t()) :: t()
  def add_match(%__MODULE__{active_handles: handles} = observer, %Handle{} = handle) do
    %__MODULE__{observer | active_handles: MapSet.put(handles, handle)}
  end

  @doc """
  Records that an assertion handle is no longer matched by this observer.

  If the handle is not in the active set, this is a no-op (idempotent).

  ## Parameters

  - `observer` - The observer to update
  - `handle` - The assertion handle to remove from the active set

  ## Returns

  Updated `Observer` struct with the handle removed from active matches.

  ## Examples

      iex> observer = Absynthe.Dataspace.Observer.new(:obs1, :_, ref)
      iex> handle = %Absynthe.Assertions.Handle{id: 1}
      iex> observer = Absynthe.Dataspace.Observer.add_match(observer, handle)
      iex> Absynthe.Dataspace.Observer.match_count(observer)
      1
      iex> observer = Absynthe.Dataspace.Observer.remove_match(observer, handle)
      iex> Absynthe.Dataspace.Observer.match_count(observer)
      0
      iex> Absynthe.Dataspace.Observer.matches?(observer, handle)
      false

      # Removing a handle that doesn't exist is idempotent
      iex> observer = Absynthe.Dataspace.Observer.remove_match(observer, handle)
      iex> Absynthe.Dataspace.Observer.match_count(observer)
      0
  """
  @spec remove_match(t(), Handle.t()) :: t()
  def remove_match(%__MODULE__{active_handles: handles} = observer, %Handle{} = handle) do
    %__MODULE__{observer | active_handles: MapSet.delete(handles, handle)}
  end

  @doc """
  Checks if a handle is currently matched by this observer.

  ## Parameters

  - `observer` - The observer to check
  - `handle` - The assertion handle to look for

  ## Returns

  `true` if the handle is in the active set, `false` otherwise.

  ## Examples

      iex> observer = Absynthe.Dataspace.Observer.new(:obs1, :_, ref)
      iex> handle = %Absynthe.Assertions.Handle{id: 1}
      iex> Absynthe.Dataspace.Observer.matches?(observer, handle)
      false
      iex> observer = Absynthe.Dataspace.Observer.add_match(observer, handle)
      iex> Absynthe.Dataspace.Observer.matches?(observer, handle)
      true
  """
  @spec matches?(t(), Handle.t()) :: boolean()
  def matches?(%__MODULE__{active_handles: handles}, %Handle{} = handle) do
    MapSet.member?(handles, handle)
  end

  @doc """
  Returns all currently matched handles for this observer.

  The returned set can be used to iterate over all active matches or to
  perform set operations.

  ## Parameters

  - `observer` - The observer to query

  ## Returns

  A `MapSet` of `Handle.t()` representing all currently matched assertions.

  ## Examples

      iex> observer = Absynthe.Dataspace.Observer.new(:obs1, :_, ref)
      iex> handle1 = %Absynthe.Assertions.Handle{id: 1}
      iex> handle2 = %Absynthe.Assertions.Handle{id: 2}
      iex> observer = observer
      ...>   |> Absynthe.Dataspace.Observer.add_match(handle1)
      ...>   |> Absynthe.Dataspace.Observer.add_match(handle2)
      iex> handles = Absynthe.Dataspace.Observer.matched_handles(observer)
      iex> MapSet.size(handles)
      2
      iex> MapSet.member?(handles, handle1)
      true
  """
  @spec matched_handles(t()) :: MapSet.t(Handle.t())
  def matched_handles(%__MODULE__{active_handles: handles}) do
    handles
  end

  @doc """
  Returns the number of currently matched handles.

  ## Parameters

  - `observer` - The observer to query

  ## Returns

  A non-negative integer representing the number of active matches.

  ## Examples

      iex> observer = Absynthe.Dataspace.Observer.new(:obs1, :_, ref)
      iex> Absynthe.Dataspace.Observer.match_count(observer)
      0
      iex> handle1 = %Absynthe.Assertions.Handle{id: 1}
      iex> handle2 = %Absynthe.Assertions.Handle{id: 2}
      iex> observer = observer
      ...>   |> Absynthe.Dataspace.Observer.add_match(handle1)
      ...>   |> Absynthe.Dataspace.Observer.add_match(handle2)
      iex> Absynthe.Dataspace.Observer.match_count(observer)
      2
  """
  @spec match_count(t()) :: non_neg_integer()
  def match_count(%__MODULE__{active_handles: handles}) do
    MapSet.size(handles)
  end

  # Assertion Conversion

  @doc """
  Creates an Observe assertion value for this observer.

  The Observe assertion has the standard format:
  `<Observe pattern entity_ref>` which is represented as:
  `{:record, {{:symbol, "Observe"}, [pattern, {:embedded, entity_ref}]}}`

  This assertion can be used to recreate the observer or to serialize it
  for transmission across actor boundaries.

  ## Parameters

  - `observer` - The observer to convert to an assertion

  ## Returns

  A Preserves `Value.t()` representing the Observe assertion.

  ## Examples

      iex> pattern = {:capture, :x}
      iex> ref = %Absynthe.Core.Ref{actor_id: :a1, entity_id: :e1}
      iex> observer = Absynthe.Dataspace.Observer.new(:obs1, pattern, ref)
      iex> assertion = Absynthe.Dataspace.Observer.to_assertion(observer)
      iex> {:record, {{:symbol, "Observe"}, [p, {:embedded, r}]}} = assertion
      iex> p
      {:capture, :x}
      iex> r
      %Absynthe.Core.Ref{actor_id: :a1, entity_id: :e1, attenuation: nil}
  """
  @spec to_assertion(t()) :: Value.t()
  def to_assertion(%__MODULE__{pattern: pattern, entity_ref: entity_ref}) do
    Value.record(
      Value.symbol("Observe"),
      [pattern, Value.embedded(entity_ref)]
    )
  end

  @doc """
  Parses an Observe assertion to extract the pattern and entity reference.

  This function validates that the assertion has the correct structure:
  - Must be a record
  - Label must be the symbol "Observe"
  - Must have exactly 2 fields
  - Second field must be an embedded value

  ## Parameters

  - `value` - A Preserves value that may be an Observe assertion

  ## Returns

  - `{:ok, {pattern, entity_ref}}` if the value is a valid Observe assertion
  - `:error` if the value is not a valid Observe assertion

  ## Examples

      # Valid Observe assertion
      iex> pattern = {:capture, :x}
      iex> ref = %Absynthe.Core.Ref{actor_id: :a1, entity_id: :e1}
      iex> assertion = Absynthe.Preserves.Value.record(
      ...>   Absynthe.Preserves.Value.symbol("Observe"),
      ...>   [pattern, Absynthe.Preserves.Value.embedded(ref)]
      ...> )
      iex> {:ok, {p, r}} = Absynthe.Dataspace.Observer.from_assertion(assertion)
      iex> p
      {:capture, :x}
      iex> r
      %Absynthe.Core.Ref{actor_id: :a1, entity_id: :e1, attenuation: nil}

      # Invalid: wrong label
      iex> assertion = Absynthe.Preserves.Value.record(
      ...>   Absynthe.Preserves.Value.symbol("NotObserve"),
      ...>   [pattern, Absynthe.Preserves.Value.embedded(ref)]
      ...> )
      iex> Absynthe.Dataspace.Observer.from_assertion(assertion)
      :error

      # Invalid: wrong arity
      iex> assertion = Absynthe.Preserves.Value.record(
      ...>   Absynthe.Preserves.Value.symbol("Observe"),
      ...>   [pattern]
      ...> )
      iex> Absynthe.Dataspace.Observer.from_assertion(assertion)
      :error

      # Invalid: not a record
      iex> Absynthe.Dataspace.Observer.from_assertion({:string, "not a record"})
      :error
  """
  @spec from_assertion(Value.t()) ::
          {:ok, {pattern :: Value.t(), entity_ref :: term()}} | :error
  def from_assertion({:record, {{:symbol, "Observe"}, [pattern, {:embedded, entity_ref}]}}) do
    {:ok, {pattern, entity_ref}}
  end

  def from_assertion(_), do: :error
end
