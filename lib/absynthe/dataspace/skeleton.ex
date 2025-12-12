defmodule Absynthe.Dataspace.Skeleton do
  @moduledoc """
  High-performance ETS-based index for efficient assertion lookup in the Syndicated Actor Model.

  The Skeleton is a core indexing structure that enables fast pattern matching against
  assertions without requiring a full scan. Instead of iterating through all assertions
  for each pattern, the Skeleton indexes assertions by their structural paths, allowing
  O(1) lookups for constrained patterns.

  ## Architecture

  The Skeleton uses three ETS tables with `:read_concurrency` enabled for parallel reads:

  1. **Path Index**: Maps `{path, value_at_path}` tuples to sets of assertion handles.
     This allows quick lookup of assertions by specific path values.

  2. **Assertion Store**: Maps assertion handles to their full values. This provides
     fast retrieval of complete assertions once handles are identified.

  3. **Observer Registry**: Maps observer IDs to their compiled patterns and observer
     references. This enables efficient notification when matching assertions change.

  ## Indexing Strategy

  When an assertion is added, the Skeleton extracts all indexable paths from its structure.
  For example, a record assertion like:

      {:record, {{:symbol, "Person"}, [{:string, "Alice"}, {:integer, 30}]}}

  Generates the following index entries:

      {[:label], {:symbol, "Person"}} -> MapSet containing handle
      {[:field, 0], {:string, "Alice"}} -> MapSet containing handle
      {[:field, 1], {:integer, 30}} -> MapSet containing handle

  This indexing allows patterns constrained to specific field values to quickly find
  matching assertions without scanning the entire assertion space.

  ## Query Algorithm

  When querying for assertions matching a pattern, the Skeleton:

  1. Extracts constraints from the compiled pattern (paths with concrete values)
  2. Looks up handles satisfying the most selective constraint
  3. Retrieves full assertions for candidate handles
  4. Performs full pattern matching to extract captures
  5. Returns matches with their captures

  ## Observer Notification

  The Skeleton tracks observers interested in specific patterns. When assertions are
  added or removed, it identifies which observers' patterns match and returns their
  references along with captured values. This enables efficient dataspace subscription.

  ## Concurrency

  All ETS tables are created with `:public` access and `:read_concurrency` enabled,
  allowing multiple processes to read simultaneously. Write operations (add/remove)
  are the caller's responsibility to synchronize if needed.

  ## Examples

      # Create a new skeleton
      skeleton = Absynthe.Dataspace.Skeleton.new()

      # Add an assertion
      handle = Absynthe.Assertions.Handle.new(:my_actor, 1)
      value = Absynthe.Preserves.Value.record(
        Absynthe.Preserves.Value.symbol("Person"),
        [
          Absynthe.Preserves.Value.string("Alice"),
          Absynthe.Preserves.Value.integer(30)
        ]
      )
      observers = Absynthe.Dataspace.Skeleton.add_assertion(skeleton, handle, value)

      # Query for matching assertions
      pattern = Absynthe.Preserves.Value.record(
        Absynthe.Preserves.Value.symbol("Person"),
        [:_, {:capture, :age}]
      )
      results = Absynthe.Dataspace.Skeleton.query(skeleton, pattern)

      # Clean up when done
      Absynthe.Dataspace.Skeleton.destroy(skeleton)

  """

  alias Absynthe.Assertions.Handle
  alias Absynthe.Preserves.Value
  alias Absynthe.Dataspace.Pattern

  @typedoc """
  Path into a Preserves value structure.

  Paths describe locations within compound values:
  - `[:label]` - The label of a record
  - `[:field, n]` - The nth field of a record
  - `[:element, n]` - The nth element of a sequence
  - `[:key, key]` - A specific key in a dictionary
  """
  @type path :: [atom() | non_neg_integer() | Value.t()]

  @typedoc """
  The Skeleton index structure.

  Contains:
  - `path_index`: ETS table mapping `{path, value}` to `MapSet.t(Handle.t())`
  - `assertions`: ETS table mapping `Handle.t()` to `Value.t()`
  - `observers`: ETS table mapping observer ID to `{Pattern.t(), observer_ref}`
  - `owner`: PID of the process that created the skeleton
  """
  @type t :: %__MODULE__{
          path_index: :ets.tid(),
          assertions: :ets.tid(),
          observers: :ets.tid(),
          owner: pid()
        }

  defstruct [:path_index, :assertions, :observers, :owner]

  @doc """
  Creates a new Skeleton index.

  Initializes three ETS tables with `:read_concurrency` enabled for efficient
  parallel reads. The tables are owned by the calling process.

  ## Returns

  A new Skeleton structure with initialized ETS tables.

  ## Examples

      iex> skeleton = Absynthe.Dataspace.Skeleton.new()
      iex> is_struct(skeleton, Absynthe.Dataspace.Skeleton)
      true

  """
  @spec new() :: t()
  def new do
    %__MODULE__{
      path_index: :ets.new(:path_index, [:set, :public, read_concurrency: true]),
      assertions: :ets.new(:assertions, [:set, :public, read_concurrency: true]),
      observers: :ets.new(:observers, [:set, :public, read_concurrency: true]),
      owner: self()
    }
  end

  @doc """
  Adds an assertion to the index with its handle.

  Extracts all indexable paths from the assertion value and creates index entries
  for each path-value pair. Also checks which observers match the new assertion
  and returns a list of notifications to send.

  ## Parameters

    - `skeleton` - The Skeleton index
    - `handle` - Unique handle identifying this assertion
    - `value` - The Preserves value being asserted

  ## Returns

  A list of tuples `{observer_ref, captures}` representing observers that should
  be notified about this new assertion, along with the captured values from the
  pattern match.

  ## Examples

      skeleton = Absynthe.Dataspace.Skeleton.new()
      handle = Absynthe.Assertions.Handle.new(:my_actor, 1)
      value = Absynthe.Preserves.Value.record(
        Absynthe.Preserves.Value.symbol("User"),
        [Absynthe.Preserves.Value.string("alice")]
      )
      notifications = Absynthe.Dataspace.Skeleton.add_assertion(skeleton, handle, value)

  """
  @spec add_assertion(t(), Handle.t(), Value.t()) ::
          [{observer_ref :: term(), Value.t(), captures :: [Value.t()]}]
  def add_assertion(%__MODULE__{} = skeleton, %Handle{} = handle, value) do
    # Store the full assertion
    :ets.insert(skeleton.assertions, {handle, value})

    # Extract and index all paths
    paths = extract_paths(value)

    Enum.each(paths, fn {path, path_value} ->
      key = {path, path_value}

      case :ets.lookup(skeleton.path_index, key) do
        [{^key, handle_set}] ->
          new_set = MapSet.put(handle_set, handle)
          :ets.insert(skeleton.path_index, {key, new_set})

        [] ->
          :ets.insert(skeleton.path_index, {key, MapSet.new([handle])})
      end
    end)

    # Find matching observers and return notifications
    find_matching_observers(skeleton, handle, value)
  end

  @doc """
  Removes an assertion from the index by its handle.

  Cleans up all path index entries associated with the assertion and removes
  the assertion from storage. Also checks which observers were matching this
  assertion and returns a list of notifications.

  ## Parameters

    - `skeleton` - The Skeleton index
    - `handle` - The handle of the assertion to remove

  ## Returns

  A list of tuples `{observer_ref, captures}` representing observers that should
  be notified about the removal of this assertion, along with the captured values
  from the pattern match.

  ## Examples

      skeleton = Absynthe.Dataspace.Skeleton.new()
      handle = Absynthe.Assertions.Handle.new(:my_actor, 1)
      # ... add assertion ...
      notifications = Absynthe.Dataspace.Skeleton.remove_assertion(skeleton, handle)

  """
  @spec remove_assertion(t(), Handle.t()) :: [observer_ref :: term()]
  def remove_assertion(%__MODULE__{} = skeleton, %Handle{} = handle) do
    # Get the assertion value before removing and find matching observers
    {value_for_cleanup, observer_refs} =
      case :ets.lookup(skeleton.assertions, handle) do
        [{^handle, value}] ->
          matches = find_matching_observers(skeleton, handle, value)
          # Extract just observer refs for retraction notifications
          refs = Enum.map(matches, fn {observer_ref, _value, _captures} -> observer_ref end)
          {value, refs}

        [] ->
          {nil, []}
      end

    # Remove from assertions table
    :ets.delete(skeleton.assertions, handle)

    # Clean up path indices
    if value_for_cleanup do
      remove_from_path_index(skeleton, handle, value_for_cleanup)
    else
      # If we can't find it, we need to scan the entire path index
      # This is expensive but necessary for cleanup
      cleanup_handle_from_index(skeleton, handle)
    end

    observer_refs
  end

  @doc """
  Registers an observer with a compiled pattern.

  Stores the observer's pattern and reference, then returns all existing assertions
  that match the pattern along with their captured values.

  ## Parameters

    - `skeleton` - The Skeleton index
    - `observer_id` - Unique identifier for this observer
    - `observer_ref` - Reference to send to the observer (opaque term)
    - `pattern` - Compiled pattern to match against assertions

  ## Returns

  A list of tuples `{handle, captures}` representing existing assertions that
  match the pattern, along with their captured values.

  ## Examples

      skeleton = Absynthe.Dataspace.Skeleton.new()
      pattern = Absynthe.Preserves.Value.record(
        Absynthe.Preserves.Value.symbol("User"),
        [{:capture, :name}]
      )
      matches = Absynthe.Dataspace.Skeleton.add_observer(
        skeleton,
        :observer_1,
        self(),
        pattern
      )

  """
  @spec add_observer(t(), observer_id :: term(), observer_ref :: term(), Pattern.t()) ::
          [{Handle.t(), Value.t(), captures :: [Value.t()]}]
  def add_observer(%__MODULE__{} = skeleton, observer_id, observer_ref, pattern) do
    # Store the observer
    :ets.insert(skeleton.observers, {observer_id, {pattern, observer_ref}})

    # Find all existing matching assertions - query returns {handle, value, captures}
    query(skeleton, pattern)
  end

  @doc """
  Unregisters an observer.

  Removes the observer from the registry. The observer will no longer receive
  notifications about assertion changes.

  ## Parameters

    - `skeleton` - The Skeleton index
    - `observer_id` - The identifier of the observer to remove

  ## Returns

  `:ok`

  ## Examples

      skeleton = Absynthe.Dataspace.Skeleton.new()
      # ... add observer ...
      :ok = Absynthe.Dataspace.Skeleton.remove_observer(skeleton, :observer_1)

  """
  @spec remove_observer(t(), observer_id :: term()) :: :ok
  def remove_observer(%__MODULE__{} = skeleton, observer_id) do
    :ets.delete(skeleton.observers, observer_id)
    :ok
  end

  @doc """
  Queries for all assertions matching a pattern.

  Uses the path index to efficiently find candidate assertions, then performs
  full pattern matching to extract captures.

  ## Parameters

    - `skeleton` - The Skeleton index
    - `pattern` - Pattern to match against assertions

  ## Returns

  A list of tuples `{handle, value, captures}` where:
  - `handle` is the assertion's unique handle
  - `value` is the full assertion value
  - `captures` is a list of captured values in the order they appear in the pattern

  ## Examples

      skeleton = Absynthe.Dataspace.Skeleton.new()
      pattern = Absynthe.Preserves.Value.record(
        Absynthe.Preserves.Value.symbol("Person"),
        [:_, {:capture, :age}]
      )
      results = Absynthe.Dataspace.Skeleton.query(skeleton, pattern)

  """
  @spec query(t(), Pattern.t()) ::
          [{Handle.t(), Value.t(), captures :: [Value.t()]}]
  def query(%__MODULE__{} = skeleton, %Pattern{} = compiled_pattern) do
    # Extract constraints from the compiled pattern
    index_paths = Pattern.index_paths(compiled_pattern)

    # Get candidate handles using the most selective constraint
    candidate_handles =
      case index_paths do
        [] ->
          # No constraints - must check all assertions
          :ets.tab2list(skeleton.assertions)
          |> Enum.map(fn {handle, _value} -> handle end)

        constraints ->
          # Find the most selective constraint (smallest handle set)
          find_most_selective_candidates(skeleton, constraints)
      end

    # For each candidate, retrieve the full value and perform pattern matching
    candidate_handles
    |> Enum.flat_map(fn handle ->
      case :ets.lookup(skeleton.assertions, handle) do
        [{^handle, value}] ->
          case Pattern.match(compiled_pattern, value) do
            {:ok, captures} ->
              [{handle, value, captures}]

            :no_match ->
              []
          end

        [] ->
          []
      end
    end)
  end

  # Finds candidates using the most selective (smallest) index
  defp find_most_selective_candidates(skeleton, constraints) do
    # Look up all constraints and find the one with the smallest result set
    candidates_by_constraint =
      constraints
      |> Enum.map(fn {path, expected} ->
        lookup_key = {path, expected}

        case :ets.lookup(skeleton.path_index, lookup_key) do
          [{^lookup_key, handle_set}] -> {MapSet.size(handle_set), handle_set}
          [] -> {0, MapSet.new()}
        end
      end)

    case candidates_by_constraint do
      [] ->
        []

      results ->
        # Pick the constraint with the smallest non-empty result set
        # If all are empty, return empty list
        non_empty = Enum.filter(results, fn {size, _set} -> size > 0 end)

        case non_empty do
          [] ->
            # All constraints returned empty - no matches possible
            []

          _ ->
            # Pick the smallest (most selective)
            {_size, best_set} = Enum.min_by(non_empty, fn {size, _set} -> size end)
            MapSet.to_list(best_set)
        end
    end
  end

  @doc """
  Destroys the Skeleton and cleans up all ETS tables.

  This should be called when the Skeleton is no longer needed to free resources.

  ## Parameters

    - `skeleton` - The Skeleton index to destroy

  ## Returns

  `:ok`

  ## Examples

      skeleton = Absynthe.Dataspace.Skeleton.new()
      # ... use skeleton ...
      :ok = Absynthe.Dataspace.Skeleton.destroy(skeleton)

  """
  @spec destroy(t()) :: :ok
  def destroy(%__MODULE__{} = skeleton) do
    :ets.delete(skeleton.path_index)
    :ets.delete(skeleton.assertions)
    :ets.delete(skeleton.observers)
    :ok
  end

  # Private Helper Functions

  # Extracts all indexable paths from a value
  @spec extract_paths(Value.t()) :: [{path(), Value.t()}]
  defp extract_paths(value), do: extract_paths(value, [])

  @spec extract_paths(Value.t(), path()) :: [{path(), Value.t()}]
  defp extract_paths({:record, {label, fields}}, path) do
    label_paths = [{path ++ [:label], label} | extract_paths(label, path ++ [:label])]

    field_paths =
      fields
      |> Enum.with_index()
      |> Enum.flat_map(fn {field, idx} ->
        field_path = path ++ [:field, idx]
        [{field_path, field} | extract_paths(field, field_path)]
      end)

    label_paths ++ field_paths
  end

  defp extract_paths({:sequence, items}, path) do
    items
    |> Enum.with_index()
    |> Enum.flat_map(fn {item, idx} ->
      item_path = path ++ [:element, idx]
      [{item_path, item} | extract_paths(item, item_path)]
    end)
  end

  defp extract_paths({:set, items}, path) do
    items
    |> Enum.flat_map(fn item ->
      # For sets, we can't use positional indices, so we index by the value itself
      item_path = path ++ [:member, item]
      [{item_path, item} | extract_paths(item, item_path)]
    end)
  end

  defp extract_paths({:dictionary, map}, path) do
    map
    |> Enum.flat_map(fn {key, val} ->
      key_path = path ++ [:key, key]
      val_path = path ++ [:value, key]

      key_paths = [{key_path, key} | extract_paths(key, key_path)]
      val_paths = [{val_path, val} | extract_paths(val, val_path)]

      key_paths ++ val_paths
    end)
  end

  # Atomic values don't have nested paths
  defp extract_paths(_atomic, _path), do: []

  # Removes assertion handle from path index
  defp remove_from_path_index(%__MODULE__{} = skeleton, %Handle{} = handle, value) do
    paths = extract_paths(value)

    Enum.each(paths, fn {path, path_value} ->
      key = {path, path_value}

      case :ets.lookup(skeleton.path_index, key) do
        [{^key, handle_set}] ->
          new_set = MapSet.delete(handle_set, handle)

          if MapSet.size(new_set) == 0 do
            :ets.delete(skeleton.path_index, key)
          else
            :ets.insert(skeleton.path_index, {key, new_set})
          end

        [] ->
          :ok
      end
    end)
  end

  # Cleanup handle from entire index (expensive fallback)
  defp cleanup_handle_from_index(%__MODULE__{} = skeleton, %Handle{} = handle) do
    :ets.foldl(
      fn {key, handle_set}, _acc ->
        if MapSet.member?(handle_set, handle) do
          new_set = MapSet.delete(handle_set, handle)

          if MapSet.size(new_set) == 0 do
            :ets.delete(skeleton.path_index, key)
          else
            :ets.insert(skeleton.path_index, {key, new_set})
          end
        end

        :ok
      end,
      :ok,
      skeleton.path_index
    )
  end

  # Find observers that match a given assertion
  @spec find_matching_observers(t(), Handle.t(), Value.t()) ::
          [{observer_ref :: term(), Value.t(), captures :: [Value.t()]}]
  defp find_matching_observers(%__MODULE__{} = skeleton, _handle, value) do
    :ets.tab2list(skeleton.observers)
    |> Enum.flat_map(fn {_observer_id, {pattern, observer_ref}} ->
      case Pattern.match(pattern, value) do
        {:ok, captures} ->
          [{observer_ref, value, captures}]

        :no_match ->
          []
      end
    end)
  end
end
