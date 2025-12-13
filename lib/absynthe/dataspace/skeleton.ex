defmodule Absynthe.Dataspace.Skeleton do
  @moduledoc """
  High-performance ETS-based index for efficient assertion lookup in the Syndicated Actor Model.

  The Skeleton is a core indexing structure that enables fast pattern matching against
  assertions without requiring a full scan. Instead of iterating through all assertions
  for each pattern, the Skeleton indexes assertions by their structural paths, allowing
  O(1) lookups for constrained patterns.

  ## Architecture

  The Skeleton uses four ETS tables with `:read_concurrency` enabled for parallel reads:

  1. **Path Index**: Maps `{path, value_at_path}` tuples to sets of assertion handles.
     This allows quick lookup of assertions by specific path values.

  2. **Assertion Store**: Maps assertion handles to their full values. This provides
     fast retrieval of complete assertions once handles are identified.

  3. **Observer Registry**: Maps observer IDs to their compiled patterns and observer
     references. This enables efficient notification when matching assertions change.

  4. **Observer Index**: Maps `{path, value_class}` tuples to sets of `{observer_id, exact_value}`
     pairs. This allows O(matching observers) lookup instead of scanning all observers.

  ## Indexing Strategy

  When an assertion is added, the Skeleton extracts all indexable paths from its structure.
  For example, a record assertion like:

      {:record, {{:symbol, "Person"}, [{:string, "Alice"}, {:integer, 30}]}}

  Generates the following index entries:

      {[:label], {:symbol, "Person"}} -> MapSet containing handle
      {[{:field, 0}], {:string, "Alice"}} -> MapSet containing handle
      {[{:field, 1}], {:integer, 30}} -> MapSet containing handle

  Path elements are tuples to match the compiled Pattern format:
  - `[:label]` for record labels
  - `[{:field, n}]` for record fields
  - `[n]` (bare integer) for sequence elements
  - `[{:key, k}]` for dictionary values

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

  **Important**: The `wildcard_observers` set is stored in the struct, not in ETS.
  Functions `add_observer/4` and `remove_observer/2` return an updated skeleton struct
  that MUST be used for subsequent operations. Using a stale skeleton will cause
  wildcard observers to be missed during notification. This design assumes a single
  owner process (typically the dataspace actor) manages the skeleton.

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

  Paths describe locations within compound values using elements that match
  the compiled Pattern format:
  - `[:label]` - The label of a record
  - `[{:field, n}]` - The nth field of a record (tuple)
  - `[n]` - The nth element of a sequence (bare integer)
  - `[{:key, key}]` - A specific key in a dictionary (tuple)
  """
  @type path :: [atom() | non_neg_integer() | {:field, non_neg_integer()} | {:key, Value.t()}]

  @typedoc """
  The Skeleton index structure.

  Contains:
  - `path_index`: ETS table mapping `{path, value}` to `MapSet.t(Handle.t())`
  - `assertions`: ETS table mapping `Handle.t()` to `Value.t()`
  - `observers`: ETS table mapping observer ID to `{Pattern.t(), observer_ref}`
  - `observer_index`: ETS table mapping `{path, value_class}` to `MapSet.t({observer_id, exact_value | nil})`
    where value_class is an atom like `:symbol`, `:integer`, etc. and exact_value is the
    constraint value (or nil for wildcard constraints at that path)
  - `wildcard_observers`: Set of observer IDs with unconstrained patterns (match everything)
  - `owner`: PID of the process that created the skeleton
  """
  @type t :: %__MODULE__{
          path_index: :ets.tid(),
          assertions: :ets.tid(),
          observers: :ets.tid(),
          observer_index: :ets.tid(),
          wildcard_observers: MapSet.t(term()),
          owner: pid()
        }

  defstruct [
    :path_index,
    :assertions,
    :observers,
    :observer_index,
    wildcard_observers: MapSet.new(),
    owner: nil
  ]

  @doc """
  Creates a new Skeleton index.

  Initializes four ETS tables with `:read_concurrency` enabled for efficient
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
      observer_index: :ets.new(:observer_index, [:set, :public, read_concurrency: true]),
      wildcard_observers: MapSet.new(),
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

  A list of tuples `{observer_id, observer_ref, value, captures}` where:
  - `observer_id` - The unique identifier for the observer (used for lookup in dataspace)
  - `observer_ref` - The entity reference to notify
  - `value` - The matched assertion value
  - `captures` - Values captured by pattern variables

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
          [{observer_id :: term(), observer_ref :: term(), Value.t(), captures :: [Value.t()]}]
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

  Note: If the assertion was added multiple times with different handles (via
  reference counting in the Bag), use `remove_assertion_by_value/2` instead
  to ensure the correct handle is removed.

  ## Parameters

    - `skeleton` - The Skeleton index
    - `handle` - The handle of the assertion to remove

  ## Returns

  A list of tuples `{observer_id, observer_ref}` where:
  - `observer_id` - The unique identifier for the observer (used for lookup in dataspace)
  - `observer_ref` - The entity reference to notify about the retraction

  ## Examples

      skeleton = Absynthe.Dataspace.Skeleton.new()
      handle = Absynthe.Assertions.Handle.new(:my_actor, 1)
      # ... add assertion ...
      notifications = Absynthe.Dataspace.Skeleton.remove_assertion(skeleton, handle)

  """
  @spec remove_assertion(t(), Handle.t()) :: [{observer_id :: term(), observer_ref :: term()}]
  def remove_assertion(%__MODULE__{} = skeleton, %Handle{} = handle) do
    # Get the assertion value before removing and find matching observers
    {value_for_cleanup, observer_info} =
      case :ets.lookup(skeleton.assertions, handle) do
        [{^handle, value}] ->
          matches = find_matching_observers(skeleton, handle, value)
          # Extract observer_id and observer_ref pairs for retraction notifications
          info =
            Enum.map(matches, fn {observer_id, observer_ref, _value, _captures} ->
              {observer_id, observer_ref}
            end)

          {value, info}

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

    observer_info
  end

  @doc """
  Removes an assertion from the index by its value.

  This function is used when the Bag tracks multiple handles for the same assertion
  value, but the Skeleton only knows about the first (canonical) handle. When the
  last handle is removed from the Bag, this function finds and removes the canonical
  handle from the Skeleton.

  ## Parameters

    - `skeleton` - The Skeleton index
    - `value` - The assertion value to remove

  ## Returns

  A tuple `{canonical_handle, observer_info}` where:
  - `canonical_handle` - The handle that was stored in the skeleton for this value,
    or `nil` if not found
  - `observer_info` - List of `{observer_id, observer_ref}` tuples for observers
    that need retraction notifications

  ## Examples

      skeleton = Absynthe.Dataspace.Skeleton.new()
      handle = Absynthe.Assertions.Handle.new(:my_actor, 1)
      value = Absynthe.Preserves.Value.string("test")
      # ... add assertion ...
      {canonical_handle, notifications} =
        Absynthe.Dataspace.Skeleton.remove_assertion_by_value(skeleton, value)

  """
  @spec remove_assertion_by_value(t(), Value.t()) ::
          {Handle.t() | nil, [{observer_id :: term(), observer_ref :: term()}]}
  def remove_assertion_by_value(%__MODULE__{} = skeleton, value) do
    # Find the canonical handle for this value by scanning the assertions table
    canonical_handle = find_handle_for_value(skeleton, value)

    case canonical_handle do
      nil ->
        {nil, []}

      handle ->
        observer_info = remove_assertion(skeleton, handle)
        {handle, observer_info}
    end
  end

  # Finds the handle stored in the skeleton for a given assertion value
  defp find_handle_for_value(%__MODULE__{} = skeleton, value) do
    :ets.foldl(
      fn
        {handle, ^value}, _acc -> throw({:found, handle})
        _, acc -> acc
      end,
      nil,
      skeleton.assertions
    )
  catch
    {:found, handle} -> handle
  end

  @doc """
  Registers an observer with a compiled pattern.

  Stores the observer's pattern and reference, indexes the observer by its
  constraints for efficient lookup, then returns all existing assertions
  that match the pattern along with their captured values.

  ## Parameters

    - `skeleton` - The Skeleton index
    - `observer_id` - Unique identifier for this observer
    - `observer_ref` - Reference to send to the observer (opaque term)
    - `pattern` - Compiled pattern to match against assertions

  ## Returns

  A tuple `{updated_skeleton, matches}` where:
  - `updated_skeleton` - The skeleton with the observer indexed
  - `matches` - A list of tuples `{handle, value, captures}` representing existing
    assertions that match the pattern, along with their captured values.

  ## Examples

      skeleton = Absynthe.Dataspace.Skeleton.new()
      pattern = Absynthe.Preserves.Value.record(
        Absynthe.Preserves.Value.symbol("User"),
        [{:capture, :name}]
      )
      {updated_skeleton, matches} = Absynthe.Dataspace.Skeleton.add_observer(
        skeleton,
        :observer_1,
        self(),
        pattern
      )

  """
  @spec add_observer(t(), observer_id :: term(), observer_ref :: term(), Pattern.t()) ::
          {t(), [{Handle.t(), Value.t(), captures :: [Value.t()]}]}
  def add_observer(%__MODULE__{} = skeleton, observer_id, observer_ref, pattern) do
    # Store the observer
    :ets.insert(skeleton.observers, {observer_id, {pattern, observer_ref}})

    # Index the observer by its constraints for efficient lookup
    skeleton = index_observer(skeleton, observer_id, pattern)

    # Find all existing matching assertions - query returns {handle, value, captures}
    {skeleton, query(skeleton, pattern)}
  end

  @doc """
  Unregisters an observer.

  Removes the observer from the registry and from the observer index.
  The observer will no longer receive notifications about assertion changes.

  ## Parameters

    - `skeleton` - The Skeleton index
    - `observer_id` - The identifier of the observer to remove

  ## Returns

  The updated skeleton with the observer removed from all indices.

  ## Examples

      skeleton = Absynthe.Dataspace.Skeleton.new()
      # ... add observer ...
      updated_skeleton = Absynthe.Dataspace.Skeleton.remove_observer(skeleton, :observer_1)

  """
  @spec remove_observer(t(), observer_id :: term()) :: t()
  def remove_observer(%__MODULE__{} = skeleton, observer_id) do
    # Get pattern before deleting to know what to unindex
    skeleton =
      case :ets.lookup(skeleton.observers, observer_id) do
        [{^observer_id, {pattern, _observer_ref}}] ->
          unindex_observer(skeleton, observer_id, pattern)

        [] ->
          skeleton
      end

    :ets.delete(skeleton.observers, observer_id)
    skeleton
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
    :ets.delete(skeleton.observer_index)
    :ok
  end

  # Private Helper Functions

  # Extracts all indexable paths from a value.
  # IMPORTANT: Path format must match Pattern.compile_pattern/2:
  # - Records: [:label] for label, [{:field, index}] for fields
  # - Sequences: [index] (bare integer)
  # - Dictionaries: [{:key, key}] for values
  # - Sets: [index] (sorted canonical order)
  @spec extract_paths(Value.t()) :: [{path(), Value.t()}]
  defp extract_paths(value), do: extract_paths(value, [])

  @spec extract_paths(Value.t(), path()) :: [{path(), Value.t()}]
  defp extract_paths({:record, {label, fields}}, path) do
    # Label uses [:label] atom
    label_path = path ++ [:label]
    label_paths = [{label_path, label} | extract_paths(label, label_path)]

    # Fields use [{:field, index}] tuples (matching Pattern.compile_pattern)
    field_paths =
      fields
      |> Enum.with_index()
      |> Enum.flat_map(fn {field, idx} ->
        field_path = path ++ [{:field, idx}]
        [{field_path, field} | extract_paths(field, field_path)]
      end)

    label_paths ++ field_paths
  end

  defp extract_paths({:sequence, items}, path) do
    # Sequences use bare integer indices (matching Pattern.compile_pattern)
    items
    |> Enum.with_index()
    |> Enum.flat_map(fn {item, idx} ->
      item_path = path ++ [idx]
      [{item_path, item} | extract_paths(item, item_path)]
    end)
  end

  defp extract_paths({:set, items}, path) do
    # Sets use sorted canonical order with integer indices (matching Pattern.compile_pattern)
    sorted_items = items |> MapSet.to_list() |> Absynthe.Preserves.Compare.sort()

    sorted_items
    |> Enum.with_index()
    |> Enum.flat_map(fn {item, idx} ->
      item_path = path ++ [idx]
      [{item_path, item} | extract_paths(item, item_path)]
    end)
  end

  defp extract_paths({:dictionary, entries}, path) do
    # Dictionaries use [{:key, key}] tuples (matching Pattern.compile_pattern)
    entries
    |> Enum.flat_map(fn {key, val} ->
      val_path = path ++ [{:key, key}]
      [{val_path, val} | extract_paths(val, val_path)]
    end)
  end

  # Atomic values at root level: include root path entry for literal patterns like {:integer, 42}
  defp extract_paths(atomic, []), do: [{[], atomic}]

  # Non-root atomic values: already indexed by parent clause, no additional entries needed
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

  # Find observers that match a given assertion using the observer index.
  # This is O(matching observers) instead of O(all observers).
  @spec find_matching_observers(t(), Handle.t(), Value.t()) ::
          [{observer_id :: term(), observer_ref :: term(), Value.t(), captures :: [Value.t()]}]
  defp find_matching_observers(%__MODULE__{} = skeleton, _handle, value) do
    # Get candidate observers using the observer index
    candidate_ids = find_candidate_observers(skeleton, value)

    # For each candidate, perform full pattern matching
    candidate_ids
    |> Enum.flat_map(fn observer_id ->
      case :ets.lookup(skeleton.observers, observer_id) do
        [{^observer_id, {pattern, observer_ref}}] ->
          case Pattern.match(pattern, value) do
            {:ok, captures} ->
              [{observer_id, observer_ref, value, captures}]

            :no_match ->
              []
          end

        [] ->
          []
      end
    end)
  end

  # Find candidate observers that might match a value by looking up the observer index.
  # Returns a set of observer_ids that have constraints potentially satisfied by the value.
  @spec find_candidate_observers(t(), Value.t()) :: MapSet.t(term())
  defp find_candidate_observers(%__MODULE__{} = skeleton, value) do
    # Start with wildcard observers (they always need to be checked)
    base_candidates = skeleton.wildcard_observers

    # Extract paths from the assertion value
    paths = extract_paths(value)

    # For each path in the value, look up which observers have constraints on that path
    # An observer is a candidate if ALL its constraints could be satisfied
    # But we can only determine this by checking, so we gather all observers with
    # at least one matching constraint and let Pattern.match filter the rest

    # Gather observer_ids that have at least one constraint matching our value
    path_candidates =
      paths
      |> Enum.reduce(MapSet.new(), fn {path, path_value}, acc ->
        value_class = value_class(path_value)
        index_key = {path, value_class}

        case :ets.lookup(skeleton.observer_index, index_key) do
          [{^index_key, observer_entries}] ->
            # Check each observer entry - if exact_value matches (or is nil for any-of-class)
            matching =
              observer_entries
              |> Enum.filter(fn {_observer_id, exact_value} ->
                exact_value == nil or exact_value == path_value
              end)
              |> Enum.map(fn {observer_id, _} -> observer_id end)

            MapSet.union(acc, MapSet.new(matching))

          [] ->
            acc
        end
      end)

    MapSet.union(base_candidates, path_candidates)
  end

  # Index an observer by its pattern constraints for efficient lookup
  @spec index_observer(t(), term(), Pattern.t()) :: t()
  defp index_observer(%__MODULE__{} = skeleton, observer_id, %Pattern{} = pattern) do
    constraints = Pattern.index_paths(pattern)

    case constraints do
      [] ->
        # No constraints - this observer matches everything, add to wildcard set
        %{skeleton | wildcard_observers: MapSet.put(skeleton.wildcard_observers, observer_id)}

      _ ->
        # Add observer to index for each constraint
        Enum.each(constraints, fn {path, expected_value} ->
          value_class = value_class(expected_value)
          index_key = {path, value_class}

          case :ets.lookup(skeleton.observer_index, index_key) do
            [{^index_key, observer_entries}] ->
              new_entries = MapSet.put(observer_entries, {observer_id, expected_value})
              :ets.insert(skeleton.observer_index, {index_key, new_entries})

            [] ->
              :ets.insert(
                skeleton.observer_index,
                {index_key, MapSet.new([{observer_id, expected_value}])}
              )
          end
        end)

        skeleton
    end
  end

  # Remove an observer from the index
  @spec unindex_observer(t(), term(), Pattern.t()) :: t()
  defp unindex_observer(%__MODULE__{} = skeleton, observer_id, %Pattern{} = pattern) do
    constraints = Pattern.index_paths(pattern)

    case constraints do
      [] ->
        # Was a wildcard observer
        %{skeleton | wildcard_observers: MapSet.delete(skeleton.wildcard_observers, observer_id)}

      _ ->
        # Remove from each index entry
        Enum.each(constraints, fn {path, expected_value} ->
          value_class = value_class(expected_value)
          index_key = {path, value_class}

          case :ets.lookup(skeleton.observer_index, index_key) do
            [{^index_key, observer_entries}] ->
              new_entries = MapSet.delete(observer_entries, {observer_id, expected_value})

              if MapSet.size(new_entries) == 0 do
                :ets.delete(skeleton.observer_index, index_key)
              else
                :ets.insert(skeleton.observer_index, {index_key, new_entries})
              end

            [] ->
              :ok
          end
        end)

        skeleton
    end
  end

  # Get the value class (type tag) for a Preserves value
  @spec value_class(Value.t()) :: atom()
  defp value_class({tag, _}) when is_atom(tag), do: tag
  defp value_class(_), do: :unknown
end
