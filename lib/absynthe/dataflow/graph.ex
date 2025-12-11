defmodule Absynthe.Dataflow.Graph do
  @moduledoc """
  Dataflow graph for the Syndicated Actor Model.

  ## Overview

  The Graph module implements the dependency tracking and propagation system for
  the dataflow layer of the Syndicated Actor Model (as designed by Tony Garnock-Jones).
  It manages a directed acyclic graph (DAG) of Field dependencies, ensuring that
  changes propagate correctly and updates occur in topological order.

  ## Architecture

  The dataflow graph tracks:

  - **Fields** - Computational values that can depend on other fields
  - **Dependencies** - Edges from fields to fields they depend on
  - **Dependents** - Edges from fields to fields that depend on them
  - **Dirty Set** - Fields that need recomputation
  - **Computing Stack** - Fields currently being computed (for cycle detection)

  ```
  Field A ─────┐
               ├──> Field C (depends on A and B)
  Field B ─────┘
  ```

  When Field A changes, the graph:
  1. Marks A as dirty
  2. Propagates dirtiness to C (dependent of A)
  3. Recomputes fields in topological order: A, then C

  ## Graph Structure

  ```elixir
  %Absynthe.Dataflow.Graph{
    fields: %{field_id => field_value},
    edges: %{dependency_id => MapSet.t(dependent_id)},
    reverse_edges: %{dependent_id => MapSet.t(dependency_id)},
    dirty: MapSet.t(field_id),
    computing: [field_id]
  }
  ```

  - `fields` - Registry of all fields in the graph
  - `edges` - Forward edges (dependency → set of dependents)
  - `reverse_edges` - Reverse edges (dependent → set of dependencies)
  - `dirty` - Set of fields requiring recomputation
  - `computing` - Stack of fields being computed (for cycle detection)

  ## Glitch-Free Updates

  The graph ensures glitch-free updates by:

  1. Computing fields in topological order
  2. Ensuring all dependencies are fresh before computing a dependent
  3. Making updates atomic (all-or-nothing)
  4. Detecting and rejecting cyclic dependencies

  This prevents "glitches" where a field might see an inconsistent mix of
  old and new dependency values.

  ## Process-Local Graphs

  Each actor maintains its own Graph instance, typically stored in actor state:

  ```elixir
  def init(opts) do
    graph = Graph.new()
    {:ok, %{graph: graph, ...}}
  end

  def handle_turn(turn, state) do
    # Update dirty fields at the end of each turn
    graph = Graph.update(state.graph)
    {:ok, %{state | graph: graph}}
  end
  ```

  Alternatively, graphs can be stored in the process dictionary for convenience:

  ```elixir
  Graph.with_graph(graph, fn ->
    # Operations within this function use the specified graph
    Graph.mark_dirty(field_id)
  end)
  ```

  ## Cycle Detection

  The graph detects dependency cycles using the `computing` stack:

  - Before computing a field, push it onto the stack
  - If computing field A requires computing field B, push B
  - If we try to push a field already in the stack, we have a cycle
  - After computing a field, pop it from the stack

  Cycles are rejected with a descriptive error showing the cycle path.

  ## Examples

  ### Basic Usage

  ```elixir
  # Create a new graph
  graph = Graph.new()

  # Register fields
  {graph, a_id} = Graph.register_field(graph, 10)
  {graph, b_id} = Graph.register_field(graph, 20)

  # Add dependency edges (if sum depends on a and b)
  graph = Graph.add_edge(graph, a_id, sum_id)
  graph = Graph.add_edge(graph, b_id, sum_id)

  # Mark a field dirty and propagate
  graph = Graph.mark_dirty(graph, a_id)
  graph = Graph.propagate(graph, a_id)

  # Check dirty fields
  Graph.dirty?(graph, sum_id)  # => true

  # Get dependencies and dependents
  Graph.get_dependencies(graph, sum_id)  # => MapSet.new([a_id, b_id])
  Graph.get_dependents(graph, a_id)      # => MapSet.new([sum_id])
  ```

  ### Topological Update

  ```elixir
  # Mark multiple fields dirty
  graph = graph
    |> Graph.mark_dirty(a_id)
    |> Graph.mark_dirty(b_id)

  # Update all dirty fields in topological order
  # (providing a recompute function)
  graph = Graph.update(graph, fn field_id, graph ->
    # Recompute the field
    new_value = compute_field(field_id, graph)
    Graph.update_field(graph, field_id, new_value)
  end)

  # All fields are now clean
  Graph.has_dirty?(graph)  # => false
  ```

  ### Cycle Detection

  ```elixir
  graph = Graph.new()
  {graph, a_id} = Graph.register_field(graph, 1)
  {graph, b_id} = Graph.register_field(graph, 2)

  # Create a cycle: a depends on b, b depends on a
  graph = graph
    |> Graph.add_edge(a_id, b_id)
    |> Graph.add_edge(b_id, a_id)

  # Attempting to update will detect the cycle
  Graph.update(graph, recompute_fn)
  # => {:error, {:cycle, [a_id, b_id, a_id]}}
  ```

  ## Integration with Fields

  The Graph module is designed to work with the Field module:

  ```elixir
  # Field values can track their dependencies
  field = Field.computed(fn ->
    # During computation, record dependencies
    a_value = Graph.get_field(graph, a_id)
    b_value = Graph.get_field(graph, b_id)
    a_value + b_value
  end)

  # When dependencies change
  graph = Graph.mark_dirty(graph, a_id)
  graph = Graph.propagate(graph, a_id)
  graph = Graph.update(graph)
  ```

  ## Performance Considerations

  - Use MapSet for efficient set operations
  - Topological sort is O(V + E) where V = vertices, E = edges
  - Propagation is O(reachable nodes)
  - For very large graphs, consider using ETS

  ## See Also

  - `Absynthe.Dataflow.Field` - Field values and computation
  - `Absynthe.Core.Actor` - Actor integration
  - `Absynthe.Core.Turn` - Transaction-based execution
  """

  defstruct fields: %{},
            edges: %{},
            reverse_edges: %{},
            dirty: MapSet.new(),
            computing: []

  @typedoc """
  Unique identifier for a field in the graph.

  Typically a reference, atom, or integer.
  """
  @type field_id :: term()

  @typedoc """
  Field value stored in the graph.

  Can be any term representing the current value of the field.
  """
  @type field_value :: term()

  @typedoc """
  Dataflow graph structure.

  Tracks fields, dependencies, and dirty state for incremental updates.
  """
  @type t :: %__MODULE__{
          fields: %{field_id() => field_value()},
          edges: %{field_id() => MapSet.t(field_id())},
          reverse_edges: %{field_id() => MapSet.t(field_id())},
          dirty: MapSet.t(field_id()),
          computing: [field_id()]
        }

  @typedoc """
  Function for recomputing a field's value.

  Receives the field_id and current graph, returns updated graph.
  """
  @type recompute_fn :: (field_id(), t() -> t())

  # Process dictionary key for storing current graph
  @graph_key :"$absynthe_dataflow_graph"

  # Core Operations

  @doc """
  Creates a new empty dataflow graph.

  ## Examples

      iex> graph = Graph.new()
      iex> graph.fields
      %{}
      iex> MapSet.size(graph.dirty)
      0

  ## Returns

  A new, empty `Graph` struct.
  """
  @spec new() :: t()
  def new do
    %__MODULE__{}
  end

  @doc """
  Registers a field in the graph with an initial value.

  Generates a unique field ID and stores the field value. The field
  starts in a clean state (not dirty).

  ## Parameters

  - `graph` - The graph to register the field in
  - `value` - The initial value of the field

  ## Examples

      iex> graph = Graph.new()
      iex> {graph, field_id} = Graph.register_field(graph, 42)
      iex> Graph.get_field(graph, field_id)
      42

  ## Returns

  A tuple `{updated_graph, field_id}` where `field_id` is the newly
  assigned identifier for the field.
  """
  @spec register_field(t(), field_value()) :: {t(), field_id()}
  def register_field(graph, value) do
    field_id = make_ref()
    graph = %{graph | fields: Map.put(graph.fields, field_id, value)}
    {graph, field_id}
  end

  @doc """
  Registers a field with a specific ID in the graph.

  Unlike `register_field/2`, this allows you to specify the field ID
  explicitly. Useful when you need stable, predictable identifiers.

  ## Parameters

  - `graph` - The graph to register the field in
  - `field_id` - The identifier for the field
  - `value` - The initial value of the field

  ## Examples

      iex> graph = Graph.new()
      iex> graph = Graph.register_field_with_id(graph, :my_field, 100)
      iex> Graph.get_field(graph, :my_field)
      100

  ## Returns

  The updated graph with the field registered.
  """
  @spec register_field_with_id(t(), field_id(), field_value()) :: t()
  def register_field_with_id(graph, field_id, value) do
    %{graph | fields: Map.put(graph.fields, field_id, value)}
  end

  @doc """
  Unregisters a field from the graph.

  Removes the field and all its dependency edges (both incoming and outgoing).
  Also removes the field from the dirty set if present.

  ## Parameters

  - `graph` - The graph to unregister from
  - `field_id` - The ID of the field to remove

  ## Examples

      iex> graph = Graph.new()
      iex> {graph, field_id} = Graph.register_field(graph, 42)
      iex> graph = Graph.unregister_field(graph, field_id)
      iex> Graph.has_field?(graph, field_id)
      false

  ## Returns

  The updated graph with the field removed.
  """
  @spec unregister_field(t(), field_id()) :: t()
  def unregister_field(graph, field_id) do
    # Remove from fields
    graph = %{graph | fields: Map.delete(graph.fields, field_id)}

    # Remove all edges where this field is a dependency
    dependents = Map.get(graph.edges, field_id, MapSet.new())

    graph =
      Enum.reduce(dependents, graph, fn dependent_id, acc ->
        remove_edge(acc, field_id, dependent_id)
      end)

    # Remove all edges where this field is a dependent
    dependencies = Map.get(graph.reverse_edges, field_id, MapSet.new())

    graph =
      Enum.reduce(dependencies, graph, fn dependency_id, acc ->
        remove_edge(acc, dependency_id, field_id)
      end)

    # Remove from dirty set
    %{graph | dirty: MapSet.delete(graph.dirty, field_id)}
  end

  @doc """
  Adds a dependency edge to the graph.

  Creates an edge from `dependency_id` to `dependent_id`, indicating that
  `dependent_id` depends on `dependency_id`. When `dependency_id` changes,
  `dependent_id` will be marked dirty.

  ## Parameters

  - `graph` - The graph to add the edge to
  - `dependency_id` - The field being depended on
  - `dependent_id` - The field that depends on the dependency

  ## Examples

      iex> graph = Graph.new()
      iex> {graph, a} = Graph.register_field(graph, 1)
      iex> {graph, b} = Graph.register_field(graph, 2)
      iex> graph = Graph.add_edge(graph, a, b)
      iex> MapSet.member?(Graph.get_dependents(graph, a), b)
      true

  ## Returns

  The updated graph with the edge added.
  """
  @spec add_edge(t(), field_id(), field_id()) :: t()
  def add_edge(graph, dependency_id, dependent_id) do
    # Add to forward edges (dependency -> dependents)
    edges =
      Map.update(
        graph.edges,
        dependency_id,
        MapSet.new([dependent_id]),
        &MapSet.put(&1, dependent_id)
      )

    # Add to reverse edges (dependent -> dependencies)
    reverse_edges =
      Map.update(
        graph.reverse_edges,
        dependent_id,
        MapSet.new([dependency_id]),
        &MapSet.put(&1, dependency_id)
      )

    %{graph | edges: edges, reverse_edges: reverse_edges}
  end

  @doc """
  Removes a dependency edge from the graph.

  Removes the edge from `dependency_id` to `dependent_id`.

  ## Parameters

  - `graph` - The graph to remove the edge from
  - `dependency_id` - The field being depended on
  - `dependent_id` - The field that depends on the dependency

  ## Examples

      iex> graph = Graph.new()
      iex> {graph, a} = Graph.register_field(graph, 1)
      iex> {graph, b} = Graph.register_field(graph, 2)
      iex> graph = graph |> Graph.add_edge(a, b) |> Graph.remove_edge(a, b)
      iex> MapSet.size(Graph.get_dependents(graph, a))
      0

  ## Returns

  The updated graph with the edge removed.
  """
  @spec remove_edge(t(), field_id(), field_id()) :: t()
  def remove_edge(graph, dependency_id, dependent_id) do
    # Remove from forward edges
    edges =
      Map.update(graph.edges, dependency_id, MapSet.new(), fn dependents ->
        MapSet.delete(dependents, dependent_id)
      end)

    # Remove from reverse edges
    reverse_edges =
      Map.update(graph.reverse_edges, dependent_id, MapSet.new(), fn dependencies ->
        MapSet.delete(dependencies, dependency_id)
      end)

    %{graph | edges: edges, reverse_edges: reverse_edges}
  end

  @doc """
  Marks a field as dirty (needing recomputation).

  Dirty fields will be recomputed on the next call to `update/2`.

  ## Parameters

  - `graph` - The graph to update
  - `field_id` - The ID of the field to mark dirty

  ## Examples

      iex> graph = Graph.new()
      iex> {graph, field_id} = Graph.register_field(graph, 42)
      iex> graph = Graph.mark_dirty(graph, field_id)
      iex> Graph.dirty?(graph, field_id)
      true

  ## Returns

  The updated graph with the field marked as dirty.
  """
  @spec mark_dirty(t(), field_id()) :: t()
  def mark_dirty(graph, field_id) do
    %{graph | dirty: MapSet.put(graph.dirty, field_id)}
  end

  @doc """
  Marks a field as clean (not needing recomputation).

  Removes the field from the dirty set.

  ## Parameters

  - `graph` - The graph to update
  - `field_id` - The ID of the field to mark clean

  ## Examples

      iex> graph = Graph.new()
      iex> {graph, field_id} = Graph.register_field(graph, 42)
      iex> graph = graph |> Graph.mark_dirty(field_id) |> Graph.mark_clean(field_id)
      iex> Graph.dirty?(graph, field_id)
      false

  ## Returns

  The updated graph with the field marked as clean.
  """
  @spec mark_clean(t(), field_id()) :: t()
  def mark_clean(graph, field_id) do
    %{graph | dirty: MapSet.delete(graph.dirty, field_id)}
  end

  @doc """
  Propagates dirtiness from a field to all its dependents.

  When a field changes, all fields that depend on it must be marked dirty.
  This function recursively marks all transitive dependents as dirty.

  ## Parameters

  - `graph` - The graph to propagate in
  - `field_id` - The ID of the field whose dependents should be marked dirty

  ## Examples

      iex> graph = Graph.new()
      iex> {graph, a} = Graph.register_field(graph, 1)
      iex> {graph, b} = Graph.register_field(graph, 2)
      iex> {graph, c} = Graph.register_field(graph, 3)
      iex> graph = graph |> Graph.add_edge(a, b) |> Graph.add_edge(b, c)
      iex> graph = graph |> Graph.mark_dirty(a) |> Graph.propagate(a)
      iex> Graph.dirty?(graph, b) and Graph.dirty?(graph, c)
      true

  ## Returns

  The updated graph with all transitive dependents marked as dirty.
  """
  @spec propagate(t(), field_id()) :: t()
  def propagate(graph, field_id) do
    dependents = get_dependents(graph, field_id)

    Enum.reduce(dependents, graph, fn dependent_id, acc ->
      acc = mark_dirty(acc, dependent_id)
      propagate(acc, dependent_id)
    end)
  end

  @doc """
  Gets all fields that directly depend on the given field.

  Returns the immediate dependents, not transitive dependents.

  ## Parameters

  - `graph` - The graph to query
  - `field_id` - The ID of the field to get dependents for

  ## Examples

      iex> graph = Graph.new()
      iex> {graph, a} = Graph.register_field(graph, 1)
      iex> {graph, b} = Graph.register_field(graph, 2)
      iex> graph = Graph.add_edge(graph, a, b)
      iex> dependents = Graph.get_dependents(graph, a)
      iex> MapSet.member?(dependents, b)
      true

  ## Returns

  A `MapSet` of field IDs that depend on the given field.
  """
  @spec get_dependents(t(), field_id()) :: MapSet.t(field_id())
  def get_dependents(graph, field_id) do
    Map.get(graph.edges, field_id, MapSet.new())
  end

  @doc """
  Gets all fields that the given field directly depends on.

  Returns the immediate dependencies, not transitive dependencies.

  ## Parameters

  - `graph` - The graph to query
  - `field_id` - The ID of the field to get dependencies for

  ## Examples

      iex> graph = Graph.new()
      iex> {graph, a} = Graph.register_field(graph, 1)
      iex> {graph, b} = Graph.register_field(graph, 2)
      iex> graph = Graph.add_edge(graph, a, b)
      iex> deps = Graph.get_dependencies(graph, b)
      iex> MapSet.member?(deps, a)
      true

  ## Returns

  A `MapSet` of field IDs that the given field depends on.
  """
  @spec get_dependencies(t(), field_id()) :: MapSet.t(field_id())
  def get_dependencies(graph, field_id) do
    Map.get(graph.reverse_edges, field_id, MapSet.new())
  end

  @doc """
  Gets the current value of a field.

  ## Parameters

  - `graph` - The graph to query
  - `field_id` - The ID of the field to get

  ## Examples

      iex> graph = Graph.new()
      iex> {graph, field_id} = Graph.register_field(graph, 42)
      iex> Graph.get_field(graph, field_id)
      42

  ## Returns

  The current value of the field, or `nil` if the field doesn't exist.
  """
  @spec get_field(t(), field_id()) :: field_value() | nil
  def get_field(graph, field_id) do
    Map.get(graph.fields, field_id)
  end

  @doc """
  Updates the value of a field.

  Sets the field's value to the given value. Does not affect dirtiness.

  ## Parameters

  - `graph` - The graph to update
  - `field_id` - The ID of the field to update
  - `value` - The new value for the field

  ## Examples

      iex> graph = Graph.new()
      iex> {graph, field_id} = Graph.register_field(graph, 42)
      iex> graph = Graph.update_field(graph, field_id, 100)
      iex> Graph.get_field(graph, field_id)
      100

  ## Returns

  The updated graph with the field's value changed.
  """
  @spec update_field(t(), field_id(), field_value()) :: t()
  def update_field(graph, field_id, value) do
    %{graph | fields: Map.put(graph.fields, field_id, value)}
  end

  @doc """
  Checks if a field exists in the graph.

  ## Parameters

  - `graph` - The graph to query
  - `field_id` - The ID of the field to check

  ## Examples

      iex> graph = Graph.new()
      iex> {graph, field_id} = Graph.register_field(graph, 42)
      iex> Graph.has_field?(graph, field_id)
      true

  ## Returns

  `true` if the field exists, `false` otherwise.
  """
  @spec has_field?(t(), field_id()) :: boolean()
  def has_field?(graph, field_id) do
    Map.has_key?(graph.fields, field_id)
  end

  @doc """
  Checks if a field is dirty (needs recomputation).

  ## Parameters

  - `graph` - The graph to query
  - `field_id` - The ID of the field to check

  ## Examples

      iex> graph = Graph.new()
      iex> {graph, field_id} = Graph.register_field(graph, 42)
      iex> graph = Graph.mark_dirty(graph, field_id)
      iex> Graph.dirty?(graph, field_id)
      true

  ## Returns

  `true` if the field is dirty, `false` otherwise.
  """
  @spec dirty?(t(), field_id()) :: boolean()
  def dirty?(graph, field_id) do
    MapSet.member?(graph.dirty, field_id)
  end

  @doc """
  Checks if the graph has any dirty fields.

  ## Parameters

  - `graph` - The graph to query

  ## Examples

      iex> graph = Graph.new()
      iex> {graph, field_id} = Graph.register_field(graph, 42)
      iex> graph = Graph.mark_dirty(graph, field_id)
      iex> Graph.has_dirty?(graph)
      true

  ## Returns

  `true` if there are any dirty fields, `false` otherwise.
  """
  @spec has_dirty?(t()) :: boolean()
  def has_dirty?(graph) do
    not MapSet.equal?(graph.dirty, MapSet.new())
  end

  @doc """
  Gets all dirty field IDs.

  ## Parameters

  - `graph` - The graph to query

  ## Examples

      iex> graph = Graph.new()
      iex> {graph, a} = Graph.register_field(graph, 1)
      iex> {graph, b} = Graph.register_field(graph, 2)
      iex> graph = graph |> Graph.mark_dirty(a) |> Graph.mark_dirty(b)
      iex> dirty = Graph.get_dirty(graph)
      iex> MapSet.size(dirty)
      2

  ## Returns

  A `MapSet` of all dirty field IDs.
  """
  @spec get_dirty(t()) :: MapSet.t(field_id())
  def get_dirty(graph) do
    graph.dirty
  end

  # Topological Update

  @doc """
  Updates all dirty fields in topological order.

  Recomputes all dirty fields, ensuring that dependencies are computed before
  dependents. This ensures glitch-free updates where no field sees an
  inconsistent mix of old and new dependency values.

  The recompute function is called for each dirty field in topological order.
  It should compute the new value based on the current graph state and return
  an updated graph with the new value.

  ## Parameters

  - `graph` - The graph to update
  - `recompute_fn` - Function to recompute a field's value

  ## Examples

      iex> graph = Graph.new()
      iex> {graph, a} = Graph.register_field(graph, 10)
      iex> {graph, sum} = Graph.register_field(graph, 0)
      iex> graph = Graph.add_edge(graph, a, sum)
      iex> graph = graph |> Graph.mark_dirty(a) |> Graph.mark_dirty(sum)
      iex> graph = Graph.update(graph, fn field_id, g ->
      ...>   if field_id == sum do
      ...>     a_val = Graph.get_field(g, a)
      ...>     Graph.update_field(g, sum, a_val * 2)
      ...>   else
      ...>     g
      ...>   end
      ...> end)
      iex> Graph.get_field(graph, sum)
      20

  ## Returns

  - `{:ok, graph}` - Successfully updated all dirty fields
  - `{:error, {:cycle, path}}` - Detected a dependency cycle

  ## Errors

  Raises an error if a dependency cycle is detected during computation.
  """
  @spec update(t(), recompute_fn()) :: {:ok, t()} | {:error, {:cycle, [field_id()]}}
  def update(graph, recompute_fn) do
    case topological_sort(graph, MapSet.to_list(graph.dirty)) do
      {:ok, sorted_fields} ->
        # Recompute fields in topological order
        updated_graph =
          Enum.reduce(sorted_fields, graph, fn field_id, acc_graph ->
            # Recompute the field
            new_graph = recompute_fn.(field_id, acc_graph)
            # Mark as clean after recomputation
            mark_clean(new_graph, field_id)
          end)

        {:ok, updated_graph}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Updates all dirty fields using a simple recompute function.

  Convenience wrapper around `update/2` that handles the success case
  and returns the updated graph directly. Raises on cycle detection.

  ## Parameters

  - `graph` - The graph to update
  - `recompute_fn` - Function to recompute a field's value

  ## Examples

      iex> graph = Graph.update!(graph, fn field_id, g ->
      ...>   # Recompute field_id
      ...>   new_value = compute(field_id, g)
      ...>   Graph.update_field(g, field_id, new_value)
      ...> end)

  ## Returns

  The updated graph with all dirty fields recomputed.

  ## Raises

  Raises an error if a dependency cycle is detected.
  """
  @spec update!(t(), recompute_fn()) :: t()
  def update!(graph, recompute_fn) do
    case update(graph, recompute_fn) do
      {:ok, updated_graph} -> updated_graph
      {:error, reason} -> raise "Graph update failed: #{inspect(reason)}"
    end
  end

  @doc """
  Performs a topological sort of the given field IDs.

  Orders fields such that dependencies come before dependents. This ensures
  that when computing fields in order, all dependencies have been computed
  before they're needed.

  ## Parameters

  - `graph` - The graph to sort
  - `field_ids` - List of field IDs to sort

  ## Examples

      iex> graph = Graph.new()
      iex> {graph, a} = Graph.register_field(graph, 1)
      iex> {graph, b} = Graph.register_field(graph, 2)
      iex> {graph, c} = Graph.register_field(graph, 3)
      iex> graph = graph |> Graph.add_edge(a, b) |> Graph.add_edge(b, c)
      iex> {:ok, sorted} = Graph.topological_sort(graph, [c, a, b])
      iex> Enum.find_index(sorted, &(&1 == a)) < Enum.find_index(sorted, &(&1 == b))
      true

  ## Returns

  - `{:ok, sorted_list}` - Successfully sorted field IDs
  - `{:error, {:cycle, path}}` - Detected a dependency cycle
  """
  @spec topological_sort(t(), [field_id()]) :: {:ok, [field_id()]} | {:error, {:cycle, [field_id()]}}
  def topological_sort(graph, field_ids) do
    # Use Kahn's algorithm for topological sorting
    # Build in-degree map for the subgraph
    field_set = MapSet.new(field_ids)

    in_degrees =
      Enum.reduce(field_ids, %{}, fn field_id, acc ->
        deps = get_dependencies(graph, field_id)
        # Only count dependencies that are in our subgraph
        relevant_deps = MapSet.intersection(deps, field_set)
        Map.put(acc, field_id, MapSet.size(relevant_deps))
      end)

    # Start with fields that have no dependencies in the subgraph
    queue =
      field_ids
      |> Enum.filter(fn field_id -> Map.get(in_degrees, field_id, 0) == 0 end)

    do_topological_sort(graph, queue, in_degrees, field_set, [])
  end

  @doc false
  defp do_topological_sort(_graph, [], _in_degrees, field_set, result) do
    # Check if all fields have been processed
    if length(result) == MapSet.size(field_set) do
      {:ok, Enum.reverse(result)}
    else
      # There's a cycle - find it
      remaining = MapSet.difference(field_set, MapSet.new(result))
      {:error, {:cycle, MapSet.to_list(remaining)}}
    end
  end

  defp do_topological_sort(graph, [field_id | rest], in_degrees, field_set, result) do
    # Add this field to result
    new_result = [field_id | result]

    # Get dependents in our subgraph
    dependents = get_dependents(graph, field_id)
    relevant_dependents = MapSet.intersection(dependents, field_set)

    # Reduce in-degree for each dependent
    {new_in_degrees, new_queue} =
      Enum.reduce(relevant_dependents, {in_degrees, rest}, fn dep_id, {degrees, queue} ->
        new_degree = Map.get(degrees, dep_id, 1) - 1
        new_degrees = Map.put(degrees, dep_id, new_degree)

        # If in-degree is now 0, add to queue
        if new_degree == 0 do
          {new_degrees, [dep_id | queue]}
        else
          {new_degrees, queue}
        end
      end)

    do_topological_sort(graph, new_queue, new_in_degrees, field_set, new_result)
  end

  # Cycle Detection

  @doc """
  Pushes a field onto the computing stack.

  Used to track which fields are currently being computed, enabling
  cycle detection. Before computing a field, push it onto the stack.

  ## Parameters

  - `graph` - The graph to update
  - `field_id` - The ID of the field being computed

  ## Examples

      iex> graph = Graph.new()
      iex> {graph, field_id} = Graph.register_field(graph, 42)
      iex> {:ok, graph} = Graph.push_computing(graph, field_id)
      iex> Graph.computing?(graph, field_id)
      true

  ## Returns

  - `{:ok, graph}` - Successfully pushed
  - `{:error, {:cycle, path}}` - Field already in computing stack (cycle detected)
  """
  @spec push_computing(t(), field_id()) :: {:ok, t()} | {:error, {:cycle, [field_id()]}}
  def push_computing(graph, field_id) do
    if Enum.member?(graph.computing, field_id) do
      # Cycle detected - build the cycle path
      cycle_path = build_cycle_path(graph.computing, field_id)
      {:error, {:cycle, cycle_path}}
    else
      {:ok, %{graph | computing: [field_id | graph.computing]}}
    end
  end

  @doc """
  Pops a field from the computing stack.

  Called after a field has been computed to remove it from the computing stack.

  ## Parameters

  - `graph` - The graph to update

  ## Examples

      iex> graph = Graph.new()
      iex> {graph, field_id} = Graph.register_field(graph, 42)
      iex> {:ok, graph} = Graph.push_computing(graph, field_id)
      iex> graph = Graph.pop_computing(graph)
      iex> Graph.computing?(graph, field_id)
      false

  ## Returns

  The updated graph with the top field removed from the computing stack.
  """
  @spec pop_computing(t()) :: t()
  def pop_computing(graph) do
    case graph.computing do
      [_head | tail] -> %{graph | computing: tail}
      [] -> graph
    end
  end

  @doc """
  Checks if a field is currently being computed.

  ## Parameters

  - `graph` - The graph to query
  - `field_id` - The ID of the field to check

  ## Examples

      iex> graph = Graph.new()
      iex> {graph, field_id} = Graph.register_field(graph, 42)
      iex> {:ok, graph} = Graph.push_computing(graph, field_id)
      iex> Graph.computing?(graph, field_id)
      true

  ## Returns

  `true` if the field is in the computing stack, `false` otherwise.
  """
  @spec computing?(t(), field_id()) :: boolean()
  def computing?(graph, field_id) do
    Enum.member?(graph.computing, field_id)
  end

  @doc false
  defp build_cycle_path(computing_stack, field_id) do
    # Find the field in the stack and return the cycle
    case Enum.split_while(computing_stack, fn id -> id != field_id end) do
      {_before, [^field_id | _] = cycle} ->
        Enum.reverse([field_id | cycle])

      _ ->
        # Should not happen, but return full stack as fallback
        Enum.reverse([field_id | computing_stack])
    end
  end

  @doc """
  Records a dependency during field computation.

  When computing a field's value, call this function for each field that
  is read. This automatically creates dependency edges and tracks the
  relationship for future updates.

  This function operates on the current graph (from process dictionary)
  if no graph is provided.

  ## Parameters

  - `field_id` - The ID of the field being depended on

  ## Examples

      iex> Graph.with_graph(graph, fn ->
      ...>   # During computation of field 'sum'
      ...>   a_value = Graph.get_field(Graph.current_graph(), a_id)
      ...>   Graph.record_dependency(a_id)
      ...>   b_value = Graph.get_field(Graph.current_graph(), b_id)
      ...>   Graph.record_dependency(b_id)
      ...>   a_value + b_value
      ...> end)

  ## Returns

  `:ok` if the dependency was recorded successfully.
  """
  @spec record_dependency(field_id()) :: :ok
  def record_dependency(field_id) do
    graph = current_graph()

    case graph.computing do
      [computing_field | _] ->
        # Add edge from dependency to computing field
        updated_graph = add_edge(graph, field_id, computing_field)
        put_current_graph(updated_graph)
        :ok

      [] ->
        # No field is currently being computed, ignore
        :ok
    end
  end

  # Process-Local Graph Management

  @doc """
  Gets the current graph from the process dictionary.

  Each actor process can maintain a current graph for convenience.
  This allows field computation functions to access the graph without
  explicitly passing it around.

  ## Examples

      iex> Graph.put_current_graph(graph)
      iex> current = Graph.current_graph()
      iex> current == graph
      true

  ## Returns

  The current graph, or a new empty graph if none is set.
  """
  @spec current_graph() :: t()
  def current_graph do
    Process.get(@graph_key, new())
  end

  @doc """
  Sets the current graph in the process dictionary.

  ## Parameters

  - `graph` - The graph to set as current

  ## Examples

      iex> Graph.put_current_graph(graph)
      :ok

  ## Returns

  `:ok`
  """
  @spec put_current_graph(t()) :: :ok
  def put_current_graph(graph) do
    Process.put(@graph_key, graph)
    :ok
  end

  @doc """
  Executes a function with a specific graph as current.

  Temporarily sets the given graph as current, executes the function,
  then restores the previous current graph. Useful for isolating
  graph operations.

  ## Parameters

  - `graph` - The graph to use during function execution
  - `fun` - The function to execute

  ## Examples

      iex> result = Graph.with_graph(graph, fn ->
      ...>   # Operations here use 'graph' as current
      ...>   Graph.mark_dirty(field_id)
      ...>   compute_values()
      ...> end)

  ## Returns

  The return value of the function and the updated graph as a tuple
  `{return_value, updated_graph}`.
  """
  @spec with_graph(t(), (-> term())) :: {term(), t()}
  def with_graph(graph, fun) do
    old_graph = Process.get(@graph_key)

    try do
      put_current_graph(graph)
      result = fun.()
      updated_graph = current_graph()
      {result, updated_graph}
    after
      if old_graph do
        Process.put(@graph_key, old_graph)
      else
        Process.delete(@graph_key)
      end
    end
  end

  @doc """
  Clears the current graph from the process dictionary.

  ## Examples

      iex> Graph.clear_current_graph()
      :ok

  ## Returns

  `:ok`
  """
  @spec clear_current_graph() :: :ok
  def clear_current_graph do
    Process.delete(@graph_key)
    :ok
  end
end
