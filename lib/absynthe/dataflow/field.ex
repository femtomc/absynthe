defmodule Absynthe.Dataflow.Field do
  @moduledoc """
  Reactive dataflow fields for the Syndicated Actor Model.

  A Field is a reactive cell that holds a value and automatically propagates
  changes to dependent computations. Fields integrate with the Turn transaction
  system to ensure consistent, glitch-free updates across the dependency graph.

  ## Overview

  Fields provide a foundation for dataflow-style reactive programming within
  the Syndicated Actor Model. They support:

  - **Simple fields**: Mutable cells that hold values
  - **Computed fields**: Derived values that automatically recompute when dependencies change
  - **Automatic dependency tracking**: Dependencies are discovered during computation
  - **Lazy evaluation**: Computed fields only recompute when needed
  - **Version-based dirty tracking**: Efficient change detection using monotonic versions
  - **Turn integration**: Changes are staged and propagated within transactions

  ## Field Structure

  Each field contains:

  - `id`: Unique identifier for the field
  - `value`: The current value held by the field
  - `version`: Monotonic counter incremented on each change
  - `dependents`: Set of fields/observers that depend on this field
  - `dependencies`: Set of fields this field depends on
  - `compute`: Optional computation function for derived fields
  - `dependency_versions`: Map tracking the version of each dependency at last computation
  - `dirty?`: Flag indicating if recomputation is needed

  ## Transaction Semantics

  Field updates integrate with the Turn system:

  1. Changes are staged within a Turn using `set/3`
  2. The Turn accumulates all field updates
  3. When the Turn commits, changes propagate to dependents
  4. Dependent fields are marked dirty and recompute lazily on next read

  This ensures atomicity and consistency - either all changes in a Turn
  happen together, or none happen at all.

  ## Dependency Tracking

  When a computed field evaluates its computation function, it automatically
  tracks which fields are read. This is done using the process dictionary:

  1. Before computation, the field registers itself as "currently computing"
  2. During computation, any `get/1` calls register the read field as a dependency
  3. After computation, dependency relationships are established

  This approach enables automatic, transparent dependency tracking without
  requiring explicit dependency declarations.

  ## Examples

  ### Simple Fields

      # Create a field with an initial value
      name = Field.new("Alice")
      age = Field.new(30)

      # Read the current value
      Field.get(name)  # => "Alice"

      # Update the value (within a Turn)
      turn = Turn.new(:actor_1, :facet_1)
      turn = Field.set(name, "Bob", turn)
      {_turn, _actions} = Turn.commit(turn)

  ### Computed Fields

      # Create derived fields that auto-update
      first_name = Field.new("Alice")
      last_name = Field.new("Smith")

      full_name = Field.computed(fn ->
        Field.get(first_name) <> " " <> Field.get(last_name)
      end)

      Field.get(full_name)  # => "Alice Smith"

      # Updates propagate automatically
      turn = Turn.new(:actor_1, :facet_1)
      turn = Field.set(first_name, "Bob", turn)
      {_turn, _actions} = Turn.commit(turn)

      Field.get(full_name)  # => "Bob Smith"

  ### Chained Computations

      # Fields can depend on other computed fields
      price = Field.new(100)
      quantity = Field.new(3)

      subtotal = Field.computed(fn ->
        Field.get(price) * Field.get(quantity)
      end)

      tax_rate = Field.new(0.1)

      total = Field.computed(fn ->
        Field.get(subtotal) * (1 + Field.get(tax_rate))
      end)

      Field.get(total)  # => 330.0

      # Updates flow through the dependency graph
      turn = Turn.new(:actor_1, :facet_1)
      turn = Field.set(quantity, 5, turn)
      {_turn, _actions} = Turn.commit(turn)

      Field.get(total)  # => 550.0

  ## Design Notes

  ### Version Numbers

  Each field maintains a monotonic version number that increments on every
  update. Computed fields track the versions of their dependencies at the
  time of computation. This allows efficient dirty checking: if any
  dependency's current version differs from the tracked version, the
  field is dirty and needs recomputation.

  ### Lazy Evaluation

  Computed fields don't immediately recompute when dependencies change.
  Instead, they are marked dirty and only recompute when their value
  is actually read via `get/1`. This prevents unnecessary computation
  for intermediate values that are never observed.

  ### Glitch Freedom

  The Turn-based transaction system ensures glitch-free propagation.
  A "glitch" occurs when a computed field observes inconsistent values
  from multiple dependencies during an update. By staging all changes
  in a Turn and propagating them atomically, we guarantee that each
  field only recomputes once per transaction, seeing a consistent
  snapshot of its dependencies.

  ### Memory Management

  Fields maintain bidirectional references (dependencies and dependents).
  In a production system, you would need garbage collection to remove
  fields when they're no longer needed and clean up these references.
  This implementation focuses on correctness and clarity over lifecycle
  management.
  """

  alias Absynthe.Core.Turn

  @typedoc """
  A reactive field that holds a value and propagates changes.

  Fields can be simple (holding a value) or computed (deriving a value
  from other fields).

  ## Fields

  - `id` - Unique identifier for this field
  - `value` - Current value held by the field
  - `version` - Monotonic counter incremented on each update
  - `dependents` - Set of field IDs that depend on this field
  - `dependencies` - Set of field IDs this field depends on
  - `compute` - Optional function to compute value (nil for simple fields)
  - `dependency_versions` - Map of dependency ID to version at last computation
  - `dirty?` - True if field needs recomputation
  """
  @type t :: %__MODULE__{
          id: term(),
          value: term(),
          version: non_neg_integer(),
          dependents: MapSet.t(),
          dependencies: MapSet.t(),
          compute: (-> term()) | nil,
          dependency_versions: %{term() => non_neg_integer()},
          dirty?: boolean()
        }

  @enforce_keys [:id]
  defstruct [
    :id,
    :value,
    :compute,
    version: 0,
    dependents: MapSet.new(),
    dependencies: MapSet.new(),
    dependency_versions: %{},
    dirty?: false
  ]

  # Process dictionary key for tracking current computation
  @computing_field_key :__dataflow_computing_field__

  # Field Registry - In a real implementation, this would be a proper store
  # For now, we use a simple Agent for demonstration
  @doc false
  def start_registry do
    case Agent.start_link(fn -> %{} end, name: __MODULE__.Registry) do
      {:ok, pid} -> {:ok, pid}
      {:error, {:already_started, pid}} -> {:ok, pid}
    end
  end

  @doc false
  defp get_registry do
    case Process.whereis(__MODULE__.Registry) do
      nil ->
        {:ok, _} = start_registry()
        Agent.get(__MODULE__.Registry, & &1)

      _pid ->
        Agent.get(__MODULE__.Registry, & &1)
    end
  end

  @doc false
  defp update_registry(field) do
    start_registry()
    Agent.update(__MODULE__.Registry, fn registry ->
      Map.put(registry, field.id, field)
    end)
    field
  end

  @doc false
  defp lookup_field(field_id) do
    get_registry()
    |> Map.get(field_id)
  end

  # Constructor Functions

  @doc """
  Creates a new field with an initial value.

  The field is assigned a unique ID and initialized with version 0.
  It has no dependencies or dependents initially.

  ## Parameters

  - `initial_value` - The initial value to store in the field

  ## Returns

  A new `Field` struct.

  ## Examples

      iex> name = Absynthe.Dataflow.Field.new("Alice")
      iex> Absynthe.Dataflow.Field.get(name)
      "Alice"

      iex> count = Absynthe.Dataflow.Field.new(0)
      iex> Absynthe.Dataflow.Field.get(count)
      0

      iex> data = Absynthe.Dataflow.Field.new(%{key: "value"})
      iex> Absynthe.Dataflow.Field.get(data)
      %{key: "value"}
  """
  @spec new(term()) :: t()
  def new(initial_value) do
    field = %__MODULE__{
      id: make_ref(),
      value: initial_value,
      version: 0,
      compute: nil,
      dirty?: false
    }

    update_registry(field)
  end

  @doc """
  Creates a computed field that derives its value from other fields.

  The computation function is called lazily when the field's value is
  read. During computation, any fields accessed via `get/1` are
  automatically tracked as dependencies.

  When a dependency changes, this field is marked dirty and will
  recompute on the next read.

  ## Parameters

  - `compute_fn` - A zero-argument function that computes the field's value

  ## Returns

  A new computed `Field` struct.

  ## Examples

  Simple computation:

      iex> x = Absynthe.Dataflow.Field.new(10)
      iex> y = Absynthe.Dataflow.Field.new(20)
      iex> sum = Absynthe.Dataflow.Field.computed(fn ->
      ...>   Absynthe.Dataflow.Field.get(x) + Absynthe.Dataflow.Field.get(y)
      ...> end)
      iex> Absynthe.Dataflow.Field.get(sum)
      30

  Automatic dependency tracking:

      iex> first = Absynthe.Dataflow.Field.new("Hello")
      iex> second = Absynthe.Dataflow.Field.new("World")
      iex> greeting = Absynthe.Dataflow.Field.computed(fn ->
      ...>   Absynthe.Dataflow.Field.get(first) <> " " <> Absynthe.Dataflow.Field.get(second)
      ...> end)
      iex> Absynthe.Dataflow.Field.get(greeting)
      "Hello World"

  Chained computations:

      iex> price = Absynthe.Dataflow.Field.new(100)
      iex> quantity = Absynthe.Dataflow.Field.new(2)
      iex> subtotal = Absynthe.Dataflow.Field.computed(fn ->
      ...>   Absynthe.Dataflow.Field.get(price) * Absynthe.Dataflow.Field.get(quantity)
      ...> end)
      iex> tax = Absynthe.Dataflow.Field.computed(fn ->
      ...>   Absynthe.Dataflow.Field.get(subtotal) * 0.1
      ...> end)
      iex> Absynthe.Dataflow.Field.get(tax)
      20.0
  """
  @spec computed((-> term())) :: t()
  def computed(compute_fn) when is_function(compute_fn, 0) do
    field = %__MODULE__{
      id: make_ref(),
      value: nil,
      version: 0,
      compute: compute_fn,
      dirty?: true
    }

    update_registry(field)
  end

  # Value Access

  @doc """
  Reads the current value of a field.

  For simple fields, this returns the stored value immediately.

  For computed fields, this triggers lazy recomputation if the field
  is dirty (any dependency has changed). During computation, dependencies
  are automatically tracked.

  If this function is called while another field is computing, this
  field is registered as a dependency of that field.

  ## Parameters

  - `field` - The field to read from

  ## Returns

  The current value of the field.

  ## Examples

  Reading a simple field:

      iex> name = Absynthe.Dataflow.Field.new("Alice")
      iex> Absynthe.Dataflow.Field.get(name)
      "Alice"

  Reading a computed field:

      iex> x = Absynthe.Dataflow.Field.new(5)
      iex> doubled = Absynthe.Dataflow.Field.computed(fn ->
      ...>   Absynthe.Dataflow.Field.get(x) * 2
      ...> end)
      iex> Absynthe.Dataflow.Field.get(doubled)
      10

  Automatic dependency tracking:

      iex> a = Absynthe.Dataflow.Field.new(1)
      iex> b = Absynthe.Dataflow.Field.new(2)
      iex> c = Absynthe.Dataflow.Field.computed(fn ->
      ...>   Absynthe.Dataflow.Field.get(a) + Absynthe.Dataflow.Field.get(b)
      ...> end)
      iex> # First read triggers computation and tracks dependencies
      iex> Absynthe.Dataflow.Field.get(c)
      3
  """
  @spec get(t()) :: term()
  def get(%__MODULE__{} = field) do
    # Register this field as a dependency if we're currently computing another field
    maybe_register_dependency(field)

    # Recompute if needed
    field = maybe_recompute(field)

    field.value
  end

  @doc """
  Updates a field's value within a Turn transaction.

  This function stages a field update in the given Turn. When the Turn
  commits, the update is applied and all dependent fields are marked dirty.

  Only simple fields (non-computed) can be updated with `set/3`.
  Computed fields derive their values and cannot be set directly.

  ## Parameters

  - `field` - The field to update
  - `new_value` - The new value to assign
  - `turn` - The Turn transaction to stage the update in

  ## Returns

  An updated `Turn` with the field update staged.

  ## Examples

  Basic update:

      iex> name = Absynthe.Dataflow.Field.new("Alice")
      iex> turn = Absynthe.Core.Turn.new(:actor, :facet)
      iex> turn = Absynthe.Dataflow.Field.set(name, "Bob", turn)
      iex> {_turn, _actions} = Absynthe.Core.Turn.commit(turn)
      iex> Absynthe.Dataflow.Field.get(name)
      "Bob"

  Propagation to dependents:

      iex> x = Absynthe.Dataflow.Field.new(10)
      iex> doubled = Absynthe.Dataflow.Field.computed(fn ->
      ...>   Absynthe.Dataflow.Field.get(x) * 2
      ...> end)
      iex> Absynthe.Dataflow.Field.get(doubled)
      20
      iex> turn = Absynthe.Core.Turn.new(:actor, :facet)
      iex> turn = Absynthe.Dataflow.Field.set(x, 15, turn)
      iex> {_turn, _actions} = Absynthe.Core.Turn.commit(turn)
      iex> Absynthe.Dataflow.Field.get(doubled)
      30
  """
  @spec set(t(), term(), Turn.t()) :: Turn.t()
  def set(%__MODULE__{compute: nil} = field, new_value, %Turn{} = turn) do
    # Create an action to update the field
    update_action = fn ->
      perform_update(field, new_value)
    end

    Turn.add_action(turn, {:field_update, update_action})
  end

  def set(%__MODULE__{compute: compute}, _new_value, %Turn{} = _turn)
      when is_function(compute) do
    raise ArgumentError, "Cannot set value of computed field"
  end

  @doc """
  Performs the actual field update (called when Turn commits).

  This is an internal function that applies the update and propagates
  changes to dependents.

  ## Parameters

  - `field` - The field to update
  - `new_value` - The new value to assign

  ## Returns

  The updated field.
  """
  @spec perform_update(t(), term()) :: t()
  def perform_update(%__MODULE__{} = field, new_value) do
    # Increment version to invalidate dependent computations
    updated_field = %__MODULE__{
      field
      | value: new_value,
        version: field.version + 1
    }

    # Update in registry
    update_registry(updated_field)

    # Mark all dependents as dirty
    mark_dependents_dirty(updated_field)

    updated_field
  end

  # Dependency Management

  @doc """
  Adds a dependent field or observer to this field.

  When this field changes, all registered dependents are notified
  (marked dirty for recomputation).

  This is typically called automatically during dependency tracking,
  but can be used manually for explicit dependency management.

  ## Parameters

  - `field` - The field to add a dependent to
  - `dependent_id` - The ID of the dependent field

  ## Returns

  The updated field with the new dependent registered.

  ## Examples

      iex> source = Absynthe.Dataflow.Field.new("value")
      iex> observer_id = make_ref()
      iex> source = Absynthe.Dataflow.Field.add_dependent(source, observer_id)
      iex> MapSet.member?(source.dependents, observer_id)
      true
  """
  @spec add_dependent(t(), term()) :: t()
  def add_dependent(%__MODULE__{} = field, dependent_id) do
    updated_field = %__MODULE__{
      field
      | dependents: MapSet.put(field.dependents, dependent_id)
    }

    update_registry(updated_field)
  end

  @doc """
  Removes a dependent field or observer from this field.

  After removal, the dependent will no longer be notified when
  this field changes.

  ## Parameters

  - `field` - The field to remove a dependent from
  - `dependent_id` - The ID of the dependent to remove

  ## Returns

  The updated field with the dependent removed.

  ## Examples

      iex> source = Absynthe.Dataflow.Field.new("value")
      iex> observer_id = make_ref()
      iex> source = Absynthe.Dataflow.Field.add_dependent(source, observer_id)
      iex> source = Absynthe.Dataflow.Field.remove_dependent(source, observer_id)
      iex> MapSet.member?(source.dependents, observer_id)
      false
  """
  @spec remove_dependent(t(), term()) :: t()
  def remove_dependent(%__MODULE__{} = field, dependent_id) do
    updated_field = %__MODULE__{
      field
      | dependents: MapSet.delete(field.dependents, dependent_id)
    }

    update_registry(updated_field)
  end

  @doc """
  Checks if a field is dirty and needs recomputation.

  A field is dirty if:
  - It is a computed field marked as dirty
  - Any of its dependencies has a newer version than recorded

  Simple (non-computed) fields are never dirty.

  ## Parameters

  - `field` - The field to check

  ## Returns

  `true` if the field needs recomputation, `false` otherwise.

  ## Examples

      iex> simple = Absynthe.Dataflow.Field.new(42)
      iex> Absynthe.Dataflow.Field.dirty?(simple)
      false

      iex> x = Absynthe.Dataflow.Field.new(10)
      iex> computed = Absynthe.Dataflow.Field.computed(fn ->
      ...>   Absynthe.Dataflow.Field.get(x)
      ...> end)
      iex> # Before first read, computed field is dirty
      iex> Absynthe.Dataflow.Field.dirty?(computed)
      true
  """
  @spec dirty?(t()) :: boolean()
  def dirty?(%__MODULE__{compute: nil}), do: false

  def dirty?(%__MODULE__{dirty?: true}), do: true

  def dirty?(%__MODULE__{dependencies: dependencies, dependency_versions: dep_versions}) do
    # Check if any dependency has a newer version
    Enum.any?(dependencies, fn dep_id ->
      case lookup_field(dep_id) do
        nil ->
          false

        dep_field ->
          current_version = dep_field.version
          tracked_version = Map.get(dep_versions, dep_id, -1)
          current_version > tracked_version
      end
    end)
  end

  @doc """
  Forces recomputation of a field's value.

  For simple fields, this is a no-op and returns the field unchanged.

  For computed fields, this executes the computation function,
  tracks dependencies, and updates the value.

  This is typically called automatically during `get/1` when a field
  is dirty, but can be called manually to force recomputation.

  ## Parameters

  - `field` - The field to recompute

  ## Returns

  The field with updated value and dependency tracking.

  ## Examples

      iex> x = Absynthe.Dataflow.Field.new(5)
      iex> doubled = Absynthe.Dataflow.Field.computed(fn ->
      ...>   Absynthe.Dataflow.Field.get(x) * 2
      ...> end)
      iex> doubled = Absynthe.Dataflow.Field.recompute(doubled)
      iex> doubled.value
      10
  """
  @spec recompute(t()) :: t()
  def recompute(%__MODULE__{compute: nil} = field), do: field

  def recompute(%__MODULE__{compute: compute_fn} = field) when is_function(compute_fn, 0) do
    # Clear old dependencies
    clear_old_dependencies(field)

    # Track dependencies during computation
    Process.put(@computing_field_key, field.id)
    new_dependencies = MapSet.new()
    Process.put({@computing_field_key, :deps}, new_dependencies)

    # Execute computation
    new_value = compute_fn.()

    # Collect tracked dependencies
    tracked_deps = Process.get({@computing_field_key, :deps}, MapSet.new())

    # Record dependency versions
    dep_versions =
      tracked_deps
      |> Enum.map(fn dep_id ->
        case lookup_field(dep_id) do
          nil -> {dep_id, 0}
          dep_field -> {dep_id, dep_field.version}
        end
      end)
      |> Map.new()

    # Clean up process dictionary
    Process.delete(@computing_field_key)
    Process.delete({@computing_field_key, :deps})

    # Register this field as dependent of its dependencies
    Enum.each(tracked_deps, fn dep_id ->
      case lookup_field(dep_id) do
        nil -> :ok
        dep_field -> add_dependent(dep_field, field.id)
      end
    end)

    # Update field
    updated_field = %__MODULE__{
      field
      | value: new_value,
        version: field.version + 1,
        dependencies: tracked_deps,
        dependency_versions: dep_versions,
        dirty?: false
    }

    update_registry(updated_field)
  end

  # Internal Helpers

  # Recomputes field if it's dirty
  @spec maybe_recompute(t()) :: t()
  defp maybe_recompute(%__MODULE__{} = field) do
    # Refresh field from registry
    field = lookup_field(field.id) || field

    if dirty?(field) do
      recompute(field)
    else
      field
    end
  end

  # Registers this field as a dependency if another field is computing
  @spec maybe_register_dependency(t()) :: :ok
  defp maybe_register_dependency(%__MODULE__{id: field_id}) do
    case Process.get(@computing_field_key) do
      nil ->
        :ok

      computing_field_id when computing_field_id != field_id ->
        # Add this field to the dependencies of the computing field
        deps = Process.get({@computing_field_key, :deps}, MapSet.new())
        Process.put({@computing_field_key, :deps}, MapSet.put(deps, field_id))
        :ok

      _same_field ->
        # Don't register self-dependency
        :ok
    end
  end

  # Marks all dependent fields as dirty
  @spec mark_dependents_dirty(t()) :: :ok
  defp mark_dependents_dirty(%__MODULE__{dependents: dependents}) do
    Enum.each(dependents, fn dependent_id ->
      case lookup_field(dependent_id) do
        nil ->
          :ok

        %__MODULE__{} = dependent_field ->
          updated_dependent = %__MODULE__{dependent_field | dirty?: true}
          update_registry(updated_dependent)
          # Recursively mark dependents (for transitive propagation)
          mark_dependents_dirty(updated_dependent)
      end
    end)
  end

  # Removes this field from its dependencies' dependent lists
  @spec clear_old_dependencies(t()) :: :ok
  defp clear_old_dependencies(%__MODULE__{id: field_id, dependencies: dependencies}) do
    Enum.each(dependencies, fn dep_id ->
      case lookup_field(dep_id) do
        nil ->
          :ok

        dep_field ->
          remove_dependent(dep_field, field_id)
      end
    end)
  end

  # Query Functions

  @doc """
  Returns the unique identifier of a field.

  ## Parameters

  - `field` - The field to get the ID from

  ## Returns

  The field's unique identifier.

  ## Examples

      iex> field = Absynthe.Dataflow.Field.new("value")
      iex> id = Absynthe.Dataflow.Field.id(field)
      iex> is_reference(id)
      true
  """
  @spec id(t()) :: term()
  def id(%__MODULE__{id: id}), do: id

  @doc """
  Returns the current version number of a field.

  Version numbers are monotonically increasing and change whenever
  the field's value is updated.

  ## Parameters

  - `field` - The field to get the version from

  ## Returns

  A non-negative integer representing the current version.

  ## Examples

      iex> field = Absynthe.Dataflow.Field.new("initial")
      iex> Absynthe.Dataflow.Field.version(field)
      0
  """
  @spec version(t()) :: non_neg_integer()
  def version(%__MODULE__{version: version}), do: version

  @doc """
  Returns the set of field IDs that depend on this field.

  ## Parameters

  - `field` - The field to get dependents from

  ## Returns

  A `MapSet` of dependent field IDs.

  ## Examples

      iex> field = Absynthe.Dataflow.Field.new("value")
      iex> Absynthe.Dataflow.Field.dependents(field)
      #MapSet<[]>
  """
  @spec dependents(t()) :: MapSet.t()
  def dependents(%__MODULE__{dependents: dependents}), do: dependents

  @doc """
  Returns the set of field IDs this field depends on.

  For simple fields, this is always empty. For computed fields,
  this is populated during computation.

  ## Parameters

  - `field` - The field to get dependencies from

  ## Returns

  A `MapSet` of dependency field IDs.

  ## Examples

      iex> field = Absynthe.Dataflow.Field.new("value")
      iex> Absynthe.Dataflow.Field.dependencies(field)
      #MapSet<[]>
  """
  @spec dependencies(t()) :: MapSet.t()
  def dependencies(%__MODULE__{dependencies: dependencies}), do: dependencies

  @doc """
  Checks if a field is a computed field.

  ## Parameters

  - `field` - The field to check

  ## Returns

  `true` if the field has a computation function, `false` otherwise.

  ## Examples

      iex> simple = Absynthe.Dataflow.Field.new(42)
      iex> Absynthe.Dataflow.Field.computed?(simple)
      false

      iex> computed = Absynthe.Dataflow.Field.computed(fn -> 42 end)
      iex> Absynthe.Dataflow.Field.computed?(computed)
      true
  """
  @spec computed?(t()) :: boolean()
  def computed?(%__MODULE__{compute: nil}), do: false
  def computed?(%__MODULE__{compute: compute}) when is_function(compute), do: true
end
