defmodule Absynthe.Core.Facet do
  @moduledoc """
  Conversational scope for actors in the Syndicated Actor Model.

  A Facet represents a conversational context or subsystem within an actor.
  Facets form a hierarchical tree structure that enables structured concurrency
  patterns and fate-sharing semantics.

  ## Overview

  In the Syndicated Actor Model, actors are single-threaded and maintain
  internal state. Facets provide a way to organize this state into logical
  scopes, each representing a conversation or interaction with other actors.

  ## Hierarchy and Fate-Sharing

  Every actor has a root facet. Additional facets can be created as children
  of existing facets, forming a tree hierarchy. When a parent facet stops,
  all of its descendant facets are also stopped - this is called fate-sharing.

  This enables:
  - Structured teardown of complex subsystems
  - Automatic cleanup when conversations end
  - Hierarchical resource management

  ## Entities and Assertions

  Each facet owns a set of entities. Entities are the basic unit of behavior
  within a facet - they handle events and make assertions to the dataspace.

  When a facet stops:
  - All its entities are cleaned up
  - All outbound assertions made by the facet are retracted
  - All child facets are stopped (recursively)

  ## State Management

  Facets are pure data structures stored in actor state. They do not contain
  processes or mutable state. All facet operations return updated facet values
  that must be stored back in the actor state.

  ## Example

      # Create a root facet
      root = Facet.new(:root)

      # Add a child facet for a subsystem
      child = Facet.new(:http_server, :root)
      root = Facet.add_child(root, :http_server)

      # Register an entity in the child facet
      entity = %MyEntity{}
      child = Facet.add_entity(child, :my_entity, entity)

      # When the child facet stops, its entity is cleaned up
      child = Facet.stop(child)
      assert child.alive? == false
  """

  @type facet_id :: any()
  @type entity_id :: any()
  @type entity :: any()
  @type handle :: any()

  @type t :: %__MODULE__{
          id: facet_id(),
          parent_id: facet_id() | nil,
          children: MapSet.t(facet_id()),
          entities: %{entity_id() => entity()},
          outbound_handles: MapSet.t(handle()),
          alive?: boolean(),
          # Counter for preventing inertness checks during setup
          inert_check_preventers: non_neg_integer(),
          # Set of linked task references (for future async task support)
          linked_tasks: MapSet.t(reference())
        }

  defstruct [
    :id,
    :parent_id,
    children: MapSet.new(),
    entities: %{},
    outbound_handles: MapSet.new(),
    alive?: true,
    inert_check_preventers: 0,
    linked_tasks: MapSet.new()
  ]

  @doc """
  Creates a new facet.

  ## Parameters

    - `id` - Unique identifier for the facet within the actor
    - `parent_id` - Optional parent facet ID. Use `nil` for root facets.

  ## Returns

  A new `Facet` struct with the given ID and parent.

  ## Examples

      iex> Facet.new(:root)
      %Facet{id: :root, parent_id: nil, alive?: true}

      iex> Facet.new(:child, :root)
      %Facet{id: :child, parent_id: :root, alive?: true}
  """
  @spec new(facet_id(), facet_id() | nil) :: t()
  def new(id, parent_id \\ nil) do
    %__MODULE__{
      id: id,
      parent_id: parent_id,
      children: MapSet.new(),
      entities: %{},
      outbound_handles: MapSet.new(),
      alive?: true
    }
  end

  @doc """
  Adds a child facet ID to this facet's children set.

  This creates the parent-child relationship in the facet hierarchy.
  The child facet must be created separately with `new/2`.

  ## Parameters

    - `facet` - The parent facet
    - `child_id` - The ID of the child facet to add

  ## Returns

  Updated facet with the child added to its children set.

  ## Examples

      iex> root = Facet.new(:root)
      iex> root = Facet.add_child(root, :child1)
      iex> MapSet.member?(root.children, :child1)
      true
  """
  @spec add_child(t(), facet_id()) :: t()
  def add_child(%__MODULE__{} = facet, child_id) do
    %{facet | children: MapSet.put(facet.children, child_id)}
  end

  @doc """
  Removes a child facet ID from this facet's children set.

  This breaks the parent-child relationship. The child facet itself
  is not modified - it must be removed from actor state separately.

  ## Parameters

    - `facet` - The parent facet
    - `child_id` - The ID of the child facet to remove

  ## Returns

  Updated facet with the child removed from its children set.

  ## Examples

      iex> root = Facet.new(:root) |> Facet.add_child(:child1)
      iex> root = Facet.remove_child(root, :child1)
      iex> MapSet.member?(root.children, :child1)
      false
  """
  @spec remove_child(t(), facet_id()) :: t()
  def remove_child(%__MODULE__{} = facet, child_id) do
    %{facet | children: MapSet.delete(facet.children, child_id)}
  end

  @doc """
  Registers an entity in this facet.

  Entities are stored in a map keyed by entity ID. Each entity
  represents a unit of behavior within the facet's conversational scope.

  ## Parameters

    - `facet` - The facet to add the entity to
    - `entity_id` - Unique identifier for the entity within this facet
    - `entity` - The entity data structure

  ## Returns

  Updated facet with the entity added to its entities map.

  ## Examples

      iex> facet = Facet.new(:my_facet)
      iex> entity = %{type: :handler, state: :idle}
      iex> facet = Facet.add_entity(facet, :handler1, entity)
      iex> facet.entities[:handler1]
      %{type: :handler, state: :idle}
  """
  @spec add_entity(t(), entity_id(), entity()) :: t()
  def add_entity(%__MODULE__{} = facet, entity_id, entity) do
    %{facet | entities: Map.put(facet.entities, entity_id, entity)}
  end

  @doc """
  Retrieves an entity from this facet by its ID.

  ## Parameters

    - `facet` - The facet to lookup the entity in
    - `entity_id` - The entity identifier to lookup

  ## Returns

  The entity if found, or `nil` if not present.

  ## Examples

      iex> facet = Facet.new(:my_facet)
      iex> entity = %{type: :handler}
      iex> facet = Facet.add_entity(facet, :handler1, entity)
      iex> Facet.get_entity(facet, :handler1)
      %{type: :handler}

      iex> facet = Facet.new(:my_facet)
      iex> Facet.get_entity(facet, :nonexistent)
      nil
  """
  @spec get_entity(t(), entity_id()) :: entity() | nil
  def get_entity(%__MODULE__{} = facet, entity_id) do
    Map.get(facet.entities, entity_id)
  end

  @doc """
  Removes an entity from this facet.

  ## Parameters

    - `facet` - The facet to remove the entity from
    - `entity_id` - The entity identifier to remove

  ## Returns

  Updated facet with the entity removed from its entities map.

  ## Examples

      iex> facet = Facet.new(:my_facet)
      iex> facet = Facet.add_entity(facet, :handler1, %{})
      iex> facet = Facet.remove_entity(facet, :handler1)
      iex> Facet.get_entity(facet, :handler1)
      nil
  """
  @spec remove_entity(t(), entity_id()) :: t()
  def remove_entity(%__MODULE__{} = facet, entity_id) do
    %{facet | entities: Map.delete(facet.entities, entity_id)}
  end

  @doc """
  Adds a handle to this facet's outbound handles set.

  Handles represent assertions that this facet has made to the dataspace.
  When the facet stops, these handles are used to retract all assertions.

  ## Parameters

    - `facet` - The facet to add the handle to
    - `handle` - An assertion handle to track

  ## Returns

  Updated facet with the handle added to its outbound handles set.

  ## Examples

      iex> facet = Facet.new(:my_facet)
      iex> handle = {:assert, :my_assertion, 123}
      iex> facet = Facet.add_handle(facet, handle)
      iex> MapSet.member?(facet.outbound_handles, handle)
      true
  """
  @spec add_handle(t(), handle()) :: t()
  def add_handle(%__MODULE__{} = facet, handle) do
    %{facet | outbound_handles: MapSet.put(facet.outbound_handles, handle)}
  end

  @doc """
  Removes a handle from this facet's outbound handles set.

  ## Parameters

    - `facet` - The facet to remove the handle from
    - `handle` - The assertion handle to remove

  ## Returns

  Updated facet with the handle removed from its outbound handles set.

  ## Examples

      iex> facet = Facet.new(:my_facet)
      iex> handle = {:assert, :my_assertion, 123}
      iex> facet = Facet.add_handle(facet, handle)
      iex> facet = Facet.remove_handle(facet, handle)
      iex> MapSet.member?(facet.outbound_handles, handle)
      false
  """
  @spec remove_handle(t(), handle()) :: t()
  def remove_handle(%__MODULE__{} = facet, handle) do
    %{facet | outbound_handles: MapSet.delete(facet.outbound_handles, handle)}
  end

  @doc """
  Marks a facet as stopped.

  This sets `alive?` to `false`, indicating that the facet is no longer
  active. The facet's entities and handles are preserved for cleanup
  purposes - the actor must handle entity cleanup and assertion retraction.

  When a facet is stopped, its children should also be stopped recursively
  to implement fate-sharing semantics. This is typically handled by the
  actor managing the facets.

  ## Parameters

    - `facet` - The facet to stop

  ## Returns

  Updated facet with `alive?` set to `false`.

  ## Examples

      iex> facet = Facet.new(:my_facet)
      iex> facet.alive?
      true

      iex> facet = Facet.stop(facet)
      iex> facet.alive?
      false
  """
  @spec stop(t()) :: t()
  def stop(%__MODULE__{} = facet) do
    %{facet | alive?: false}
  end

  @doc """
  Checks if a facet is alive.

  ## Parameters

    - `facet` - The facet to check

  ## Returns

  `true` if the facet is alive, `false` otherwise.

  ## Examples

      iex> facet = Facet.new(:my_facet)
      iex> Facet.alive?(facet)
      true

      iex> facet = Facet.stop(facet)
      iex> Facet.alive?(facet)
      false
  """
  @spec alive?(t()) :: boolean()
  def alive?(%__MODULE__{alive?: alive?}), do: alive?

  @doc """
  Returns all entity IDs in this facet.

  ## Parameters

    - `facet` - The facet to get entity IDs from

  ## Returns

  A list of entity IDs.

  ## Examples

      iex> facet = Facet.new(:my_facet)
      iex> facet = Facet.add_entity(facet, :e1, %{})
      iex> facet = Facet.add_entity(facet, :e2, %{})
      iex> Facet.entity_ids(facet) |> Enum.sort()
      [:e1, :e2]
  """
  @spec entity_ids(t()) :: [entity_id()]
  def entity_ids(%__MODULE__{entities: entities}) do
    Map.keys(entities)
  end

  @doc """
  Returns all child facet IDs.

  ## Parameters

    - `facet` - The facet to get child IDs from

  ## Returns

  A MapSet of child facet IDs.

  ## Examples

      iex> facet = Facet.new(:root)
      iex> facet = Facet.add_child(facet, :child1)
      iex> facet = Facet.add_child(facet, :child2)
      iex> Facet.children(facet) |> MapSet.to_list() |> Enum.sort()
      [:child1, :child2]
  """
  @spec children(t()) :: MapSet.t(facet_id())
  def children(%__MODULE__{children: children}), do: children

  @doc """
  Returns all outbound handles for this facet.

  ## Parameters

    - `facet` - The facet to get handles from

  ## Returns

  A MapSet of outbound handles.

  ## Examples

      iex> facet = Facet.new(:my_facet)
      iex> facet = Facet.add_handle(facet, :handle1)
      iex> facet = Facet.add_handle(facet, :handle2)
      iex> Facet.handles(facet) |> MapSet.to_list() |> Enum.sort()
      [:handle1, :handle2]
  """
  @spec handles(t()) :: MapSet.t(handle())
  def handles(%__MODULE__{outbound_handles: handles}), do: handles

  # Inertness Detection

  @doc """
  Checks if a facet is inert (has no work to do).

  A facet is considered inert if ALL of the following are true:
  - It is alive (dead facets are not "inert" - they're terminated)
  - It has no child facets
  - It has no outbound assertion handles
  - It has no linked tasks
  - It has no active inert-check preventers

  Inert facets should be automatically terminated to clean up unused
  conversational scopes. When a facet becomes inert, it should be
  stopped, and this may cascade to parent facets if they also become
  inert as a result.

  ## Parameters

    - `facet` - The facet to check

  ## Returns

  `true` if the facet is inert, `false` otherwise.

  ## Examples

      iex> facet = Facet.new(:my_facet)
      iex> Facet.is_inert?(facet)
      true

      iex> facet = Facet.new(:my_facet) |> Facet.add_child(:child)
      iex> Facet.is_inert?(facet)
      false

      iex> facet = Facet.new(:my_facet) |> Facet.add_handle(:h1)
      iex> Facet.is_inert?(facet)
      false
  """
  @spec is_inert?(t()) :: boolean()
  def is_inert?(%__MODULE__{alive?: false}), do: false

  def is_inert?(%__MODULE__{
        alive?: true,
        children: children,
        outbound_handles: handles,
        linked_tasks: tasks,
        inert_check_preventers: preventers
      }) do
    MapSet.size(children) == 0 and
      MapSet.size(handles) == 0 and
      MapSet.size(tasks) == 0 and
      preventers == 0
  end

  @doc """
  Prevents inertness checks from succeeding for this facet.

  Returns a tuple of `{token, updated_facet}` where the token can be used
  with `allow_inert_check/2` to re-enable inertness checks.

  This is used during facet boot to prevent premature garbage collection
  while the facet is still being set up (e.g., while wiring up assertions
  or spawning entities).

  ## Parameters

    - `facet` - The facet to prevent inertness checks for

  ## Returns

  A tuple of `{token, updated_facet}` where token is a unique reference.

  ## Examples

      iex> facet = Facet.new(:my_facet)
      iex> {token, facet} = Facet.prevent_inert_check(facet)
      iex> Facet.is_inert?(facet)
      false
      iex> {_, facet} = Facet.allow_inert_check(facet, token)
      iex> Facet.is_inert?(facet)
      true
  """
  @spec prevent_inert_check(t()) :: {reference(), t()}
  def prevent_inert_check(%__MODULE__{inert_check_preventers: count} = facet) do
    token = make_ref()
    {token, %{facet | inert_check_preventers: count + 1}}
  end

  @doc """
  Re-enables inertness checks after a previous `prevent_inert_check/1`.

  This decrements the facet's preventer counter. When the counter reaches zero,
  the facet becomes eligible for automatic inertness termination.

  Note: The token parameter is required for API symmetry with `prevent_inert_check/1`
  but is not validated. Callers are responsible for ensuring balanced
  prevent/allow calls.

  ## Parameters

    - `facet` - The facet to allow inertness checks for
    - `token` - The token from `prevent_inert_check/1` (kept for API symmetry)

  ## Returns

  A tuple of `{:ok, updated_facet}` or `{:error, :no_preventers}`.

  ## Examples

      iex> facet = Facet.new(:my_facet)
      iex> {token, facet} = Facet.prevent_inert_check(facet)
      iex> {:ok, facet} = Facet.allow_inert_check(facet, token)
      iex> facet.inert_check_preventers
      0
  """
  @spec allow_inert_check(t(), reference()) :: {:ok, t()} | {:error, :no_preventers}
  def allow_inert_check(%__MODULE__{inert_check_preventers: 0}, _token) do
    {:error, :no_preventers}
  end

  def allow_inert_check(%__MODULE__{inert_check_preventers: count} = facet, _token) do
    {:ok, %{facet | inert_check_preventers: count - 1}}
  end

  # Linked Tasks (for future async task support)

  @doc """
  Links a task to this facet.

  Linked tasks prevent the facet from becoming inert until they complete.
  This is used for async operations that should keep the facet alive.

  ## Parameters

    - `facet` - The facet to link the task to
    - `task_ref` - A reference identifying the task

  ## Returns

  Updated facet with the task linked.
  """
  @spec link_task(t(), reference()) :: t()
  def link_task(%__MODULE__{linked_tasks: tasks} = facet, task_ref) do
    %{facet | linked_tasks: MapSet.put(tasks, task_ref)}
  end

  @doc """
  Unlinks a task from this facet.

  ## Parameters

    - `facet` - The facet to unlink the task from
    - `task_ref` - The reference of the task to unlink

  ## Returns

  Updated facet with the task unlinked.
  """
  @spec unlink_task(t(), reference()) :: t()
  def unlink_task(%__MODULE__{linked_tasks: tasks} = facet, task_ref) do
    %{facet | linked_tasks: MapSet.delete(tasks, task_ref)}
  end

  @doc """
  Checks if the facet has any linked tasks.

  ## Parameters

    - `facet` - The facet to check

  ## Returns

  `true` if the facet has linked tasks, `false` otherwise.
  """
  @spec has_linked_tasks?(t()) :: boolean()
  def has_linked_tasks?(%__MODULE__{linked_tasks: tasks}) do
    MapSet.size(tasks) > 0
  end
end
