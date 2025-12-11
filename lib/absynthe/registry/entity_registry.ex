defmodule Absynthe.Registry.EntityRegistry do
  @moduledoc """
  A high-performance registry for looking up entities across actors in the Syndicated Actor Model.

  The EntityRegistry provides a centralized registry that maps entity references to their
  metadata across all actors in the system. This enables fast lookups, reverse queries
  (finding all entities for an actor), and distributed communication between actors.

  ## Architecture

  The registry uses an ETS (Erlang Term Storage) table with read concurrency optimizations
  for fast concurrent reads. The table is owned by a GenServer process that manages writes
  and ensures crash recovery through supervision.

  ## Key Structure

  Entities are stored with composite keys: `{actor_id, entity_id}`. This allows:
  - Fast lookups by both actor and entity ID
  - Efficient batch operations on all entities for a given actor
  - Natural cleanup when actors terminate

  ## Metadata

  Each registered entity can store arbitrary metadata (e.g., entity type, capabilities,
  routing information). This metadata is cached in the registry for fast access without
  needing to query the actor directly.

  ## Concurrency

  The registry is designed for high concurrency:
  - Reads are lock-free and can happen from any process
  - Writes are serialized through the GenServer
  - The ETS table uses `:read_concurrency` for optimized parallel reads

  ## Usage

  The registry is typically started under a supervisor and referenced by name:

      children = [
        {Absynthe.Registry.EntityRegistry, name: MyApp.EntityRegistry}
      ]

  Entities are registered when they are spawned:

      Absynthe.Registry.EntityRegistry.register(
        MyApp.EntityRegistry,
        actor_id,
        entity_id,
        %{type: :worker, pid: self()}
      )

  And can be looked up using either raw IDs or Ref structs:

      metadata = Absynthe.Registry.EntityRegistry.lookup(
        MyApp.EntityRegistry,
        actor_id,
        entity_id
      )

      # Or using a Ref
      ref = Absynthe.Core.Ref.new(actor_id, entity_id)
      metadata = Absynthe.Registry.EntityRegistry.lookup_by_ref(
        MyApp.EntityRegistry,
        ref
      )

  ## Cleanup

  When an actor terminates, all its entities should be cleaned up:

      Absynthe.Registry.EntityRegistry.unregister_actor(
        MyApp.EntityRegistry,
        actor_id
      )

  ## Examples

      # Start the registry
      {:ok, registry} = Absynthe.Registry.EntityRegistry.start_link(name: :my_registry)

      # Register an entity
      :ok = Absynthe.Registry.EntityRegistry.register(
        :my_registry,
        :actor_1,
        :entity_1,
        %{type: :dataspace, created_at: DateTime.utc_now()}
      )

      # Look up the entity
      {:ok, metadata} = Absynthe.Registry.EntityRegistry.lookup(
        :my_registry,
        :actor_1,
        :entity_1
      )

      # List all entities for an actor
      entity_ids = Absynthe.Registry.EntityRegistry.entities_for_actor(
        :my_registry,
        :actor_1
      )

      # Get total count
      count = Absynthe.Registry.EntityRegistry.count(:my_registry)

      # Unregister a single entity
      :ok = Absynthe.Registry.EntityRegistry.unregister(
        :my_registry,
        :actor_1,
        :entity_1
      )

      # Unregister all entities for an actor
      :ok = Absynthe.Registry.EntityRegistry.unregister_actor(
        :my_registry,
        :actor_1
      )
  """

  use GenServer

  alias Absynthe.Core.Ref

  @typedoc """
  A reference to the registry process.

  Can be a PID, registered name (atom), or `{:via, module, term}` tuple.
  """
  @type registry :: GenServer.server()

  @typedoc """
  Arbitrary metadata associated with an entity.

  Metadata can be any term and is typically used to cache information about
  the entity (e.g., type, capabilities, process ID, routing information).
  """
  @type metadata :: term()

  # Client API

  @doc """
  Starts the entity registry.

  ## Options

  - `:name` - The name to register the process under (optional but recommended)
  - All other options are passed to `GenServer.start_link/3`

  ## Examples

      # Start with a registered name
      {:ok, pid} = Absynthe.Registry.EntityRegistry.start_link(name: :my_registry)

      # Start without a name
      {:ok, pid} = Absynthe.Registry.EntityRegistry.start_link([])
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    {name_opts, gen_server_opts} = Keyword.split(opts, [:name])

    case name_opts[:name] do
      nil -> GenServer.start_link(__MODULE__, opts, gen_server_opts)
      name -> GenServer.start_link(__MODULE__, opts, [{:name, name} | gen_server_opts])
    end
  end

  @doc """
  Registers an entity in the registry.

  If an entity with the same `actor_id` and `entity_id` already exists, its
  metadata will be replaced with the new metadata.

  ## Parameters

  - `registry` - The registry process (PID or registered name)
  - `actor_id` - The ID of the actor that owns the entity
  - `entity_id` - The ID of the entity within that actor
  - `metadata` - Arbitrary metadata to associate with the entity

  ## Returns

  `:ok` on success.

  ## Examples

      :ok = Absynthe.Registry.EntityRegistry.register(
        :my_registry,
        :actor_1,
        :entity_1,
        %{type: :worker, pid: self()}
      )

      # Update metadata for an existing entity
      :ok = Absynthe.Registry.EntityRegistry.register(
        :my_registry,
        :actor_1,
        :entity_1,
        %{type: :worker, pid: self(), updated_at: DateTime.utc_now()}
      )
  """
  @spec register(registry(), term(), term(), metadata()) :: :ok
  def register(registry, actor_id, entity_id, metadata) do
    GenServer.call(registry, {:register, actor_id, entity_id, metadata})
  end

  @doc """
  Unregisters a single entity from the registry.

  If the entity does not exist, this operation succeeds silently.

  ## Parameters

  - `registry` - The registry process (PID or registered name)
  - `actor_id` - The ID of the actor that owns the entity
  - `entity_id` - The ID of the entity to unregister

  ## Returns

  `:ok` on success.

  ## Examples

      :ok = Absynthe.Registry.EntityRegistry.unregister(
        :my_registry,
        :actor_1,
        :entity_1
      )
  """
  @spec unregister(registry(), term(), term()) :: :ok
  def unregister(registry, actor_id, entity_id) do
    GenServer.call(registry, {:unregister, actor_id, entity_id})
  end

  @doc """
  Unregisters all entities for a given actor.

  This is typically called when an actor terminates to clean up all its
  entities at once. If the actor has no registered entities, this operation
  succeeds silently.

  ## Parameters

  - `registry` - The registry process (PID or registered name)
  - `actor_id` - The ID of the actor whose entities should be removed

  ## Returns

  `:ok` on success.

  ## Examples

      :ok = Absynthe.Registry.EntityRegistry.unregister_actor(
        :my_registry,
        :actor_1
      )
  """
  @spec unregister_actor(registry(), term()) :: :ok
  def unregister_actor(registry, actor_id) do
    GenServer.call(registry, {:unregister_actor, actor_id})
  end

  @doc """
  Looks up an entity's metadata by actor ID and entity ID.

  ## Parameters

  - `registry` - The registry process (PID or registered name)
  - `actor_id` - The ID of the actor that owns the entity
  - `entity_id` - The ID of the entity to look up

  ## Returns

  - `{:ok, metadata}` if the entity is found
  - `:error` if the entity is not registered

  ## Examples

      {:ok, metadata} = Absynthe.Registry.EntityRegistry.lookup(
        :my_registry,
        :actor_1,
        :entity_1
      )

      :error = Absynthe.Registry.EntityRegistry.lookup(
        :my_registry,
        :actor_1,
        :nonexistent_entity
      )
  """
  @spec lookup(registry(), term(), term()) :: {:ok, metadata()} | :error
  def lookup(registry, actor_id, entity_id) do
    table = get_table(registry)

    case :ets.lookup(table, {actor_id, entity_id}) do
      [{{^actor_id, ^entity_id}, metadata}] -> {:ok, metadata}
      [] -> :error
    end
  end

  @doc """
  Looks up an entity's metadata using a Ref struct.

  This is a convenience function that extracts the actor ID and entity ID
  from a Ref and calls `lookup/3`.

  ## Parameters

  - `registry` - The registry process (PID or registered name)
  - `ref` - A `Absynthe.Core.Ref` struct

  ## Returns

  - `{:ok, metadata}` if the entity is found
  - `:error` if the entity is not registered

  ## Examples

      ref = Absynthe.Core.Ref.new(:actor_1, :entity_1)
      {:ok, metadata} = Absynthe.Registry.EntityRegistry.lookup_by_ref(
        :my_registry,
        ref
      )
  """
  @spec lookup_by_ref(registry(), Ref.t()) :: {:ok, metadata()} | :error
  def lookup_by_ref(registry, %Ref{} = ref) do
    lookup(registry, Ref.actor_id(ref), Ref.entity_id(ref))
  end

  @doc """
  Returns a list of all entity IDs for a given actor.

  This is useful for finding all entities owned by an actor, for example
  when broadcasting a message to all entities in an actor.

  ## Parameters

  - `registry` - The registry process (PID or registered name)
  - `actor_id` - The ID of the actor

  ## Returns

  A list of entity IDs (possibly empty if the actor has no registered entities).

  ## Examples

      entity_ids = Absynthe.Registry.EntityRegistry.entities_for_actor(
        :my_registry,
        :actor_1
      )
      # => [:entity_1, :entity_2, :entity_3]

      # Actor with no entities
      entity_ids = Absynthe.Registry.EntityRegistry.entities_for_actor(
        :my_registry,
        :nonexistent_actor
      )
      # => []
  """
  @spec entities_for_actor(registry(), term()) :: [term()]
  def entities_for_actor(registry, actor_id) do
    table = get_table(registry)

    # Use match specification to find all entries where the key's first element
    # matches the actor_id
    match_spec = [
      {{{:"$1", :"$2"}, :_}, [{:==, :"$1", actor_id}], [:"$2"]}
    ]

    :ets.select(table, match_spec)
  end

  @doc """
  Returns the total number of registered entities across all actors.

  ## Parameters

  - `registry` - The registry process (PID or registered name)

  ## Returns

  The total count of registered entities as a non-negative integer.

  ## Examples

      count = Absynthe.Registry.EntityRegistry.count(:my_registry)
      # => 42
  """
  @spec count(registry()) :: non_neg_integer()
  def count(registry) do
    table = get_table(registry)
    :ets.info(table, :size)
  end

  # Server Callbacks

  @impl true
  def init(opts) do
    # Create the ETS table with read_concurrency for fast parallel reads
    table_name = Keyword.get(opts, :table_name, __MODULE__)

    table =
      :ets.new(table_name, [
        :set,
        :named_table,
        :public,
        :read_concurrency,
        {:write_concurrency, false}
      ])

    {:ok, %{table: table}}
  end

  @impl true
  def handle_call({:register, actor_id, entity_id, metadata}, _from, state) do
    :ets.insert(state.table, {{actor_id, entity_id}, metadata})
    {:reply, :ok, state}
  end

  @impl true
  def handle_call({:unregister, actor_id, entity_id}, _from, state) do
    :ets.delete(state.table, {actor_id, entity_id})
    {:reply, :ok, state}
  end

  @impl true
  def handle_call({:unregister_actor, actor_id}, _from, state) do
    # Use match specification to delete all entries where the key's first
    # element matches the actor_id
    match_spec = [
      {{{:"$1", :_}, :_}, [{:==, :"$1", actor_id}], [true]}
    ]

    :ets.select_delete(state.table, match_spec)
    {:reply, :ok, state}
  end

  # Handle the get_table call for PID-based registries
  @impl true
  def handle_call(:get_table, _from, state) do
    {:reply, state.table, state}
  end

  # Private Helpers

  # Gets the ETS table name from the registry process
  defp get_table(registry) when is_atom(registry) do
    # When using a named GenServer, we also use the same name for the ETS table
    registry
  end

  defp get_table(registry) when is_pid(registry) do
    # When using a PID, we need to get the table from the state
    GenServer.call(registry, :get_table)
  end
end
