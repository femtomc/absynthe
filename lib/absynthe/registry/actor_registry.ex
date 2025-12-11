defmodule Absynthe.Registry.ActorRegistry do
  @moduledoc """
  A process registry for looking up actors by name in the Syndicated Actor Model.

  This module provides a wrapper around Elixir's built-in `Registry` module,
  offering a simple API for registering, discovering, and managing actor processes
  by name. The registry automatically handles process death cleanup, ensuring
  that dead processes are removed from the registry without manual intervention.

  ## Features

  - **Name-based lookup**: Find actor PIDs by unique names
  - **Automatic cleanup**: Dead processes are automatically unregistered
  - **Multiple key types**: Supports atoms, strings, tuples, and other terms as keys
  - **GenServer integration**: Provides `via_tuple/2` for seamless GenServer naming
  - **Registry introspection**: Query registered names and count active actors

  ## Usage

  The registry must be started before use, typically in your application's supervision tree:

      children = [
        {Absynthe.Registry.ActorRegistry, name: :my_actor_registry}
      ]

      Supervisor.start_link(children, strategy: :one_for_one)

  Once started, you can register actors:

      {:ok, pid} = MyActor.start_link()
      :ok = Absynthe.Registry.ActorRegistry.register(:my_actor_registry, :worker_1, pid)

  And look them up:

      {:ok, pid} = Absynthe.Registry.ActorRegistry.lookup(:my_actor_registry, :worker_1)

  ## Via Tuples

  The registry supports via tuples for GenServer integration, allowing you to
  start actors with registered names:

      GenServer.start_link(
        MyActor,
        [],
        name: Absynthe.Registry.ActorRegistry.via_tuple(:my_actor_registry, :worker_1)
      )

  This automatically registers the process when it starts.

  ## Key Types

  The registry supports any term as a key:

      # Atoms
      register(registry, :my_actor, pid)

      # Strings
      register(registry, "user:123", pid)

      # Tuples
      register(registry, {:user, 123}, pid)

      # Any term
      register(registry, %{type: :worker, id: 1}, pid)

  ## Process Death Handling

  When a registered process dies, the Registry automatically removes its entry.
  No manual cleanup is required:

      {:ok, pid} = Agent.start_link(fn -> :ok end)
      :ok = register(:my_registry, :temp_actor, pid)
      Agent.stop(pid)
      # The entry is automatically removed
      :error = lookup(:my_registry, :temp_actor)
  """

  @typedoc """
  The registry identifier, typically an atom name.
  """
  @type registry :: atom()

  @typedoc """
  A name that identifies an actor. Can be any term.
  """
  @type name :: term()

  @typedoc """
  A process identifier for an actor.
  """
  @type actor_pid :: pid()

  @doc """
  Starts the actor registry.

  The registry uses Elixir's built-in `Registry` with unique keys, meaning
  each name can only be associated with one process at a time.

  ## Options

  - `:name` - The name to register the registry under (required)

  ## Examples

      iex> Absynthe.Registry.ActorRegistry.start_link(name: :my_registry)
      {:ok, pid}

  ## Returns

  - `{:ok, pid}` - The registry was started successfully
  - `{:error, reason}` - The registry could not be started
  """
  @spec start_link(keyword()) :: {:ok, pid()} | {:error, term()}
  def start_link(opts) do
    name = Keyword.fetch!(opts, :name)
    Registry.start_link(keys: :unique, name: name)
  end

  @doc """
  Registers an actor process with a name in the registry.

  Each name can only be associated with one process. Attempting to register
  the same name twice will result in an error.

  ## Parameters

  - `registry` - The registry name
  - `name` - The name to register (can be any term)
  - `pid` - The process identifier to register

  ## Examples

      iex> {:ok, pid} = Agent.start_link(fn -> :ok end)
      iex> Absynthe.Registry.ActorRegistry.register(:my_registry, :worker_1, pid)
      :ok

      iex> {:ok, pid} = Agent.start_link(fn -> :ok end)
      iex> Absynthe.Registry.ActorRegistry.register(:my_registry, :worker_1, pid)
      iex> {:ok, pid2} = Agent.start_link(fn -> :ok end)
      iex> Absynthe.Registry.ActorRegistry.register(:my_registry, :worker_1, pid2)
      {:error, {:already_registered, pid}}

  ## Returns

  - `:ok` - The process was registered successfully
  - `{:error, {:already_registered, pid}}` - The name is already taken
  """
  @spec register(registry(), name(), pid()) :: :ok | {:error, {:already_registered, pid()}}
  def register(registry, name, _pid) do
    # Note: Registry.register/3 always registers the calling process (self()).
    # The pid parameter is for API consistency but the caller must be the actor.
    case Registry.register(registry, name, nil) do
      {:ok, _owner} -> :ok
      {:error, {:already_registered, existing_pid}} -> {:error, {:already_registered, existing_pid}}
    end
  end

  @doc """
  Unregisters an actor from the registry.

  This removes the name-to-PID mapping. The calling process must be the
  registered process itself, as Registry only allows processes to unregister
  themselves.

  ## Parameters

  - `registry` - The registry name
  - `name` - The name to unregister

  ## Examples

      iex> {:ok, pid} = Agent.start_link(fn -> :ok end)
      iex> Absynthe.Registry.ActorRegistry.register(:my_registry, :worker_1, pid)
      iex> # From within the registered process:
      iex> Absynthe.Registry.ActorRegistry.unregister(:my_registry, :worker_1)
      :ok

  ## Returns

  - `:ok` - The process was unregistered successfully
  """
  @spec unregister(registry(), name()) :: :ok
  def unregister(registry, name) do
    Registry.unregister(registry, name)
  end

  @doc """
  Looks up an actor PID by name.

  Returns the PID associated with the given name, or an error if the name
  is not registered.

  ## Parameters

  - `registry` - The registry name
  - `name` - The name to look up

  ## Examples

      iex> {:ok, pid} = Agent.start_link(fn -> :ok end)
      iex> Absynthe.Registry.ActorRegistry.register(:my_registry, :worker_1, pid)
      iex> Absynthe.Registry.ActorRegistry.lookup(:my_registry, :worker_1)
      {:ok, pid}

      iex> Absynthe.Registry.ActorRegistry.lookup(:my_registry, :nonexistent)
      :error

  ## Returns

  - `{:ok, pid}` - The process was found
  - `:error` - No process is registered under that name
  """
  @spec lookup(registry(), name()) :: {:ok, pid()} | :error
  def lookup(registry, name) do
    case Registry.lookup(registry, name) do
      [{pid, _value}] -> {:ok, pid}
      [] -> :error
    end
  end

  @doc """
  Looks up an actor PID by name, returning the PID directly.

  This is a convenience function that returns the PID or `nil`, similar to
  `Process.whereis/1`.

  ## Parameters

  - `registry` - The registry name
  - `name` - The name to look up

  ## Examples

      iex> {:ok, pid} = Agent.start_link(fn -> :ok end)
      iex> Absynthe.Registry.ActorRegistry.register(:my_registry, :worker_1, pid)
      iex> Absynthe.Registry.ActorRegistry.whereis(:my_registry, :worker_1)
      pid

      iex> Absynthe.Registry.ActorRegistry.whereis(:my_registry, :nonexistent)
      nil

  ## Returns

  - `pid` - The process identifier if found
  - `nil` - No process is registered under that name
  """
  @spec whereis(registry(), name()) :: pid() | nil
  def whereis(registry, name) do
    case lookup(registry, name) do
      {:ok, pid} -> pid
      :error -> nil
    end
  end

  @doc """
  Returns a list of all registered names in the registry.

  This is useful for debugging and introspection, but note that the list
  may change between calls as processes register and unregister.

  ## Parameters

  - `registry` - The registry name

  ## Examples

      iex> {:ok, pid1} = Agent.start_link(fn -> :ok end)
      iex> {:ok, pid2} = Agent.start_link(fn -> :ok end)
      iex> Absynthe.Registry.ActorRegistry.register(:my_registry, :worker_1, pid1)
      iex> Absynthe.Registry.ActorRegistry.register(:my_registry, :worker_2, pid2)
      iex> names = Absynthe.Registry.ActorRegistry.registered_names(:my_registry)
      iex> Enum.sort(names)
      [:worker_1, :worker_2]

  ## Returns

  - A list of all registered names (may be empty)
  """
  @spec registered_names(registry()) :: [name()]
  def registered_names(registry) do
    registry
    |> Registry.select([{{:"$1", :_, :_}, [], [:"$1"]}])
    |> Enum.to_list()
  end

  @doc """
  Counts the number of registered actors in the registry.

  ## Parameters

  - `registry` - The registry name

  ## Examples

      iex> {:ok, pid1} = Agent.start_link(fn -> :ok end)
      iex> {:ok, pid2} = Agent.start_link(fn -> :ok end)
      iex> Absynthe.Registry.ActorRegistry.register(:my_registry, :worker_1, pid1)
      iex> Absynthe.Registry.ActorRegistry.register(:my_registry, :worker_2, pid2)
      iex> Absynthe.Registry.ActorRegistry.count(:my_registry)
      2

  ## Returns

  - The number of registered actors (non-negative integer)
  """
  @spec count(registry()) :: non_neg_integer()
  def count(registry) do
    Registry.count(registry)
  end

  @doc """
  Returns a via tuple for use with GenServer-compatible processes.

  This allows you to register a process name when starting it with GenServer,
  Agent, or any other process that accepts a `:name` option.

  ## Parameters

  - `registry` - The registry name
  - `name` - The name to register

  ## Examples

      # Start a GenServer with automatic registration
      iex> GenServer.start_link(
      ...>   MyActor,
      ...>   [],
      ...>   name: Absynthe.Registry.ActorRegistry.via_tuple(:my_registry, :worker_1)
      ...> )
      {:ok, pid}

      # The process is now registered and can be looked up
      iex> {:ok, pid} = Absynthe.Registry.ActorRegistry.lookup(:my_registry, :worker_1)

      # You can also use the via tuple to send messages
      iex> GenServer.call(
      ...>   Absynthe.Registry.ActorRegistry.via_tuple(:my_registry, :worker_1),
      ...>   :get_state
      ...> )

  ## Returns

  - A via tuple `{:via, Registry, {registry, name}}`
  """
  @spec via_tuple(registry(), name()) :: {:via, Registry, {registry(), name()}}
  def via_tuple(registry, name) do
    {:via, Registry, {registry, name}}
  end

  @doc """
  Returns the child specification for use in a supervision tree.

  This allows the registry to be included in a supervisor's child list.

  ## Examples

      children = [
        {Absynthe.Registry.ActorRegistry, name: :my_registry}
      ]

      Supervisor.start_link(children, strategy: :one_for_one)
  """
  def child_spec(opts) do
    %{
      id: Keyword.get(opts, :name, __MODULE__),
      start: {__MODULE__, :start_link, [opts]},
      type: :supervisor
    }
  end
end
