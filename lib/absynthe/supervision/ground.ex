defmodule Absynthe.Supervision.Ground do
  @moduledoc """
  Root supervisor for the Absynthe application.

  ## Overview

  Ground is the top-level supervisor that orchestrates all Absynthe subsystems.
  It establishes the foundational supervision tree that manages the lifecycle
  of actors, entities, and their associated registries.

  The name "Ground" reflects its role as the foundational layer upon which the
  entire Absynthe actor system is built - much like electrical ground provides
  a common reference point, this supervisor provides a stable base for all
  actor operations.

  ## Supervision Tree Structure

  The Ground supervisor uses a `:one_for_one` strategy and starts children
  in a carefully ordered sequence:

  ```
  Ground (one_for_one)
  ├─ ActorRegistry    - Registry for looking up actors by name
  ├─ EntityRegistry   - Registry for looking up entities
  └─ ActorSupervisor  - DynamicSupervisor for actor processes
  ```

  ### Supervision Strategy

  The `:one_for_one` strategy means that if any child process terminates,
  only that process is restarted. This is appropriate because:

  1. **Registry Independence** - Each registry operates independently. If one
     fails, the other can continue serving requests.

  2. **Actor Isolation** - The ActorSupervisor can restart independently
     without affecting the registries. Registered actor names will be cleaned
     up automatically when processes die.

  3. **Fault Tolerance** - A crash in one subsystem doesn't cascade to others,
     maximizing system availability.

  ### Child Startup Order

  Children are started in the order they appear in the child specification list:

  1. **ActorRegistry** - Started first so that actors can register names
     immediately upon startup.

  2. **EntityRegistry** - Started second to enable entity lookups as soon
     as actors begin spawning entities.

  3. **ActorSupervisor** - Started last because it depends on the registries
     being available to properly manage actor lifecycle.

  This ordering ensures that infrastructure is in place before components
  that depend on it are started.

  ## Configuration

  Ground accepts the following configuration options:

  - `:name` - The registered name for the Ground supervisor itself.
    Default: `Absynthe.Supervision.Ground`

  - `:actor_supervisor_name` - The registered name for the ActorSupervisor.
    Default: `Absynthe.Supervision.ActorSupervisor`

  - `:actor_registry_name` - The registered name for the ActorRegistry.
    Default: `Absynthe.Registry.Actor`

  - `:entity_registry_name` - The registered name for the EntityRegistry.
    Default: `Absynthe.Registry.Entity`

  ## Usage

  ### Starting Ground as part of an Application

      def start(_type, _args) do
        children = [
          {Absynthe.Supervision.Ground, []}
        ]

        opts = [strategy: :one_for_one, name: MyApp.Supervisor]
        Supervisor.start_link(children, opts)
      end

  ### Starting Ground with custom configuration

      {:ok, ground} = Absynthe.Supervision.Ground.start_link(
        name: MyApp.Ground,
        actor_registry_name: MyApp.ActorRegistry
      )

  ### Starting Ground standalone

      # Use default names
      {:ok, ground} = Absynthe.Supervision.Ground.start_link([])

  ## Implementation Notes

  ### Registry Design

  Both ActorRegistry and EntityRegistry are expected to be implemented using
  Elixir's built-in `Registry` module with the following characteristics:

  - **ActorRegistry** - A `:unique` registry mapping actor names to PIDs.
    This ensures each actor name maps to exactly one process.

  - **EntityRegistry** - A `:unique` registry mapping entity references to
    their host actor PIDs. This enables fast entity lookup without scanning
    all actors.

  ### ActorSupervisor Design

  The ActorSupervisor is expected to be a `DynamicSupervisor` that:

  - Manages actor processes with a `:one_for_one` restart strategy
  - Allows actors to be started and stopped dynamically at runtime
  - Provides restart limits to prevent cascade failures
  - Integrates with ActorRegistry for name registration

  ### Forward Compatibility

  This module is designed to gracefully handle cases where registry or
  supervisor modules don't exist yet. If a child module is not yet
  implemented, the supervisor will fail to start with a descriptive error,
  making it clear what needs to be implemented next.

  ## Error Handling

  If Ground fails to start, it may be due to:

  1. **Missing Dependencies** - One or more child modules (ActorRegistry,
     EntityRegistry, ActorSupervisor) are not yet implemented.

  2. **Name Conflicts** - A process is already registered with one of the
     configured names.

  3. **Resource Exhaustion** - The system is unable to allocate resources
     for the supervisor or its children.

  Check the logs for specific error messages that indicate which component
  failed to start.

  ## Future Extensions

  As Absynthe evolves, Ground may be extended to supervise additional
  subsystems:

  - **DataspaceSupervisor** - For managing shared dataspace instances
  - **NetworkSupervisor** - For distributed actor communication
  - **PersistenceSupervisor** - For actor state persistence and recovery
  - **TelemetrySupervisor** - For observability and monitoring

  ## See Also

  - `Absynthe.Core.Actor` - The actor process implementation
  - `Supervisor` - Elixir's supervisor behavior
  - `DynamicSupervisor` - For dynamically managing child processes
  - `Registry` - Elixir's process registry
  """

  use Supervisor

  @typedoc """
  Configuration options for starting the Ground supervisor.
  """
  @type option ::
          {:name, atom()}
          | {:actor_supervisor_name, atom()}
          | {:actor_registry_name, atom()}
          | {:entity_registry_name, atom()}

  @typedoc """
  List of configuration options.
  """
  @type options :: [option()]

  # Default names for supervised processes
  @default_name __MODULE__
  @default_actor_supervisor_name Absynthe.Supervision.ActorSupervisor
  @default_actor_registry_name Absynthe.Registry.Actor
  @default_entity_registry_name Absynthe.Registry.Entity

  @doc """
  Starts the Ground supervisor.

  ## Options

  - `:name` - The registered name for this supervisor (default: `#{inspect(@default_name)}`)
  - `:actor_supervisor_name` - Name for ActorSupervisor (default: `#{inspect(@default_actor_supervisor_name)}`)
  - `:actor_registry_name` - Name for ActorRegistry (default: `#{inspect(@default_actor_registry_name)}`)
  - `:entity_registry_name` - Name for EntityRegistry (default: `#{inspect(@default_entity_registry_name)}`)

  ## Examples

      # Start with defaults
      {:ok, ground} = Absynthe.Supervision.Ground.start_link([])

      # Start with custom names
      {:ok, ground} = Absynthe.Supervision.Ground.start_link(
        name: MyApp.Ground,
        actor_registry_name: MyApp.ActorRegistry
      )

  ## Returns

  - `{:ok, pid}` - Successfully started the Ground supervisor
  - `{:error, reason}` - Failed to start, with reason describing the failure

  Possible failure reasons include:
  - `{:already_started, pid}` - A supervisor with this name is already running
  - `{:shutdown, term}` - One or more children failed to start
  """
  @spec start_link(options()) :: Supervisor.on_start()
  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, @default_name)
    Supervisor.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  Returns a child specification for use in a supervision tree.

  This allows Ground to be embedded in another supervisor's child list
  using the standard child specification format.

  ## Examples

      # In your application supervisor
      def start(_type, _args) do
        children = [
          {Absynthe.Supervision.Ground, []},
          # ... other children
        ]

        opts = [strategy: :one_for_one, name: MyApp.Supervisor]
        Supervisor.start_link(children, opts)
      end

      # With custom configuration
      def start(_type, _args) do
        children = [
          {Absynthe.Supervision.Ground, [name: MyApp.Ground]},
          # ... other children
        ]

        opts = [strategy: :one_for_one, name: MyApp.Supervisor]
        Supervisor.start_link(children, opts)
      end

  ## Parameters

  - `opts` - Configuration options (see `start_link/1` for details)

  ## Returns

  A child specification map with the following keys:
  - `:id` - The child identifier (always `Absynthe.Supervision.Ground`)
  - `:start` - The MFA tuple to start the supervisor
  - `:type` - The process type (always `:supervisor`)
  - `:restart` - The restart strategy (always `:permanent`)
  """
  @spec child_spec(options()) :: Supervisor.child_spec()
  def child_spec(opts) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [opts]},
      type: :supervisor,
      restart: :permanent
    }
  end

  # Supervisor Callbacks

  @impl Supervisor
  def init(opts) do
    # Extract configuration with defaults
    actor_supervisor_name =
      Keyword.get(opts, :actor_supervisor_name, @default_actor_supervisor_name)

    actor_registry_name = Keyword.get(opts, :actor_registry_name, @default_actor_registry_name)
    entity_registry_name = Keyword.get(opts, :entity_registry_name, @default_entity_registry_name)

    # Define children in startup order:
    # 1. Registries first (infrastructure)
    # 2. ActorSupervisor last (depends on registries)
    children = [
      # ActorRegistry - For looking up actors by name
      # Using a unique registry ensures each name maps to exactly one actor
      actor_registry_spec(actor_registry_name),

      # EntityRegistry - For looking up entities by reference
      # Enables fast entity resolution without scanning all actors
      entity_registry_spec(entity_registry_name),

      # ActorSupervisor - For managing actor process lifecycle
      # Started last because it may depend on registries being available
      actor_supervisor_spec(actor_supervisor_name)
    ]

    # Use :one_for_one strategy
    # Each child is independent and can restart without affecting siblings
    Supervisor.init(children, strategy: :one_for_one)
  end

  # Private Helper Functions

  @doc false
  defp actor_registry_spec(name) do
    # Define the child spec for ActorRegistry
    # If the module doesn't exist yet, this will fail gracefully with a clear error
    %{
      id: :actor_registry,
      start: {Registry, :start_link, [[keys: :unique, name: name]]},
      type: :supervisor
    }
  end

  @doc false
  defp entity_registry_spec(name) do
    # Define the child spec for EntityRegistry
    # If the module doesn't exist yet, this will fail gracefully with a clear error
    %{
      id: :entity_registry,
      start: {Registry, :start_link, [[keys: :unique, name: name]]},
      type: :supervisor
    }
  end

  @doc false
  defp actor_supervisor_spec(name) do
    # Define the child spec for ActorSupervisor
    # Using DynamicSupervisor for runtime actor management
    # If the module doesn't exist yet, this will fail gracefully with a clear error
    %{
      id: :actor_supervisor,
      start: {DynamicSupervisor, :start_link, [[name: name, strategy: :one_for_one]]},
      type: :supervisor
    }
  end
end
