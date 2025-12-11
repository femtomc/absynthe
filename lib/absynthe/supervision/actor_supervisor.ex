defmodule Absynthe.Supervision.ActorSupervisor do
  @moduledoc """
  DynamicSupervisor for managing actor processes in the Syndicated Actor Model.

  ## Overview

  The ActorSupervisor provides fault-tolerant supervision for actor processes,
  ensuring that actors can be dynamically started, stopped, and restarted on
  failure. It uses the `:one_for_one` strategy, where each actor is supervised
  independently - if one actor fails, it does not affect other actors.

  ## Architecture

  The supervisor follows the OTP DynamicSupervisor pattern, allowing actors to
  be added and removed at runtime. This is essential for the Syndicated Actor
  Model, where actors may be created dynamically in response to:

  - New client connections
  - Spawning of conversational contexts
  - Dynamic system reconfiguration
  - On-demand resource allocation

  ## Supervision Strategy

  The `:one_for_one` strategy ensures that:

  - Each actor is supervised independently
  - Actor failures are isolated (no cascading failures)
  - Failed actors can be restarted automatically
  - Sibling actors continue running during restarts

  This isolation is critical for maintaining system stability in distributed
  actor systems where partial failures are expected and should not bring down
  the entire system.

  ## Restart Behavior

  The supervisor is configured with restart limits to prevent infinite restart
  loops when actors consistently fail. The default configuration is:

  - `max_restarts: 3` - Allow 3 restarts per time window
  - `max_seconds: 5` - Time window of 5 seconds

  If an actor exceeds these limits, the supervisor will terminate to avoid
  resource exhaustion. This can be customized via the `:max_restarts` and
  `:max_seconds` options in `start_link/1`.

  ## Actor Management

  The supervisor provides several functions for managing actors:

  - `start_actor/2` - Dynamically start a new actor
  - `stop_actor/2` - Gracefully stop a running actor
  - `which_actors/1` - List all supervised actor PIDs
  - `count_actors/1` - Get the count of supervised actors

  These functions allow dynamic actor lifecycle management while maintaining
  supervision guarantees.

  ## Integration with Absynthe.Core.Actor

  This supervisor is designed to work with `Absynthe.Core.Actor` processes,
  but can supervise any GenServer-compatible process. When starting actors,
  you provide the module and initialization arguments, and the supervisor
  ensures proper lifecycle management.

  ## Examples

  Starting a supervisor and managing actors:

      # Start the supervisor
      {:ok, supervisor} = Absynthe.Supervision.ActorSupervisor.start_link(
        name: MyApp.ActorSupervisor,
        max_restarts: 5,
        max_seconds: 10
      )

      # Start an actor under supervision
      {:ok, actor_pid} = Absynthe.Supervision.ActorSupervisor.start_actor(
        supervisor,
        Absynthe.Core.Actor,
        [id: :my_actor, name: MyActor]
      )

      # Check how many actors are running
      count = Absynthe.Supervision.ActorSupervisor.count_actors(supervisor)

      # List all supervised actors
      actors = Absynthe.Supervision.ActorSupervisor.which_actors(supervisor)

      # Stop an actor
      :ok = Absynthe.Supervision.ActorSupervisor.stop_actor(supervisor, actor_pid)

  ## Application Integration

  The supervisor can be integrated into your application supervision tree:

      defmodule MyApp.Application do
        use Application

        def start(_type, _args) do
          children = [
            {Absynthe.Supervision.ActorSupervisor, name: MyApp.ActorSupervisor}
          ]

          opts = [strategy: :one_for_one, name: MyApp.Supervisor]
          Supervisor.start_link(children, opts)
        end
      end

  ## Design Decisions

  ### Why DynamicSupervisor?

  DynamicSupervisor is chosen over regular Supervisor because:

  - Actors are created at runtime, not at application start
  - The number of actors is not known in advance
  - Actors need to be added/removed dynamically
  - Each actor has unique initialization parameters

  ### Why :one_for_one Strategy?

  The `:one_for_one` strategy is appropriate because:

  - Actors are independent and isolated
  - Actor failures should not cascade to other actors
  - Each actor manages its own state and resources
  - Fate-sharing is implemented at the facet level, not supervision level

  ### Why Independent PIDs?

  Actors are identified by PIDs rather than registered names by default,
  allowing multiple instances of the same actor module to run concurrently.
  Actors can optionally register names for global access if needed.

  ## Future Extensions

  Future versions may add:

  - Actor pool management (maintain N actors of a given type)
  - Metrics and monitoring integration
  - Distributed supervision across nodes
  - Custom restart strategies per actor type
  - Health checking and proactive restarts

  ## See Also

  - `Absynthe.Core.Actor` - The actor implementation supervised by this module
  - `DynamicSupervisor` - Elixir's built-in dynamic supervisor
  - `Supervisor` - OTP supervisor behavior documentation
  """

  use DynamicSupervisor
  require Logger

  # Type Definitions

  @typedoc """
  Supervisor reference - can be a PID, registered name, or tuple.
  """
  @type supervisor_ref :: DynamicSupervisor.supervisor()

  @typedoc """
  Options for starting the supervisor.

  - `:name` - Optional registered name for the supervisor
  - `:max_restarts` - Maximum number of restarts allowed in time window (default: 3)
  - `:max_seconds` - Time window for restart counting in seconds (default: 5)
  """
  @type start_option ::
          {:name, atom() | {:global, term()} | {:via, module(), term()}}
          | {:max_restarts, non_neg_integer()}
          | {:max_seconds, pos_integer()}

  @typedoc """
  Options for starting an actor.

  These are passed to the actor module's `start_link/1` function.
  """
  @type actor_option :: term()

  # Client API

  @doc """
  Starts the ActorSupervisor.

  ## Options

  - `:name` - Optional registered name for the supervisor. If provided, the
    supervisor can be referenced by name instead of PID.
  - `:max_restarts` - Maximum number of restarts allowed in the time window
    (default: 3). If exceeded, the supervisor terminates.
  - `:max_seconds` - Time window in seconds for counting restarts (default: 5).

  ## Examples

      # Start an anonymous supervisor
      {:ok, supervisor} = Absynthe.Supervision.ActorSupervisor.start_link([])

      # Start a named supervisor
      {:ok, supervisor} = Absynthe.Supervision.ActorSupervisor.start_link(
        name: MyApp.ActorSupervisor
      )

      # Start with custom restart limits
      {:ok, supervisor} = Absynthe.Supervision.ActorSupervisor.start_link(
        name: MyApp.ActorSupervisor,
        max_restarts: 10,
        max_seconds: 60
      )

  ## Returns

  - `{:ok, pid}` - Successfully started supervisor
  - `{:error, reason}` - Failed to start (e.g., name already registered)
  """
  @spec start_link([start_option()]) :: DynamicSupervisor.on_start()
  def start_link(opts \\ []) do
    # Extract supervisor-specific options
    {supervisor_opts, init_opts} = Keyword.split(opts, [:name])

    # Start the DynamicSupervisor
    DynamicSupervisor.start_link(__MODULE__, init_opts, supervisor_opts)
  end

  @doc """
  Starts a new actor process under supervision.

  The actor is started dynamically and added to the supervision tree. If the
  actor terminates, it will be restarted according to the supervisor's restart
  strategy and limits.

  ## Parameters

  - `supervisor` - The supervisor PID or registered name
  - `module` - The actor module (e.g., `Absynthe.Core.Actor`)
  - `args` - Arguments passed to the actor's `start_link/1` function

  ## Examples

      # Start an actor with default options
      {:ok, pid} = Absynthe.Supervision.ActorSupervisor.start_actor(
        supervisor,
        Absynthe.Core.Actor,
        [id: :my_actor]
      )

      # Start a named actor
      {:ok, pid} = Absynthe.Supervision.ActorSupervisor.start_actor(
        supervisor,
        Absynthe.Core.Actor,
        [id: :chat_actor, name: MyApp.ChatActor]
      )

      # Start an actor with custom initialization
      {:ok, pid} = Absynthe.Supervision.ActorSupervisor.start_actor(
        supervisor,
        Absynthe.Core.Actor,
        [id: :dataspace, root_facet_id: :main]
      )

  ## Returns

  - `{:ok, pid}` - Successfully started actor
  - `{:ok, pid, info}` - Successfully started with additional info
  - `{:error, reason}` - Failed to start actor

  ## Error Reasons

  - `{:already_started, pid}` - Actor with this name already running
  - `{:shutdown, term}` - Actor terminated during startup
  - `{:error, term}` - Actor initialization failed
  """
  @spec start_actor(supervisor_ref(), module(), [actor_option()]) ::
          DynamicSupervisor.on_start_child()
  def start_actor(supervisor, module, args \\ []) do
    # Build the child specification
    child_spec = %{
      id: module,
      start: {module, :start_link, [args]},
      restart: :transient,
      type: :worker
    }

    Logger.debug(
      "Starting actor #{inspect(module)} under supervisor #{inspect(supervisor)} with args: #{inspect(args)}"
    )

    # Start the child under supervision
    case DynamicSupervisor.start_child(supervisor, child_spec) do
      {:ok, pid} = result ->
        Logger.info("Successfully started actor #{inspect(module)} with PID #{inspect(pid)}")
        result

      {:ok, pid, info} = result ->
        Logger.info(
          "Successfully started actor #{inspect(module)} with PID #{inspect(pid)}, info: #{inspect(info)}"
        )

        result

      {:error, reason} = error ->
        Logger.error("Failed to start actor #{inspect(module)}: #{inspect(reason)}")
        error
    end
  end

  @doc """
  Stops a supervised actor process.

  Gracefully terminates the actor by sending it a shutdown signal. The
  supervisor will not restart the actor after this termination.

  ## Parameters

  - `supervisor` - The supervisor PID or registered name
  - `actor_pid` - The PID of the actor to stop

  ## Examples

      {:ok, actor_pid} = Absynthe.Supervision.ActorSupervisor.start_actor(
        supervisor,
        Absynthe.Core.Actor,
        [id: :temp_actor]
      )

      # Later, stop the actor
      :ok = Absynthe.Supervision.ActorSupervisor.stop_actor(supervisor, actor_pid)

  ## Returns

  - `:ok` - Successfully stopped the actor
  - `{:error, :not_found}` - Actor not found under this supervisor

  ## Notes

  The actor is terminated with reason `:shutdown`, which is considered a normal
  termination. The actor's `terminate/2` callback will be called, allowing it
  to clean up resources.
  """
  @spec stop_actor(supervisor_ref(), pid()) :: :ok | {:error, :not_found}
  def stop_actor(supervisor, actor_pid) when is_pid(actor_pid) do
    Logger.debug("Stopping actor #{inspect(actor_pid)} under supervisor #{inspect(supervisor)}")

    case DynamicSupervisor.terminate_child(supervisor, actor_pid) do
      :ok ->
        Logger.info("Successfully stopped actor #{inspect(actor_pid)}")
        :ok

      {:error, :not_found} = error ->
        Logger.warning("Actor #{inspect(actor_pid)} not found under supervisor")
        error
    end
  end

  @doc """
  Lists all supervised actor PIDs.

  Returns a list of PIDs for all actors currently running under this
  supervisor. This is useful for monitoring, debugging, and administrative
  operations.

  ## Parameters

  - `supervisor` - The supervisor PID or registered name

  ## Examples

      actors = Absynthe.Supervision.ActorSupervisor.which_actors(supervisor)
      # => [#PID<0.123.0>, #PID<0.124.0>, #PID<0.125.0>]

      # Iterate over all actors
      for actor_pid <- Absynthe.Supervision.ActorSupervisor.which_actors(supervisor) do
        # Perform operations on each actor
        actor_info = :sys.get_state(actor_pid)
        IO.inspect(actor_info)
      end

  ## Returns

  A list of PIDs for all supervised actors. Returns an empty list if no
  actors are running.

  ## Notes

  The returned list is a snapshot at the time of the call. Actors may start
  or stop between calls to this function.
  """
  @spec which_actors(supervisor_ref()) :: [pid()]
  def which_actors(supervisor) do
    supervisor
    |> DynamicSupervisor.which_children()
    |> Enum.map(fn
      # Extract the PID from each child tuple
      # Format: {:undefined, pid, :worker, [module]}
      {:undefined, pid, :worker, _modules} when is_pid(pid) -> pid
      # Handle any other format by returning nil, then filter out
      _ -> nil
    end)
    |> Enum.reject(&is_nil/1)
  end

  @doc """
  Counts the number of supervised actors.

  Returns the total number of actors currently running under this supervisor.
  This is more efficient than calling `length(which_actors(supervisor))` for
  large numbers of actors.

  ## Parameters

  - `supervisor` - The supervisor PID or registered name

  ## Examples

      count = Absynthe.Supervision.ActorSupervisor.count_actors(supervisor)
      # => 42

      # Check if any actors are running
      if Absynthe.Supervision.ActorSupervisor.count_actors(supervisor) > 0 do
        IO.puts("Actors are running")
      end

  ## Returns

  A non-negative integer representing the count of supervised actors.

  ## Notes

  This count includes all child processes, even if they are currently
  restarting. The count reflects the supervisor's view at the moment of
  the call.
  """
  @spec count_actors(supervisor_ref()) :: non_neg_integer()
  def count_actors(supervisor) do
    supervisor
    |> DynamicSupervisor.count_children()
    |> Map.get(:active, 0)
  end

  # DynamicSupervisor Callbacks

  @impl DynamicSupervisor
  def init(opts) do
    # Extract restart limits from options
    max_restarts = Keyword.get(opts, :max_restarts, 3)
    max_seconds = Keyword.get(opts, :max_seconds, 5)

    Logger.info(
      "Initializing ActorSupervisor with max_restarts: #{max_restarts}, max_seconds: #{max_seconds}"
    )

    # Configure the DynamicSupervisor with :one_for_one strategy
    # This means each actor is supervised independently
    DynamicSupervisor.init(
      strategy: :one_for_one,
      max_restarts: max_restarts,
      max_seconds: max_seconds
    )
  end
end
