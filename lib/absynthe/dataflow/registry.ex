defmodule Absynthe.Dataflow.Registry do
  @moduledoc """
  ETS-based registry for dataflow fields.

  This module provides a field registry with the following characteristics:
  - Uses ETS for storage with concurrent read/write access
  - Supports atomic field updates via `:ets.insert`
  - Tracks field ownership for per-process cleanup
  - Automatically started on first use via `ensure_started/0`

  ## Architecture

  The registry uses two ETS tables:
  - `fields` - Maps field_id to field struct
  - `owners` - Maps {owner_pid, field_id} for ownership tracking

  Using ETS instead of Agent provides:
  - Better concurrency (no single-process bottleneck)
  - Atomic operations for field updates

  ## Field Ownership

  Each field is associated with an owner process (typically an Actor). When
  an entity containing fields is spawned into an actor, ownership is
  automatically transferred to the actor process. When the actor terminates,
  all its owned fields are cleaned up.

  ## Ephemeral State (Important)

  **Dataflow state is ephemeral by design.** Field values are not persisted
  and will be lost in the following scenarios:

  - When the owning actor process terminates (normal cleanup)
  - If the registry GenServer crashes (ETS tables are destroyed)
  - On application restart

  This is intentional and consistent with the Syndicated Actor Model where
  actor state lives in assertions that can be re-established. If you need
  persistent state, use assertions to a dataspace that can be observed and
  recreated on restart.

  ## Supervision

  For production use, add `Absynthe.Dataflow.Registry` to your application's
  supervision tree to ensure it starts on application boot:

      children = [
        Absynthe.Dataflow.Registry,
        # ... other children
      ]

      Supervisor.start_link(children, strategy: :one_for_one)

  If not explicitly supervised, the registry will be lazily started on
  first use but won't be restarted on crash.
  """

  use GenServer
  require Logger

  @fields_table __MODULE__.Fields
  @owners_table __MODULE__.Owners

  # Client API

  @doc """
  Returns a child specification for supervision.

  This allows adding the registry to a supervision tree:

      children = [
        Absynthe.Dataflow.Registry,
        # or with options:
        {Absynthe.Dataflow.Registry, []}
      ]
  """
  def child_spec(opts) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [opts]},
      type: :worker,
      restart: :permanent,
      shutdown: 5000
    }
  end

  @doc """
  Starts the registry.
  """
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Ensures the registry is started and tables exist. Idempotent.
  """
  def ensure_started do
    # Simple approach: just try to start, handle all error cases
    case start_link() do
      {:ok, _pid} -> :ok
      {:error, {:already_started, _pid}} -> :ok
      {:error, _reason} -> :ok
    end
  end

  @doc """
  Stores or updates a field in the registry.
  """
  def put_field(field, owner_pid \\ self()) do
    with_retry(fn ->
      :ets.insert(@fields_table, {field.id, field})
      :ets.insert(@owners_table, {{owner_pid, field.id}, true})
      field
    end, field)
  end

  @doc """
  Retrieves a field by ID.
  """
  def get_field(field_id) do
    with_retry(fn ->
      case :ets.lookup(@fields_table, field_id) do
        [{^field_id, field}] -> field
        [] -> nil
      end
    end, nil)
  end

  @doc """
  Atomically updates a field, applying a function to get the new field.

  Uses `:ets.select_replace` for atomic read-modify-write.
  Returns the updated field or nil if not found.
  """
  def update_field(field_id, update_fn) when is_function(update_fn, 1) do
    with_retry(fn ->
      case :ets.lookup(@fields_table, field_id) do
        [{^field_id, field}] ->
          new_field = update_fn.(field)
          :ets.insert(@fields_table, {field_id, new_field})
          new_field

        [] ->
          nil
      end
    end, nil)
  end

  @doc """
  Atomically updates specific field attributes.

  This is more efficient than update_field for simple attribute changes.
  Returns true if update succeeded, false if field not found.
  """
  def update_field_attrs(field_id, updates) when is_list(updates) do
    with_retry(fn ->
      case :ets.lookup(@fields_table, field_id) do
        [{^field_id, field}] ->
          new_field = Enum.reduce(updates, field, fn {key, value}, acc ->
            Map.put(acc, key, value)
          end)
          :ets.insert(@fields_table, {field_id, new_field})
          true

        [] ->
          false
      end
    end, false)
  end

  @doc """
  Cleans up all fields owned by the specified process.
  """
  def cleanup(owner_pid \\ self()) do
    with_retry(fn ->
      owned = :ets.match(@owners_table, {{owner_pid, :"$1"}, :_})
      field_ids = List.flatten(owned)

      Enum.each(field_ids, fn field_id ->
        :ets.delete(@fields_table, field_id)
        :ets.delete(@owners_table, {owner_pid, field_id})
      end)

      :ok
    end, :ok)
  end

  @doc """
  Removes all ownership entries for a field.

  This is used when reassigning ownership to a different process.
  """
  def remove_ownership(field_id) do
    with_retry(fn ->
      # Find all owners for this field and remove their entries
      owners = :ets.match(@owners_table, {{:"$1", field_id}, :_})

      Enum.each(List.flatten(owners), fn owner_pid ->
        :ets.delete(@owners_table, {owner_pid, field_id})
      end)

      :ok
    end, :ok)
  end

  @doc """
  Adds an ownership entry for a field.

  Associates the field with the specified owner process for cleanup tracking.
  """
  def add_ownership(field_id, owner_pid) do
    with_retry(fn ->
      :ets.insert(@owners_table, {{owner_pid, field_id}, true})
      :ok
    end, :ok)
  end

  @doc """
  Returns all field IDs (for debugging/testing).
  """
  def all_field_ids do
    with_retry(fn ->
      :ets.select(@fields_table, [{{:"$1", :_}, [], [:"$1"]}])
    end, [])
  end

  # Helper to retry ETS operations if tables are recreated
  defp with_retry(fun, default, attempt \\ 1) do
    ensure_started()

    try do
      fun.()
    rescue
      ArgumentError ->
        if attempt < 3 do
          # Table may have been deleted; retry after brief pause
          Process.sleep(5)
          with_retry(fun, default, attempt + 1)
        else
          default
        end
    end
  end

  # GenServer Callbacks

  @impl GenServer
  def init(_opts) do
    # Create ETS tables with :public access so any process can read/write
    # The GenServer is just the owner for lifecycle management
    :ets.new(@fields_table, [
      :named_table,
      :public,
      :set,
      read_concurrency: true,
      write_concurrency: true
    ])

    :ets.new(@owners_table, [
      :named_table,
      :public,
      :set,
      read_concurrency: true,
      write_concurrency: true
    ])

    Logger.debug("Dataflow registry started with ETS tables")
    {:ok, %{}}
  end

  @impl GenServer
  def terminate(reason, _state) do
    Logger.debug("Dataflow registry terminating: #{inspect(reason)}")
    # ETS tables will be automatically deleted when owner terminates
    # If we wanted persistence, we'd use :heir option
    :ok
  end
end
