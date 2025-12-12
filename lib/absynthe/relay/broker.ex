defmodule Absynthe.Relay.Broker do
  @moduledoc """
  Syndicate protocol broker for distributed dataspace access.

  The broker is a supervised process that hosts a dataspace and accepts
  remote connections via the Syndicate relay protocol. It provides a
  simple way to expose a local dataspace to remote peers.

  ## Architecture

      ┌─────────────────────────────────────────┐
      │              Broker                      │
      │  ┌──────────────────────────────────┐   │
      │  │         Dataspace Actor          │   │
      │  │  ┌──────────────────────────┐    │   │
      │  │  │    Dataspace Entity      │    │   │
      │  │  │  (assertions, observers) │    │   │
      │  │  └──────────────────────────┘    │   │
      │  └──────────────────────────────────┘   │
      │                                          │
      │  ┌──────────────────────────────────┐   │
      │  │         Listener                 │   │
      │  │   (Unix/TCP socket accept)       │   │
      │  └──────────────────────────────────┘   │
      │                │                         │
      │     ┌──────────┼──────────┐             │
      │     ▼          ▼          ▼             │
      │  ┌─────┐   ┌─────┐   ┌─────┐            │
      │  │Relay│   │Relay│   │Relay│  ...       │
      │  └─────┘   └─────┘   └─────┘            │
      └─────────────────────────────────────────┘

  ## Usage

      # Start a broker with a Unix socket
      {:ok, broker} = Broker.start_link(
        socket_path: "/tmp/syndicate.sock"
      )

      # Get the dataspace ref to spawn local entities
      dataspace_ref = Broker.dataspace_ref(broker)

      # Stop the broker
      Broker.stop(broker)

  ## Options

  - `:socket_path` - Path for Unix domain socket (default: generates temp path)
  - `:tcp_port` - TCP port to listen on (optional)
  - `:tcp_host` - TCP host to bind to (default: `{127, 0, 0, 1}`)
  - `:name` - Optional registered name for the broker

  ## Supervision

  The broker can be started under a supervisor:

      children = [
        {Absynthe.Relay.Broker, socket_path: "/tmp/syndicate.sock"}
      ]

      Supervisor.start_link(children, strategy: :one_for_one)
  """

  use GenServer
  require Logger

  alias Absynthe.Core.{Actor, Ref}
  alias Absynthe.Dataspace.Dataspace
  alias Absynthe.Relay.Listener

  @typedoc """
  Broker state.
  """
  @type state :: %{
          actor_pid: pid(),
          dataspace_ref: Ref.t(),
          listeners: [pid()]
        }

  # Client API

  @doc """
  Starts a broker process.

  ## Options

  - `:socket_path` - Path for Unix domain socket
  - `:tcp_port` - TCP port to listen on
  - `:tcp_host` - TCP host to bind to (default: `{127, 0, 0, 1}`)
  - `:name` - Optional GenServer name

  At least one of `:socket_path` or `:tcp_port` should be provided.

  ## Returns

  `{:ok, pid}` on success.
  """
  @spec start_link(Keyword.t()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    {gen_opts, init_opts} = Keyword.split(opts, [:name])
    GenServer.start_link(__MODULE__, init_opts, gen_opts)
  end

  @doc """
  Returns the dataspace entity ref for the broker.

  Use this ref to spawn local entities that interact with the dataspace.
  """
  @spec dataspace_ref(GenServer.server()) :: Ref.t()
  def dataspace_ref(broker) do
    GenServer.call(broker, :dataspace_ref)
  end

  @doc """
  Returns the actor PID hosting the broker's entities.
  """
  @spec actor_pid(GenServer.server()) :: pid()
  def actor_pid(broker) do
    GenServer.call(broker, :actor_pid)
  end

  @doc """
  Returns the addresses the broker is listening on.
  """
  @spec addresses(GenServer.server()) :: [{:unix, String.t()} | {:tcp, tuple(), integer()}]
  def addresses(broker) do
    GenServer.call(broker, :addresses)
  end

  @doc """
  Stops the broker and all connections.
  """
  @spec stop(GenServer.server()) :: :ok
  def stop(broker) do
    GenServer.stop(broker)
  end

  @doc """
  Child spec for supervision.
  """
  def child_spec(opts) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [opts]},
      type: :worker,
      restart: :permanent
    }
  end

  # GenServer callbacks

  @impl GenServer
  def init(opts) do
    # Start the actor that will host the dataspace
    {:ok, actor_pid} = Actor.start_link(id: {:broker, self()})

    # Spawn the dataspace entity
    {:ok, dataspace_ref} = Actor.spawn_entity(actor_pid, :root, Dataspace.new())

    Logger.info("Broker started with dataspace #{inspect(dataspace_ref)}")

    state = %{
      actor_pid: actor_pid,
      dataspace_ref: dataspace_ref,
      listeners: []
    }

    # Start listeners
    state = start_listeners(state, opts)

    {:ok, state}
  end

  defp start_listeners(state, opts) do
    listeners = []

    # Unix socket listener
    listeners =
      case Keyword.get(opts, :socket_path) do
        nil ->
          listeners

        path ->
          case Listener.start_link(
                 type: :unix,
                 path: path,
                 dataspace_ref: state.dataspace_ref,
                 actor_pid: state.actor_pid
               ) do
            {:ok, pid} ->
              Process.monitor(pid)
              [pid | listeners]

            {:error, reason} ->
              Logger.error("Failed to start Unix listener: #{inspect(reason)}")
              listeners
          end
      end

    # TCP listener
    listeners =
      case Keyword.get(opts, :tcp_port) do
        nil ->
          listeners

        port ->
          host = Keyword.get(opts, :tcp_host, {127, 0, 0, 1})

          case Listener.start_link(
                 type: :tcp,
                 port: port,
                 host: host,
                 dataspace_ref: state.dataspace_ref,
                 actor_pid: state.actor_pid
               ) do
            {:ok, pid} ->
              Process.monitor(pid)
              [pid | listeners]

            {:error, reason} ->
              Logger.error("Failed to start TCP listener: #{inspect(reason)}")
              listeners
          end
      end

    %{state | listeners: listeners}
  end

  @impl GenServer
  def handle_call(:dataspace_ref, _from, state) do
    {:reply, state.dataspace_ref, state}
  end

  def handle_call(:actor_pid, _from, state) do
    {:reply, state.actor_pid, state}
  end

  def handle_call(:addresses, _from, state) do
    addresses =
      Enum.map(state.listeners, fn listener ->
        try do
          Listener.address(listener)
        catch
          _, _ -> nil
        end
      end)
      |> Enum.reject(&is_nil/1)

    {:reply, addresses, state}
  end

  @impl GenServer
  def handle_info({:DOWN, _ref, :process, pid, reason}, state) do
    if pid in state.listeners do
      Logger.warning("Listener #{inspect(pid)} terminated: #{inspect(reason)}")
      state = %{state | listeners: List.delete(state.listeners, pid)}
      {:noreply, state}
    else
      {:noreply, state}
    end
  end

  @impl GenServer
  def terminate(_reason, state) do
    # Stop all listeners
    Enum.each(state.listeners, fn listener ->
      try do
        Listener.stop(listener)
      catch
        _, _ -> :ok
      end
    end)

    :ok
  end
end
