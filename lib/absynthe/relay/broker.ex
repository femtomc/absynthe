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
  - `:noise_keypair` - Noise protocol keypair `{public, private}` for encrypted transport
  - `:noise_secret` - 32-byte secret to derive a deterministic Noise keypair (alternative to `:noise_keypair`)

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
  alias Absynthe.Relay.Transport.Noise

  @typedoc """
  Broker state.
  """
  @type state :: %{
          actor_pid: pid(),
          dataspace_ref: Ref.t(),
          listeners: [pid()],
          # Service assertion handles for listener advertisements
          service_handles: %{pid() => Absynthe.Assertions.Handle.t()},
          # Noise keypair for encrypted transport
          noise_keypair: {binary(), binary()} | nil
        }

  # Client API

  @doc """
  Starts a broker process.

  ## Options

  - `:socket_path` - Path for Unix domain socket
  - `:tcp_port` - TCP port to listen on
  - `:tcp_host` - TCP host to bind to (default: `{127, 0, 0, 1}`)
  - `:idle_timeout` - Idle timeout in milliseconds for relay connections (default: `:infinity`)
  - `:noise_keypair` - Noise protocol keypair `{public, private}` for encrypted transport
  - `:noise_secret` - 32-byte secret to derive a deterministic Noise keypair
  - `:name` - Optional GenServer name

  At least one of `:socket_path` or `:tcp_port` should be provided.
  If `:noise_keypair` or `:noise_secret` is provided, all connections will use Noise encryption.

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
  Returns the Noise public key for the broker.

  Clients need this key to perform the NK handshake. Returns `nil` if
  the broker is not using Noise encryption.
  """
  @spec noise_public_key(GenServer.server()) :: binary() | nil
  def noise_public_key(broker) do
    GenServer.call(broker, :noise_public_key)
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

    # Derive noise keypair from options
    noise_keypair = derive_noise_keypair(opts)

    if noise_keypair do
      {public, _private} = noise_keypair

      Logger.info(
        "Broker using Noise encryption, public key: #{Base.encode16(public, case: :lower)}"
      )
    end

    state = %{
      actor_pid: actor_pid,
      dataspace_ref: dataspace_ref,
      listeners: [],
      service_handles: %{},
      noise_keypair: noise_keypair
    }

    # Start listeners and advertise them in the dataspace
    state = start_listeners(state, opts)

    {:ok, state}
  end

  # Derive noise keypair from options - supports :noise_keypair or :noise_secret
  defp derive_noise_keypair(opts) do
    case Keyword.get(opts, :noise_keypair) do
      {_public, _private} = keypair ->
        keypair

      nil ->
        case Keyword.get(opts, :noise_secret) do
          nil ->
            nil

          secret when is_binary(secret) ->
            case Noise.keypair_from_secret(secret) do
              {:ok, keypair} ->
                keypair

              {:error, reason} ->
                Logger.error("Failed to derive Noise keypair from secret: #{inspect(reason)}")
                nil
            end
        end
    end
  end

  defp start_listeners(state, opts) do
    idle_timeout = Keyword.get(opts, :idle_timeout, :infinity)

    # Build common listener options
    common_opts = [
      dataspace_ref: state.dataspace_ref,
      actor_pid: state.actor_pid,
      idle_timeout: idle_timeout
    ]

    # Add noise_keypair if configured
    common_opts =
      if state.noise_keypair do
        Keyword.put(common_opts, :noise_keypair, state.noise_keypair)
      else
        common_opts
      end

    # Unix socket listener
    state =
      case Keyword.get(opts, :socket_path) do
        nil ->
          state

        path ->
          listener_opts = Keyword.merge(common_opts, type: :unix, path: path)

          case Listener.start_link(listener_opts) do
            {:ok, pid} ->
              Process.monitor(pid)
              # Assert service advertisement into the dataspace
              handle = assert_listener_service(state, {:unix, path})

              %{
                state
                | listeners: [pid | state.listeners],
                  service_handles: Map.put(state.service_handles, pid, handle)
              }

            {:error, reason} ->
              Logger.error("Failed to start Unix listener: #{inspect(reason)}")
              state
          end
      end

    # TCP listener
    state =
      case Keyword.get(opts, :tcp_port) do
        nil ->
          state

        port ->
          host = Keyword.get(opts, :tcp_host, {127, 0, 0, 1})
          listener_opts = Keyword.merge(common_opts, type: :tcp, port: port, host: host)

          case Listener.start_link(listener_opts) do
            {:ok, pid} ->
              Process.monitor(pid)
              # Get actual port from listener
              {:tcp, actual_host, actual_port} = Listener.address(pid)
              # Assert service advertisement into the dataspace
              handle = assert_listener_service(state, {:tcp, actual_host, actual_port})

              %{
                state
                | listeners: [pid | state.listeners],
                  service_handles: Map.put(state.service_handles, pid, handle)
              }

            {:error, reason} ->
              Logger.error("Failed to start TCP listener: #{inspect(reason)}")
              state
          end
      end

    state
  end

  # Assert a listener service record into the dataspace
  # Returns the assertion handle
  defp assert_listener_service(state, {:unix, path}) do
    # Create a <ListenerService <unix "path">> record
    service_record =
      {:record,
       {{:symbol, "ListenerService"}, [{:record, {{:symbol, "unix"}, [{:string, path}]}}]}}

    handle = Actor.assert(state.actor_pid, state.dataspace_ref, service_record)
    Logger.debug("Asserted listener service: unix:#{path}")
    handle
  end

  defp assert_listener_service(state, {:tcp, host, port}) do
    # Create a <ListenerService <tcp "host" port>> record
    host_str = :inet.ntoa(host) |> to_string()

    service_record =
      {:record,
       {{:symbol, "ListenerService"},
        [{:record, {{:symbol, "tcp"}, [{:string, host_str}, {:integer, port}]}}]}}

    handle = Actor.assert(state.actor_pid, state.dataspace_ref, service_record)
    Logger.debug("Asserted listener service: tcp:#{host_str}:#{port}")
    handle
  end

  @impl GenServer
  def handle_call(:dataspace_ref, _from, state) do
    {:reply, state.dataspace_ref, state}
  end

  def handle_call(:actor_pid, _from, state) do
    {:reply, state.actor_pid, state}
  end

  def handle_call(:noise_public_key, _from, state) do
    public_key =
      case state.noise_keypair do
        {public, _private} -> public
        nil -> nil
      end

    {:reply, public_key, state}
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

      # Retract the service assertion for this listener
      state =
        case Map.get(state.service_handles, pid) do
          nil ->
            state

          handle ->
            Actor.retract(state.actor_pid, handle)
            %{state | service_handles: Map.delete(state.service_handles, pid)}
        end

      state = %{state | listeners: List.delete(state.listeners, pid)}
      {:noreply, state}
    else
      {:noreply, state}
    end
  end

  @impl GenServer
  def terminate(_reason, state) do
    # Retract all service assertions
    Enum.each(state.service_handles, fn {_pid, handle} ->
      try do
        Actor.retract(state.actor_pid, handle)
      catch
        _, _ -> :ok
      end
    end)

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
