defmodule Absynthe.Relay.Listener do
  @moduledoc """
  Socket listener for the Syndicate relay protocol.

  This module implements a GenServer that listens for incoming connections
  on either TCP or Unix domain sockets, spawning a relay worker for each
  accepted connection.

  ## Usage

  Start a Unix socket listener:

      {:ok, listener} = Listener.start_link(
        type: :unix,
        path: "/tmp/syndicate.sock",
        dataspace_ref: dataspace_ref
      )

  Start a TCP listener:

      {:ok, listener} = Listener.start_link(
        type: :tcp,
        port: 8080,
        dataspace_ref: dataspace_ref
      )

  ## Options

  Common options:
  - `:dataspace_ref` - Ref to the target dataspace entity (required)
  - `:actor_pid` - PID of the hosting actor (optional)
  - `:idle_timeout` - Idle timeout in milliseconds for relay connections (default: `:infinity`)
  - `:listener_id` - Unique identifier for logging (default: auto-generated)
  - `:noise_keypair` - Noise protocol keypair `{public, private}` for encrypted transport (optional)

  Unix socket options:
  - `:type` - `:unix`
  - `:path` - Path to the Unix socket file

  TCP options:
  - `:type` - `:tcp`
  - `:port` - Port number to listen on
  - `:host` - Host to bind to (default: `{127, 0, 0, 1}`)

  ## Supervision

  The listener should typically be started under a supervisor. It will
  restart on crashes, but note that Unix socket files may need manual
  cleanup if the process crashes.
  """

  use GenServer
  require Logger

  alias Absynthe.Relay.Relay

  @typedoc """
  Listener state.
  """
  @type state :: %{
          type: :tcp | :unix,
          listen_socket: port(),
          dataspace_ref: Absynthe.Core.Ref.t(),
          actor_pid: pid() | nil,
          socket_path: String.t() | nil,
          relays: [pid()],
          idle_timeout: timeout(),
          listener_id: String.t(),
          connection_counter: non_neg_integer(),
          noise_keypair: {binary(), binary()} | nil
        }

  # Client API

  @doc """
  Starts a listener process.

  ## Options

  For Unix sockets:
  - `:type` - `:unix`
  - `:path` - Path to the Unix socket file (required)
  - `:dataspace_ref` - Ref to the target dataspace (required)
  - `:actor_pid` - Optional actor PID for relay workers

  For TCP sockets:
  - `:type` - `:tcp`
  - `:port` - Port number (required)
  - `:host` - Host to bind to (default: `{127, 0, 0, 1}`)
  - `:dataspace_ref` - Ref to the target dataspace (required)
  - `:actor_pid` - Optional actor PID for relay workers

  ## Returns

  `{:ok, pid}` on success, `{:error, reason}` on failure.
  """
  @spec start_link(Keyword.t()) :: GenServer.on_start()
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  @doc """
  Returns the listener's socket address.

  For Unix sockets, returns the path. For TCP, returns `{host, port}`.
  """
  @spec address(GenServer.server()) :: term()
  def address(listener) do
    GenServer.call(listener, :address)
  end

  @doc """
  Stops the listener and closes all relay connections.
  """
  @spec stop(GenServer.server()) :: :ok
  def stop(listener) do
    GenServer.stop(listener)
  end

  # GenServer callbacks

  @impl GenServer
  def init(opts) do
    type = Keyword.fetch!(opts, :type)
    dataspace_ref = Keyword.fetch!(opts, :dataspace_ref)
    actor_pid = Keyword.get(opts, :actor_pid)
    idle_timeout = Keyword.get(opts, :idle_timeout, :infinity)
    noise_keypair = Keyword.get(opts, :noise_keypair)

    listener_id =
      Keyword.get_lazy(opts, :listener_id, fn ->
        generate_listener_id()
      end)

    common_state = %{
      dataspace_ref: dataspace_ref,
      actor_pid: actor_pid,
      relays: [],
      idle_timeout: idle_timeout,
      listener_id: listener_id,
      connection_counter: 0,
      noise_keypair: noise_keypair
    }

    case type do
      :unix ->
        path = Keyword.fetch!(opts, :path)
        init_unix(path, common_state)

      :tcp ->
        port = Keyword.fetch!(opts, :port)
        host = Keyword.get(opts, :host, {127, 0, 0, 1})
        init_tcp(host, port, common_state)
    end
  end

  defp generate_listener_id do
    :crypto.strong_rand_bytes(3) |> Base.encode16(case: :lower)
  end

  defp init_unix(path, common_state) do
    # Remove existing socket file if present
    File.rm(path)

    # Create Unix domain socket
    case :gen_tcp.listen(0, [
           :binary,
           {:packet, :raw},
           {:active, false},
           {:reuseaddr, true},
           {:ifaddr, {:local, path}}
         ]) do
      {:ok, listen_socket} ->
        Logger.info("[#{common_state.listener_id}] Listener started on Unix socket: #{path}")

        state =
          Map.merge(common_state, %{
            type: :unix,
            listen_socket: listen_socket,
            socket_path: path
          })

        # Start accepting connections
        send(self(), :accept)

        {:ok, state}

      {:error, reason} ->
        {:stop, {:listen_error, reason}}
    end
  end

  defp init_tcp(host, port, common_state) do
    opts = [
      :binary,
      {:packet, :raw},
      {:active, false},
      {:reuseaddr, true},
      {:ip, host}
    ]

    case :gen_tcp.listen(port, opts) do
      {:ok, listen_socket} ->
        # Get actual port (useful if port 0 was specified)
        {:ok, actual_port} = :inet.port(listen_socket)

        Logger.info(
          "[#{common_state.listener_id}] Listener started on TCP #{:inet.ntoa(host)}:#{actual_port}"
        )

        state =
          Map.merge(common_state, %{
            type: :tcp,
            listen_socket: listen_socket,
            socket_path: nil,
            host: host,
            port: actual_port
          })

        # Start accepting connections
        send(self(), :accept)

        {:ok, state}

      {:error, reason} ->
        {:stop, {:listen_error, reason}}
    end
  end

  @impl GenServer
  def handle_call(:address, _from, %{type: :unix, socket_path: path} = state) do
    {:reply, {:unix, path}, state}
  end

  def handle_call(:address, _from, %{type: :tcp, host: host, port: port} = state) do
    {:reply, {:tcp, host, port}, state}
  end

  @impl GenServer
  def handle_info(:accept, state) do
    # Accept is blocking, so we do it in a spawned task to keep the GenServer responsive
    parent = self()
    listener_id = state.listener_id

    Task.start(fn ->
      case :gen_tcp.accept(state.listen_socket) do
        {:ok, client_socket} ->
          # Transfer socket ownership to the listener BEFORE this task exits,
          # otherwise the socket gets closed when the task terminates
          case :gen_tcp.controlling_process(client_socket, parent) do
            :ok ->
              send(parent, {:accepted, client_socket})

            {:error, reason} ->
              Logger.error(
                "[#{listener_id}] Socket handoff to listener failed: #{inspect(reason)}"
              )

              :gen_tcp.close(client_socket)
              send(parent, :accept)
          end

        {:error, :closed} ->
          # Listener was closed, stop accepting
          :ok

        {:error, reason} ->
          Logger.error("[#{listener_id}] Accept failed: #{inspect(reason)}")
          send(parent, :accept)
      end
    end)

    {:noreply, state}
  end

  def handle_info({:accepted, client_socket}, state) do
    # Generate connection ID for this relay
    connection_id = "#{state.listener_id}-#{state.connection_counter}"
    state = %{state | connection_counter: state.connection_counter + 1}

    Logger.info("[#{state.listener_id}] Accepted connection #{connection_id}")

    # Determine transport type based on whether noise_keypair is configured
    transport = if state.noise_keypair, do: :noise, else: :gen_tcp

    # Spawn a relay for this connection
    relay_opts = [
      socket: client_socket,
      transport: transport,
      dataspace_ref: state.dataspace_ref,
      idle_timeout: state.idle_timeout,
      connection_id: connection_id
    ]

    # Add noise_keypair if configured
    relay_opts =
      if state.noise_keypair do
        Keyword.put(relay_opts, :noise_keypair, state.noise_keypair)
      else
        relay_opts
      end

    relay_opts =
      if state.actor_pid do
        Keyword.put(relay_opts, :actor_pid, state.actor_pid)
      else
        relay_opts
      end

    # Use start instead of start_link to avoid exit propagation.
    # The relay is monitored (below), so we'll handle its termination properly.
    # This prevents cascading crashes when a peer sends an error packet.
    case GenServer.start(Relay, relay_opts) do
      {:ok, relay_pid} ->
        # Transfer socket ownership to the relay
        Logger.debug("[#{state.listener_id}] Transferring socket to relay #{connection_id}")

        case :gen_tcp.controlling_process(client_socket, relay_pid) do
          :ok ->
            # Activate the socket now that ownership is transferred
            Relay.activate(relay_pid)

            # Monitor the relay
            Process.monitor(relay_pid)

            state = %{state | relays: [relay_pid | state.relays]}

            # Accept next connection
            send(self(), :accept)

            {:noreply, state}

          {:error, reason} ->
            Logger.error(
              "[#{state.listener_id}] Socket handoff failed for #{connection_id}: #{inspect(reason)}"
            )

            # Stop the relay since handoff failed - guard with Process.alive?
            # to avoid crashing the listener if relay already exited (which may
            # be why controlling_process failed in the first place)
            if Process.alive?(relay_pid) do
              GenServer.stop(relay_pid, :normal)
            end

            :gen_tcp.close(client_socket)

            # Accept next connection
            send(self(), :accept)

            {:noreply, state}
        end

      {:error, reason} ->
        Logger.error(
          "[#{state.listener_id}] Failed to start relay for #{connection_id}: #{inspect(reason)}"
        )

        :gen_tcp.close(client_socket)

        # Accept next connection
        send(self(), :accept)

        {:noreply, state}
    end
  end

  def handle_info({:DOWN, _ref, :process, pid, reason}, state) do
    Logger.debug("[#{state.listener_id}] Relay #{inspect(pid)} terminated: #{inspect(reason)}")
    state = %{state | relays: List.delete(state.relays, pid)}
    {:noreply, state}
  end

  @impl GenServer
  def terminate(reason, state) do
    Logger.info("[#{state.listener_id}] Listener terminating: #{inspect(reason)}")

    # Close listen socket
    :gen_tcp.close(state.listen_socket)

    # Clean up Unix socket file
    if state.socket_path do
      File.rm(state.socket_path)
    end

    # Close all relay connections
    Enum.each(state.relays, fn relay ->
      try do
        Relay.close(relay)
      catch
        _, _ -> :ok
      end
    end)

    :ok
  end
end
