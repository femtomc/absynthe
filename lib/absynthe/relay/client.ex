defmodule Absynthe.Relay.Client do
  @moduledoc """
  Client-side relay connection support.

  This module provides functions to connect to a remote Syndicate relay server,
  optionally using Noise protocol encryption for secure transport.

  ## Usage

  ### Plaintext Connection (for testing/local)

      # Connect to a Unix socket
      {:ok, client} = Client.connect_unix("/tmp/syndicate.sock")

      # Connect to a TCP socket
      {:ok, client} = Client.connect_tcp("127.0.0.1", 8080)

  ### Encrypted Connection (recommended for production)

      # Get server's public key (out of band, or from broker)
      server_public_key = Broker.noise_public_key(broker)

      # Connect with Noise encryption
      {:ok, client} = Client.connect_unix("/tmp/syndicate.sock",
        noise_server_key: server_public_key
      )

  ## Wire Protocol

  After connection (and optional Noise handshake), the client uses the standard
  Syndicate relay protocol with Preserves-encoded packets.
  """

  require Logger

  alias Absynthe.Relay.Transport.Noise
  alias Absynthe.Relay.Framing

  @typedoc """
  Client connection state.
  """
  @type t :: %__MODULE__{
          socket: port(),
          transport: :gen_tcp | :noise,
          noise_session: Noise.session() | nil,
          recv_buffer: binary()
        }

  defstruct [:socket, :transport, :noise_session, recv_buffer: <<>>]

  @connect_timeout 5_000
  @recv_timeout 5_000

  @doc """
  Connects to a relay server via Unix domain socket.

  ## Options

  - `:noise_server_key` - Server's 32-byte public key for Noise encryption (optional)
  - `:connect_timeout` - Connection timeout in milliseconds (default: 5000)

  ## Returns

  `{:ok, client}` on success, `{:error, reason}` on failure.
  """
  @spec connect_unix(String.t(), Keyword.t()) :: {:ok, t()} | {:error, term()}
  def connect_unix(path, opts \\ []) do
    connect_timeout = Keyword.get(opts, :connect_timeout, @connect_timeout)
    noise_server_key = Keyword.get(opts, :noise_server_key)

    case :gen_tcp.connect({:local, path}, 0, [:binary, {:active, false}], connect_timeout) do
      {:ok, socket} ->
        maybe_noise_handshake(socket, noise_server_key)

      {:error, reason} ->
        {:error, {:connect_failed, reason}}
    end
  end

  @doc """
  Connects to a relay server via TCP.

  ## Options

  - `:noise_server_key` - Server's 32-byte public key for Noise encryption (optional)
  - `:connect_timeout` - Connection timeout in milliseconds (default: 5000)

  ## Returns

  `{:ok, client}` on success, `{:error, reason}` on failure.
  """
  @spec connect_tcp(String.t() | tuple(), integer(), Keyword.t()) :: {:ok, t()} | {:error, term()}
  def connect_tcp(host, port, opts \\ []) do
    connect_timeout = Keyword.get(opts, :connect_timeout, @connect_timeout)
    noise_server_key = Keyword.get(opts, :noise_server_key)

    # Convert string host to charlist
    host =
      cond do
        is_binary(host) -> String.to_charlist(host)
        is_tuple(host) -> host
        true -> host
      end

    case :gen_tcp.connect(host, port, [:binary, {:active, false}], connect_timeout) do
      {:ok, socket} ->
        maybe_noise_handshake(socket, noise_server_key)

      {:error, reason} ->
        {:error, {:connect_failed, reason}}
    end
  end

  defp maybe_noise_handshake(socket, nil) do
    # No encryption - return plaintext client
    {:ok,
     %__MODULE__{
       socket: socket,
       transport: :gen_tcp,
       noise_session: nil
     }}
  end

  defp maybe_noise_handshake(socket, server_public_key) do
    # Perform Noise NK handshake
    case Noise.client_handshake(socket, server_public_key) do
      {:ok, noise_session} ->
        Logger.debug("Client Noise handshake complete")

        {:ok,
         %__MODULE__{
           socket: socket,
           transport: :noise,
           noise_session: noise_session
         }}

      {:error, reason} ->
        :gen_tcp.close(socket)
        {:error, {:noise_handshake_failed, reason}}
    end
  end

  @doc """
  Sends a packet to the server.

  ## Parameters

  - `client` - Client connection
  - `packet` - Packet to send (will be encoded via Framing)

  ## Returns

  `{:ok, client}` on success (client may have updated Noise session),
  `{:error, reason}` on failure.
  """
  @spec send_packet(t(), term()) :: {:ok, t()} | {:error, term()}
  def send_packet(%__MODULE__{} = client, packet) do
    case Framing.encode(packet) do
      {:ok, data} ->
        send_data(client, data)

      {:error, reason} ->
        {:error, {:encode_error, reason}}
    end
  end

  @doc """
  Sends raw binary data to the server.

  For Noise transport, data is encrypted before sending.
  """
  @spec send_data(t(), binary()) :: {:ok, t()} | {:error, term()}
  def send_data(%__MODULE__{transport: :gen_tcp, socket: socket} = client, data) do
    case :gen_tcp.send(socket, data) do
      :ok -> {:ok, client}
      {:error, reason} -> {:error, {:send_failed, reason}}
    end
  end

  def send_data(
        %__MODULE__{transport: :noise, socket: socket, noise_session: session} = client,
        data
      ) do
    case Noise.encrypt(session, data) do
      {:ok, ciphertext, new_session} ->
        # Send length-prefixed ciphertext
        length = byte_size(ciphertext)
        packet = <<length::16-big, ciphertext::binary>>

        case :gen_tcp.send(socket, packet) do
          :ok -> {:ok, %{client | noise_session: new_session}}
          {:error, reason} -> {:error, {:send_failed, reason}}
        end

      {:error, reason} ->
        {:error, {:encrypt_failed, reason}}
    end
  end

  @doc """
  Receives packets from the server.

  ## Options

  - `:timeout` - Receive timeout in milliseconds (default: 5000)

  ## Returns

  `{:ok, packets, client}` on success (client may have updated state),
  `{:error, reason}` on failure.
  """
  @spec recv(t(), Keyword.t()) :: {:ok, [term()], t()} | {:error, term()}
  def recv(client, opts \\ [])

  def recv(%__MODULE__{transport: :gen_tcp} = client, opts) do
    timeout = Keyword.get(opts, :timeout, @recv_timeout)

    case :gen_tcp.recv(client.socket, 0, timeout) do
      {:ok, data} ->
        case Framing.append_and_decode(client.recv_buffer, data) do
          {:ok, packets, buffer} ->
            {:ok, packets, %{client | recv_buffer: buffer}}

          {:error, reason, packets, buffer} ->
            # Return any successfully decoded packets before error
            if packets == [] do
              {:error, {:decode_error, reason}}
            else
              {:ok, packets, %{client | recv_buffer: buffer}}
            end
        end

      {:error, reason} ->
        {:error, {:recv_failed, reason}}
    end
  end

  def recv(%__MODULE__{transport: :noise} = client, opts) do
    timeout = Keyword.get(opts, :timeout, @recv_timeout)
    recv_noise(client, timeout)
  end

  defp recv_noise(client, timeout) do
    # Buffer any existing data
    buffer = client.recv_buffer

    # Try to decode a complete Noise message from buffer
    case try_decode_noise_message(buffer, client.noise_session) do
      {:ok, plaintext, new_session, remaining} ->
        # Decode the plaintext as framing
        case Framing.append_and_decode(Framing.new_state(), plaintext) do
          {:ok, packets, _framing_buffer} ->
            {:ok, packets, %{client | noise_session: new_session, recv_buffer: remaining}}

          {:error, reason, _packets, _buffer} ->
            {:error, {:decode_error, reason}}
        end

      :need_more_data ->
        # Need to read more data from socket
        case :gen_tcp.recv(client.socket, 0, timeout) do
          {:ok, data} ->
            recv_noise(%{client | recv_buffer: buffer <> data}, timeout)

          {:error, reason} ->
            {:error, {:recv_failed, reason}}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp try_decode_noise_message(buffer, _session) when byte_size(buffer) < 2 do
    :need_more_data
  end

  defp try_decode_noise_message(<<length::16-big, rest::binary>>, session) do
    if byte_size(rest) < length do
      :need_more_data
    else
      <<ciphertext::binary-size(length), remaining::binary>> = rest

      case Noise.decrypt(session, ciphertext) do
        {:ok, plaintext, new_session} ->
          {:ok, plaintext, new_session, remaining}

        {:error, reason} ->
          {:error, {:decrypt_failed, reason}}
      end
    end
  end

  @doc """
  Closes the client connection.
  """
  @spec close(t()) :: :ok
  def close(%__MODULE__{transport: :gen_tcp, socket: socket}) do
    :gen_tcp.close(socket)
  end

  def close(%__MODULE__{transport: :noise, socket: socket, noise_session: session}) do
    if session do
      Noise.close(session)
    end

    :gen_tcp.close(socket)
  end

  @doc """
  Returns the handshake hash for channel binding.

  Only available for Noise connections. Returns `nil` for plaintext connections.
  """
  @spec handshake_hash(t()) :: binary() | nil
  def handshake_hash(%__MODULE__{noise_session: nil}), do: nil

  def handshake_hash(%__MODULE__{noise_session: session}) do
    Noise.handshake_hash(session)
  end
end
