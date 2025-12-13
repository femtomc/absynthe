defmodule Absynthe.Relay.Framing do
  @moduledoc """
  Packet framing for the Syndicate relay protocol.

  This module handles the low-level framing of Preserves packets for
  transmission over sockets. Preserves values are self-delimiting, so
  framing is straightforward - we just need to encode/decode complete
  values.

  ## Framing Format

  Packets are transmitted as self-delimiting Preserves values in binary
  format. Each packet is a complete Preserves value that can be decoded
  without knowing its length in advance (the binary format includes
  length prefixes for variable-length data).

  ## Buffer Management

  Since TCP is a stream protocol, we may receive partial packets. The
  `decode_from_buffer/1` function handles this by returning:
  - `{:ok, packet, rest}` - Successfully decoded a packet, with remaining bytes
  - `{:incomplete, buffer}` - Need more data to complete the packet
  - `{:error, reason}` - Decoding failed

  ## Example Usage

      # Encoding
      {:ok, bytes} = Framing.encode(%Turn{events: []})
      :gen_tcp.send(socket, bytes)

      # Decoding with buffer management
      buffer = <<>>
      {:ok, data} = :gen_tcp.recv(socket, 0)
      buffer = buffer <> data

      case Framing.decode_from_buffer(buffer) do
        {:ok, packet, rest} ->
          handle_packet(packet)
          continue_with(rest)
        {:incomplete, buffer} ->
          wait_for_more_data(buffer)
        {:error, reason} ->
          handle_error(reason)
      end
  """

  alias Absynthe.Relay.Packet
  alias Absynthe.Preserves.Encoder.Binary, as: Encoder
  alias Absynthe.Preserves.Decoder.Binary, as: Decoder

  @typedoc """
  Result of decoding from a buffer.
  """
  @type decode_result ::
          {:ok, Packet.packet(), binary()}
          | {:incomplete, binary()}
          | {:error, term()}

  @doc """
  Encodes a packet to binary format for transmission.

  ## Parameters

  - `packet` - The packet to encode

  ## Returns

  `{:ok, binary}` on success, `{:error, reason}` on failure.

  ## Examples

      iex> turn = Packet.turn([])
      iex> {:ok, bytes} = Framing.encode(turn)
      iex> is_binary(bytes)
      true
  """
  @spec encode(Packet.packet()) :: {:ok, binary()} | {:error, term()}
  def encode(packet) do
    with {:ok, value} <- Packet.encode(packet),
         {:ok, binary} <- Encoder.encode(value) do
      {:ok, binary}
    end
  end

  @doc """
  Encodes a packet to binary format, raising on error.
  """
  @spec encode!(Packet.packet()) :: binary()
  def encode!(packet) do
    case encode(packet) do
      {:ok, binary} -> binary
      {:error, reason} -> raise "Failed to encode packet: #{inspect(reason)}"
    end
  end

  @doc """
  Attempts to decode a packet from a buffer.

  This function handles partial data by returning `{:incomplete, buffer}`
  when more data is needed.

  ## Parameters

  - `buffer` - Binary data that may contain a complete packet

  ## Returns

  - `{:ok, packet, rest}` - Successfully decoded, with remaining bytes
  - `{:incomplete, buffer}` - Need more data
  - `{:error, reason}` - Decoding failed (protocol error)

  ## Examples

      iex> {:ok, bytes} = Framing.encode(Packet.turn([]))
      iex> {:ok, packet, rest} = Framing.decode_from_buffer(bytes)
      iex> rest
      <<>>
  """
  @spec decode_from_buffer(binary()) :: decode_result()
  def decode_from_buffer(<<>>) do
    {:incomplete, <<>>}
  end

  def decode_from_buffer(buffer) when is_binary(buffer) do
    case Decoder.decode(buffer) do
      {:ok, value, rest} ->
        case Packet.decode(value) do
          {:ok, packet} -> {:ok, packet, rest}
          {:error, reason} -> {:error, {:packet_decode_error, reason}}
        end

      {:error, :incomplete} ->
        {:incomplete, buffer}

      {:error, :empty_input} ->
        {:incomplete, buffer}

      {:error, :truncated_input} ->
        {:incomplete, buffer}

      {:error, :missing_end_marker} ->
        {:incomplete, buffer}

      {:error, :truncated_varint} ->
        {:incomplete, buffer}

      {:error, reason} ->
        {:error, {:preserves_decode_error, reason}}
    end
  end

  @doc """
  Decodes all complete packets from a buffer.

  Returns decoded packets, remaining buffer, and any error that occurred.

  ## Parameters

  - `buffer` - Binary data that may contain multiple packets

  ## Returns

  - `{:ok, packets, remaining_buffer}` - All data decoded without errors
  - `{:error, reason, packets, remaining_buffer}` - Error occurred after decoding some packets.
    The packets decoded before the error are returned, and the buffer contains the corrupted data.

  ## Examples

      iex> {:ok, b1} = Framing.encode(Packet.turn([]))
      iex> {:ok, b2} = Framing.encode(:nop)
      iex> {:ok, packets, rest} = Framing.decode_all(b1 <> b2)
      iex> length(packets)
      2
  """
  @spec decode_all(binary()) ::
          {:ok, [Packet.packet()], binary()}
          | {:error, term(), [Packet.packet()], binary()}
  def decode_all(buffer) do
    decode_all(buffer, [])
  end

  defp decode_all(buffer, acc) do
    case decode_from_buffer(buffer) do
      {:ok, packet, rest} ->
        decode_all(rest, [packet | acc])

      {:incomplete, buffer} ->
        {:ok, Enum.reverse(acc), buffer}

      {:error, reason} ->
        # Return the error along with any successfully decoded packets
        {:error, reason, Enum.reverse(acc), buffer}
    end
  end

  @doc """
  Creates a framing state for incremental decoding.

  Use this when you want to process packets as they arrive over a socket.
  """
  @spec new_state() :: binary()
  def new_state do
    <<>>
  end

  @doc """
  Appends data to the framing buffer and extracts any complete packets.

  ## Parameters

  - `buffer` - Current buffer state
  - `data` - New data received

  ## Returns

  - `{:ok, packets, new_buffer}` - All data decoded without errors
  - `{:error, reason, packets, new_buffer}` - Error occurred after decoding some packets

  ## Examples

      iex> buffer = Framing.new_state()
      iex> {:ok, data} = Framing.encode(Packet.turn([]))
      iex> {:ok, packets, buffer} = Framing.append_and_decode(buffer, data)
      iex> length(packets)
      1
  """
  @spec append_and_decode(binary(), binary()) ::
          {:ok, [Packet.packet()], binary()}
          | {:error, term(), [Packet.packet()], binary()}
  def append_and_decode(buffer, data) when is_binary(buffer) and is_binary(data) do
    decode_all(buffer <> data)
  end
end
