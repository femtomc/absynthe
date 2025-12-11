defmodule Absynthe.Preserves.Encoder.Binary do
  @moduledoc """
  Binary format encoder for Preserves values.

  Implements the Preserves binary encoding specification, which uses tag bytes
  to identify value types followed by their encoded representations.

  ## Tag Bytes

  All tag bytes are in the range [0x80, 0xBF]:

  **Atomic Types:**
  - `0x80` - False
  - `0x81` - True
  - `0x82` - Float (4 bytes IEEE 754 big-endian)
  - `0x83` - Double (8 bytes IEEE 754 big-endian)
  - `0xB0` - SignedInteger (varint length + big-endian 2's complement bytes)
  - `0xB1` - String (varint length + UTF-8 bytes)
  - `0xB2` - ByteString (varint length + raw bytes)
  - `0xB3` - Symbol (varint length + UTF-8 bytes)

  **Compound Types:**
  - `0xB4` - Record (label + fields + end marker)
  - `0xB5` - Sequence (elements + end marker)
  - `0xB6` - Set (sorted elements + end marker)
  - `0xB7` - Dictionary (sorted key-value pairs + end marker)

  **Special:**
  - `0x84` - End marker (closes compound types)
  - `0x86` - Embedded (wraps embedded values)

  ## Examples

      iex> alias Absynthe.Preserves.Value
      iex> alias Absynthe.Preserves.Encoder.Binary
      iex> {:ok, encoded} = Binary.encode(Value.boolean(true))
      iex> encoded
      <<0x81>>

      iex> alias Absynthe.Preserves.Value
      iex> alias Absynthe.Preserves.Encoder.Binary
      iex> {:ok, encoded} = Binary.encode(Value.integer(42))
      iex> encoded
      <<0xB0, 0x01, 0x2A>>

      iex> alias Absynthe.Preserves.Value
      iex> alias Absynthe.Preserves.Encoder.Binary
      iex> {:ok, encoded} = Binary.encode(Value.string("hi"))
      iex> encoded
      <<0xB1, 0x02, 0x68, 0x69>>
  """

  alias Absynthe.Preserves.Value

  # Tag byte constants
  @tag_false 0x80
  @tag_true 0x81
  # 0x82 is reserved for Float (not implemented, we use Double)
  @tag_double 0x83
  @tag_end 0x84
  @tag_embedded 0x86
  @tag_signed_integer 0xB0
  @tag_string 0xB1
  @tag_bytestring 0xB2
  @tag_symbol 0xB3
  @tag_record 0xB4
  @tag_sequence 0xB5
  @tag_set 0xB6
  @tag_dictionary 0xB7

  @typedoc """
  Encoding error reasons.
  """
  @type error_reason :: {:unsupported_type, term()} | {:encoding_error, term()}

  @doc """
  Encodes a Preserves value to binary format.

  Returns `{:ok, binary}` on success or `{:error, reason}` on failure.

  ## Examples

      iex> alias Absynthe.Preserves.Value
      iex> alias Absynthe.Preserves.Encoder.Binary
      iex> Binary.encode(Value.boolean(false))
      {:ok, <<0x80>>}

      iex> alias Absynthe.Preserves.Value
      iex> alias Absynthe.Preserves.Encoder.Binary
      iex> Binary.encode(Value.integer(0))
      {:ok, <<0xB0, 0x00>>}
  """
  @spec encode(Value.t()) :: {:ok, binary()} | {:error, error_reason()}
  def encode(value) do
    {:ok, encode!(value)}
  rescue
    e -> {:error, {:encoding_error, e}}
  end

  @doc """
  Encodes a Preserves value to binary format.

  Returns the encoded binary or raises an error on failure.

  ## Examples

      iex> alias Absynthe.Preserves.Value
      iex> alias Absynthe.Preserves.Encoder.Binary
      iex> Binary.encode!(Value.boolean(true))
      <<0x81>>

      iex> alias Absynthe.Preserves.Value
      iex> alias Absynthe.Preserves.Encoder.Binary
      iex> Binary.encode!(Value.integer(255))
      <<0xB0, 0x01, 0xFF>>
  """
  @spec encode!(Value.t()) :: binary()
  def encode!({:boolean, false}), do: <<@tag_false>>
  def encode!({:boolean, true}), do: <<@tag_true>>

  def encode!({:double, value}) when is_float(value) do
    <<@tag_double, value::float-big-64>>
  end

  def encode!({:integer, value}) when is_integer(value) do
    encode_signed_integer(value)
  end

  def encode!({:string, value}) when is_binary(value) do
    encode_length_prefixed(@tag_string, value)
  end

  def encode!({:binary, value}) when is_binary(value) do
    encode_length_prefixed(@tag_bytestring, value)
  end

  def encode!({:symbol, value}) when is_binary(value) do
    encode_length_prefixed(@tag_symbol, value)
  end

  def encode!({:record, {label, fields}}) do
    label_bytes = encode!(label)
    fields_bytes = Enum.map_join(fields, &encode!/1)
    <<@tag_record, label_bytes::binary, fields_bytes::binary, @tag_end>>
  end

  def encode!({:sequence, items}) when is_list(items) do
    items_bytes = Enum.map_join(items, &encode!/1)
    <<@tag_sequence, items_bytes::binary, @tag_end>>
  end

  def encode!({:set, items}) when is_struct(items, MapSet) do
    # Convert to list and sort
    sorted_items = items |> MapSet.to_list() |> Enum.sort()
    items_bytes = Enum.map_join(sorted_items, &encode!/1)
    <<@tag_set, items_bytes::binary, @tag_end>>
  end

  def encode!({:dictionary, items}) when is_map(items) do
    # Convert to list, sort by key, then encode
    sorted_pairs =
      items
      |> Enum.to_list()
      |> Enum.sort_by(fn {k, _v} -> k end)

    pairs_bytes =
      sorted_pairs
      |> Enum.map_join(fn {k, v} ->
        key_bytes = encode!(k)
        value_bytes = encode!(v)
        <<key_bytes::binary, value_bytes::binary>>
      end)

    <<@tag_dictionary, pairs_bytes::binary, @tag_end>>
  end

  def encode!({:embedded, ref}) do
    # For embedded values, we need to encode the embedded term
    # The specification says to encode the embedded value itself
    # For now, we'll wrap it in a way that preserves the structure
    # This might need adjustment based on how embedded values should be handled
    embedded_bytes = encode_embedded_term(ref)
    <<@tag_embedded, embedded_bytes::binary>>
  end

  # Fallback for unknown types
  def encode!(value) do
    raise ArgumentError, "Cannot encode value: #{inspect(value)}"
  end

  @doc """
  Encodes a non-negative integer as a varint.

  Varint encoding uses the high bit of each byte as a continuation flag.
  The low 7 bits of each byte contain payload data in little-endian order.

  ## Examples

      iex> Absynthe.Preserves.Encoder.Binary.encode_varint(0)
      <<0x00>>

      iex> Absynthe.Preserves.Encoder.Binary.encode_varint(127)
      <<0x7F>>

      iex> Absynthe.Preserves.Encoder.Binary.encode_varint(128)
      <<0x80, 0x01>>

      iex> Absynthe.Preserves.Encoder.Binary.encode_varint(300)
      <<0xAC, 0x02>>
  """
  @spec encode_varint(non_neg_integer()) :: binary()
  def encode_varint(n) when is_integer(n) and n >= 0 do
    encode_varint_impl(n, <<>>)
  end

  defp encode_varint_impl(n, acc) when n < 128 do
    <<acc::binary, n::8>>
  end

  defp encode_varint_impl(n, acc) do
    # Take low 7 bits and set high bit to indicate continuation
    byte = Bitwise.bor(Bitwise.band(n, 0x7F), 0x80)
    # Shift right by 7 bits for next iteration
    encode_varint_impl(Bitwise.bsr(n, 7), <<acc::binary, byte::8>>)
  end

  @doc """
  Encodes a signed integer in Preserves format.

  Uses minimal big-endian two's complement representation with a varint length prefix.

  ## Examples

      iex> Absynthe.Preserves.Encoder.Binary.encode_signed_integer(0)
      <<0xB0, 0x00>>

      iex> Absynthe.Preserves.Encoder.Binary.encode_signed_integer(42)
      <<0xB0, 0x01, 0x2A>>

      iex> Absynthe.Preserves.Encoder.Binary.encode_signed_integer(-1)
      <<0xB0, 0x01, 0xFF>>

      iex> Absynthe.Preserves.Encoder.Binary.encode_signed_integer(255)
      <<0xB0, 0x02, 0x00, 0xFF>>

      iex> Absynthe.Preserves.Encoder.Binary.encode_signed_integer(-128)
      <<0xB0, 0x01, 0x80>>
  """
  @spec encode_signed_integer(integer()) :: binary()
  def encode_signed_integer(n) when is_integer(n) do
    bytes = integer_to_bytes(n)
    length_varint = encode_varint(byte_size(bytes))
    <<@tag_signed_integer, length_varint::binary, bytes::binary>>
  end

  @doc """
  Converts a signed integer to its minimal big-endian two's complement byte representation.

  ## Examples

      iex> Absynthe.Preserves.Encoder.Binary.integer_to_bytes(0)
      <<0x00>>

      iex> Absynthe.Preserves.Encoder.Binary.integer_to_bytes(1)
      <<0x01>>

      iex> Absynthe.Preserves.Encoder.Binary.integer_to_bytes(-1)
      <<0xFF>>

      iex> Absynthe.Preserves.Encoder.Binary.integer_to_bytes(127)
      <<0x7F>>

      iex> Absynthe.Preserves.Encoder.Binary.integer_to_bytes(128)
      <<0x00, 0x80>>

      iex> Absynthe.Preserves.Encoder.Binary.integer_to_bytes(-128)
      <<0x80>>

      iex> Absynthe.Preserves.Encoder.Binary.integer_to_bytes(-129)
      <<0xFF, 0x7F>>
  """
  @spec integer_to_bytes(integer()) :: binary()
  def integer_to_bytes(0), do: <<0>>

  def integer_to_bytes(n) when n > 0 do
    # For positive integers, convert to bytes and ensure high bit is 0
    bytes = positive_to_bytes(n, <<>>)

    # Check if high bit is set (would be interpreted as negative)
    <<high_byte::8, _rest::binary>> = bytes

    if Bitwise.band(high_byte, 0x80) != 0 do
      # Prepend zero byte to ensure it's interpreted as positive
      <<0, bytes::binary>>
    else
      bytes
    end
  end

  def integer_to_bytes(n) when n < 0 do
    # For negative integers, we need two's complement representation
    # Find the minimal byte size needed
    negative_to_bytes(n)
  end

  defp positive_to_bytes(0, <<>>), do: <<0>>
  defp positive_to_bytes(0, acc), do: acc

  defp positive_to_bytes(n, acc) do
    byte = Bitwise.band(n, 0xFF)
    positive_to_bytes(Bitwise.bsr(n, 8), <<byte, acc::binary>>)
  end

  defp negative_to_bytes(n) do
    # For negative numbers, we use two's complement
    # We need to find the minimal representation
    bit_size = minimal_bits_for_negative(n)
    byte_size = div(bit_size + 7, 8)
    <<n::big-signed-integer-size(byte_size)-unit(8)>>
  end

  defp minimal_bits_for_negative(n) do
    # For negative numbers, find how many bits we need
    # A negative number needs enough bits so that sign extension works
    minimal_bits_for_negative(n, 8)
  end

  defp minimal_bits_for_negative(n, bits) do
    # Check if the number fits in the given number of bits
    min_val = -(Bitwise.bsl(1, bits - 1))
    max_val = Bitwise.bsl(1, bits - 1) - 1

    if n >= min_val and n <= max_val do
      bits
    else
      minimal_bits_for_negative(n, bits + 8)
    end
  end

  # Helper function to encode length-prefixed data
  defp encode_length_prefixed(tag, data) when is_binary(data) do
    length_varint = encode_varint(byte_size(data))
    <<tag, length_varint::binary, data::binary>>
  end

  # Helper function to encode embedded terms
  # This is a placeholder - the exact encoding depends on the domain model
  defp encode_embedded_term(term) do
    # For now, we'll try to encode it as a Value if it is one
    # Otherwise, we'll need a different strategy
    case term do
      {tag, _} when tag in [:boolean, :integer, :double, :string, :binary, :symbol, :record, :sequence, :set, :dictionary] ->
        encode!(term)
      _ ->
        # For non-Value embedded terms, we need to serialize them somehow
        # This might require application-specific logic
        # For now, we'll encode them as a binary representation using :erlang.term_to_binary
        encoded = :erlang.term_to_binary(term)
        encode_length_prefixed(@tag_bytestring, encoded)
    end
  end
end
