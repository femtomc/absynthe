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
  - `0x87` - IEEE754 Double (varint length 0x08 + 8 bytes big-endian)
  - `0xB0` - SignedInteger (varint length + big-endian 2's complement bytes)
  - `0xB1` - String (varint length + UTF-8 bytes)
  - `0xB2` - ByteString (varint length + raw bytes)
  - `0xB3` - Symbol (varint length + UTF-8 bytes)

  **Compound Types:**
  - `0xB4` - Record (label + fields + end marker)
  - `0xB5` - Sequence (elements + end marker)
  - `0xB6` - Set (elements sorted by Repr + end marker)
  - `0xB7` - Dictionary (pairs sorted by key Repr + end marker)

  **Special:**
  - `0x84` - End marker (closes compound types)
  - `0x85` - Annotation (value + annotation sequence)
  - `0x86` - Embedded (wraps embedded Preserves values)

  ## Canonical Form

  This encoder produces canonical output:
  - Set elements are sorted by their binary representation
  - Dictionary key-value pairs are sorted by key binary representation
  - Integers use minimal two's complement encoding
  - Zero is encoded with empty intbytes

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

  # Tag byte constants per Preserves spec
  @tag_false 0x80
  @tag_true 0x81
  # 0x82 is Float32 (not implemented)
  # 0x83 is reserved
  @tag_end 0x84
  @tag_annotation 0x85
  @tag_embedded 0x86
  @tag_ieee754_double 0x87
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
      <<0xB0, 0x02, 0x00, 0xFF>>
  """
  @spec encode!(Value.t()) :: binary()
  def encode!({:boolean, false}), do: <<@tag_false>>
  def encode!({:boolean, true}), do: <<@tag_true>>

  # IEEE754 Double: tag 0x87 + varint(8) + 8 bytes big-endian
  def encode!({:double, value}) when is_float(value) do
    <<@tag_ieee754_double, 0x08, value::float-big-64>>
  end

  # Special IEEE754 values (BEAM can't represent these as native floats)
  def encode!({:double, :infinity}) do
    <<@tag_ieee754_double, 0x08, 0x7FF0000000000000::big-64>>
  end

  def encode!({:double, :neg_infinity}) do
    <<@tag_ieee754_double, 0x08, 0xFFF0000000000000::big-64>>
  end

  def encode!({:double, :nan}) do
    # Quiet NaN with canonical bit pattern
    <<@tag_ieee754_double, 0x08, 0x7FF8000000000000::big-64>>
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

  # Canonical form: elements sorted by their binary representation
  def encode!({:set, items}) when is_struct(items, MapSet) do
    sorted_items =
      items
      |> MapSet.to_list()
      |> Enum.map(fn item -> {encode!(item), item} end)
      |> Enum.sort_by(fn {encoded, _item} -> encoded end)
      |> Enum.map(fn {encoded, _item} -> encoded end)

    items_bytes = IO.iodata_to_binary(sorted_items)
    <<@tag_set, items_bytes::binary, @tag_end>>
  end

  # Canonical form: pairs sorted by key's binary representation
  def encode!({:dictionary, items}) when is_map(items) do
    sorted_pairs =
      items
      |> Enum.to_list()
      |> Enum.map(fn {k, v} -> {encode!(k), encode!(v), k} end)
      |> Enum.sort_by(fn {key_encoded, _val_encoded, _k} -> key_encoded end)

    pairs_bytes =
      sorted_pairs
      |> Enum.map(fn {key_encoded, val_encoded, _k} ->
        <<key_encoded::binary, val_encoded::binary>>
      end)
      |> IO.iodata_to_binary()

    <<@tag_dictionary, pairs_bytes::binary, @tag_end>>
  end

  # Annotation: tag 0x85 + annotation + value (per spec, no end marker)
  # For a single annotation wrapping a value
  def encode!({:annotated, annotation, value}) do
    annotation_bytes = encode!(annotation)
    value_bytes = encode!(value)
    <<@tag_annotation, annotation_bytes::binary, value_bytes::binary>>
  end

  # Embedded: requires the payload to be a Preserves Value
  def encode!({:embedded, payload}) do
    case payload do
      {tag, _}
      when tag in [
             :boolean,
             :integer,
             :double,
             :string,
             :binary,
             :symbol,
             :record,
             :sequence,
             :set,
             :dictionary,
             :embedded,
             :annotated
           ] ->
        payload_bytes = encode!(payload)
        <<@tag_embedded, payload_bytes::binary>>

      _ ->
        raise ArgumentError,
              "Embedded values must be Preserves Values, got: #{inspect(payload)}. " <>
                "Convert to a Preserves Value first (e.g., use {:string, ...} or {:record, ...})."
    end
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
  Per spec, zero is encoded with empty intbytes (length 0).

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

  Per Preserves spec, intbytes(0) is the empty sequence.

  ## Examples

      iex> Absynthe.Preserves.Encoder.Binary.integer_to_bytes(0)
      <<>>

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
  # Per spec: intbytes(0) is the empty sequence
  def integer_to_bytes(0), do: <<>>

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

  defp positive_to_bytes(0, acc) when byte_size(acc) > 0, do: acc
  defp positive_to_bytes(0, <<>>), do: <<>>

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
    min_val = -Bitwise.bsl(1, bits - 1)
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
end
