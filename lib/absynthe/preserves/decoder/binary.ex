defmodule Absynthe.Preserves.Decoder.Binary do
  import Bitwise

  @moduledoc """
  Binary format decoder for Preserves values.

  This module implements the Preserves binary encoding specification for decoding
  binary data into Preserves values. The binary format is a compact, self-describing
  serialization format with tag bytes in the range [0x80, 0xBF].

  ## Tag Bytes

  **Atomic Types:**
  - `0x80` - False
  - `0x81` - True
  - `0x87` - IEEE754 Double (varint length + N bytes big-endian)
  - `0xB0` - SignedInteger (varint length, then big-endian 2's complement bytes)
  - `0xB1` - String (varint length, then UTF-8 bytes)
  - `0xB2` - ByteString (varint length, then raw bytes)
  - `0xB3` - Symbol (varint length, then UTF-8 bytes)

  **Compound Types:**
  - `0xB4` - Record (label value, then field values until end marker)
  - `0xB5` - Sequence (element values until end marker)
  - `0xB6` - Set (element values until end marker)
  - `0xB7` - Dictionary (key-value pairs until end marker)

  **Special:**
  - `0x84` - End marker
  - `0x85` - Annotation (value + annotations until end marker)
  - `0x86` - Embedded (wraps a Preserves value)

  ## Examples

      iex> Absynthe.Preserves.Decoder.Binary.decode(<<0x81>>)
      {:ok, {:boolean, true}, <<>>}

      iex> Absynthe.Preserves.Decoder.Binary.decode(<<0xB1, 0x05, "hello">>)
      {:ok, {:string, "hello"}, <<>>}

      iex> Absynthe.Preserves.Decoder.Binary.decode_all(<<0x80>>)
      {:ok, {:boolean, false}}

      iex> Absynthe.Preserves.Decoder.Binary.decode_all!(<<0x81>>)
      {:boolean, true}
  """

  alias Absynthe.Preserves.Value

  # Tag byte constants per Preserves spec
  @tag_false 0x80
  @tag_true 0x81
  # 0x82 is Float32 (legacy, rarely used)
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
  Decode result with value and remaining bytes.
  """
  @type decode_result :: {:ok, Value.t(), binary()} | {:error, term()}

  @typedoc """
  Decode result for complete parsing with no remaining bytes.
  """
  @type decode_all_result :: {:ok, Value.t()} | {:error, term()}

  @doc """
  Decodes a Preserves value from binary data.

  Returns `{:ok, value, rest}` where `value` is the decoded Preserves value and
  `rest` is any remaining unparsed bytes.

  ## Examples

      iex> Absynthe.Preserves.Decoder.Binary.decode(<<0x80>>)
      {:ok, {:boolean, false}, <<>>}

      iex> Absynthe.Preserves.Decoder.Binary.decode(<<0x81, 0x80>>)
      {:ok, {:boolean, true}, <<0x80>>}

      iex> Absynthe.Preserves.Decoder.Binary.decode(<<>>)
      {:error, :empty_input}

      iex> Absynthe.Preserves.Decoder.Binary.decode(<<0x00>>)
      {:error, {:invalid_tag, 0x00}}
  """
  @spec decode(binary()) :: decode_result()
  def decode(binary) when is_binary(binary) do
    case binary do
      <<>> ->
        {:error, :empty_input}

      <<tag, rest::binary>> ->
        decode_value(tag, rest)
    end
  end

  @doc """
  Decodes a Preserves value from binary data, raising on error.

  Returns `{value, rest}` where `value` is the decoded Preserves value and
  `rest` is any remaining unparsed bytes.

  ## Examples

      iex> Absynthe.Preserves.Decoder.Binary.decode!(<<0x80>>)
      {{:boolean, false}, <<>>}

      iex> Absynthe.Preserves.Decoder.Binary.decode!(<<0x81, 0x80>>)
      {{:boolean, true}, <<0x80>>}
  """
  @spec decode!(binary()) :: {Value.t(), binary()}
  def decode!(binary) when is_binary(binary) do
    case decode(binary) do
      {:ok, value, rest} ->
        {value, rest}

      {:error, reason} ->
        raise ArgumentError, "Failed to decode binary: #{inspect(reason)}"
    end
  end

  @doc """
  Decodes a Preserves value from binary data, ensuring no trailing bytes.

  Returns `{:ok, value}` if the entire binary is consumed, or an error otherwise.

  ## Examples

      iex> Absynthe.Preserves.Decoder.Binary.decode_all(<<0x80>>)
      {:ok, {:boolean, false}}

      iex> Absynthe.Preserves.Decoder.Binary.decode_all(<<0x81, 0x80>>)
      {:error, :trailing_bytes}

      iex> Absynthe.Preserves.Decoder.Binary.decode_all(<<>>)
      {:error, :empty_input}
  """
  @spec decode_all(binary()) :: decode_all_result()
  def decode_all(binary) when is_binary(binary) do
    case decode(binary) do
      {:ok, value, <<>>} ->
        {:ok, value}

      {:ok, _value, _rest} ->
        {:error, :trailing_bytes}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Decodes a Preserves value from binary data, ensuring no trailing bytes and raising on error.

  Returns the decoded value if successful.

  ## Examples

      iex> Absynthe.Preserves.Decoder.Binary.decode_all!(<<0x80>>)
      {:boolean, false}

      iex> Absynthe.Preserves.Decoder.Binary.decode_all!(<<0x81>>)
      {:boolean, true}
  """
  @spec decode_all!(binary()) :: Value.t()
  def decode_all!(binary) when is_binary(binary) do
    case decode_all(binary) do
      {:ok, value} ->
        value

      {:error, reason} ->
        raise ArgumentError, "Failed to decode binary: #{inspect(reason)}"
    end
  end

  # Private helper functions

  # Decode value based on tag byte
  defp decode_value(@tag_false, rest), do: {:ok, Value.boolean(false), rest}
  defp decode_value(@tag_true, rest), do: {:ok, Value.boolean(true), rest}

  # IEEE754 Double: tag 0x87 + varint(8) + 8 bytes big-endian
  # Per spec, length is fixed at 8 for 64-bit double
  defp decode_value(@tag_ieee754_double, rest) do
    with {:ok, byte_count, rest} <- decode_varint(rest) do
      case byte_count do
        8 ->
          with {:ok, double_bytes, rest} <- take_bytes(rest, 8) do
            <<double_bits::64-big-unsigned>> = double_bytes
            value = decode_ieee754_double(double_bits)
            {:ok, Value.double(value), rest}
          end

        _ ->
          {:error, {:invalid_double_length, byte_count}}
      end
    end
  end

  defp decode_value(@tag_signed_integer, rest) do
    with {:ok, byte_count, rest} <- decode_varint(rest),
         {:ok, int_value, rest} <- decode_signed_integer(rest, byte_count) do
      {:ok, Value.integer(int_value), rest}
    end
  end

  defp decode_value(@tag_string, rest) do
    with {:ok, byte_count, rest} <- decode_varint(rest),
         {:ok, string_bytes, rest} <- take_bytes(rest, byte_count) do
      case String.valid?(string_bytes) do
        true ->
          {:ok, Value.string(string_bytes), rest}

        false ->
          {:error, :invalid_utf8_string}
      end
    end
  end

  defp decode_value(@tag_bytestring, rest) do
    with {:ok, byte_count, rest} <- decode_varint(rest),
         {:ok, bytes, rest} <- take_bytes(rest, byte_count) do
      {:ok, Value.binary(bytes), rest}
    end
  end

  defp decode_value(@tag_symbol, rest) do
    with {:ok, byte_count, rest} <- decode_varint(rest),
         {:ok, symbol_bytes, rest} <- take_bytes(rest, byte_count) do
      case String.valid?(symbol_bytes) do
        true ->
          {:ok, Value.symbol(symbol_bytes), rest}

        false ->
          {:error, :invalid_utf8_symbol}
      end
    end
  end

  defp decode_value(@tag_record, rest) do
    with {:ok, label, rest} <- decode(rest),
         {:ok, fields, rest} <- decode_until_end(rest, []) do
      {:ok, Value.record(label, Enum.reverse(fields)), rest}
    end
  end

  defp decode_value(@tag_sequence, rest) do
    with {:ok, elements, rest} <- decode_until_end(rest, []) do
      {:ok, Value.sequence(Enum.reverse(elements)), rest}
    end
  end

  defp decode_value(@tag_set, rest) do
    with {:ok, elements, rest} <- decode_until_end(rest, []) do
      {:ok, Value.set(Enum.reverse(elements)), rest}
    end
  end

  defp decode_value(@tag_dictionary, rest) do
    with {:ok, pairs, rest} <- decode_dictionary_pairs(rest, []) do
      {:ok, Value.dictionary(Enum.reverse(pairs)), rest}
    end
  end

  # Annotation: tag 0x85 + annotation + value (per spec, no end marker)
  defp decode_value(@tag_annotation, rest) do
    with {:ok, annotation, rest} <- decode(rest),
         {:ok, value, rest} <- decode(rest) do
      {:ok, Value.annotated(annotation, value), rest}
    end
  end

  defp decode_value(@tag_embedded, rest) do
    with {:ok, wire_value, rest} <- decode(rest) do
      codec = Absynthe.Preserves.EmbeddedCodec.get_codec()

      case codec.decode(wire_value) do
        {:ok, payload} ->
          {:ok, Value.embedded(payload), rest}

        {:error, reason} ->
          {:error, {:embedded_decode_error, reason}}
      end
    end
  end

  defp decode_value(@tag_end, _rest) do
    {:error, :unexpected_end_marker}
  end

  defp decode_value(tag, _rest) when tag >= 0x80 and tag <= 0xBF do
    {:error, {:unsupported_tag, tag}}
  end

  defp decode_value(tag, _rest) do
    {:error, {:invalid_tag, tag}}
  end

  # Decode IEEE754 double, handling special values that BEAM can't represent natively
  defp decode_ieee754_double(bits) do
    # Extract sign, exponent, and mantissa
    sign = bits >>> 63
    exponent = (bits >>> 52) &&& 0x7FF
    mantissa = bits &&& 0xFFFFFFFFFFFFF

    cond do
      # Infinity: exponent all 1s, mantissa all 0s
      exponent == 0x7FF and mantissa == 0 ->
        if sign == 0, do: :infinity, else: :neg_infinity

      # NaN: exponent all 1s, mantissa non-zero
      exponent == 0x7FF and mantissa != 0 ->
        :nan

      # Normal or subnormal float - try to decode as native float
      true ->
        <<float::64-big-float>> = <<bits::64-big-unsigned>>
        # Handle negative zero by checking the sign bit
        if float == 0.0 and sign == 1, do: -0.0, else: float
    end
  end

  # Decode varint (variable-length integer)
  @doc false
  @spec decode_varint(binary()) :: {:ok, non_neg_integer(), binary()} | {:error, term()}
  def decode_varint(binary), do: decode_varint(binary, 0, 0)

  defp decode_varint(<<>>, _acc, _shift) do
    {:error, :truncated_varint}
  end

  defp decode_varint(<<byte, rest::binary>>, acc, shift) when shift < 63 do
    # Extract the low 7 bits
    value = byte &&& 0x7F
    # Add to accumulator with appropriate shift
    acc = acc ||| value <<< shift

    # Check if high bit is set (continuation bit)
    if (byte &&& 0x80) != 0 do
      # More bytes to come
      decode_varint(rest, acc, shift + 7)
    else
      # Last byte
      {:ok, acc, rest}
    end
  end

  defp decode_varint(_binary, _acc, _shift) do
    {:error, :varint_overflow}
  end

  # Decode signed integer from 2's complement big-endian bytes
  @doc false
  @spec decode_signed_integer(binary(), non_neg_integer()) ::
          {:ok, integer(), binary()} | {:error, term()}
  def decode_signed_integer(binary, byte_count) do
    case take_bytes(binary, byte_count) do
      {:ok, int_bytes, rest} ->
        value = decode_twos_complement(int_bytes)
        {:ok, value, rest}

      error ->
        error
    end
  end

  # Decode 2's complement representation
  # Per spec: empty intbytes represents 0
  defp decode_twos_complement(<<>>) do
    0
  end

  defp decode_twos_complement(bytes) do
    # Convert to integer treating as unsigned big-endian
    <<unsigned::big-unsigned-size(byte_size(bytes))-unit(8)>> = bytes

    # Check if the sign bit is set (high bit of first byte)
    bit_size = byte_size(bytes) * 8
    sign_bit_mask = 1 <<< (bit_size - 1)

    if (unsigned &&& sign_bit_mask) != 0 do
      # Negative number - convert from 2's complement
      # Subtract 2^bit_size to get the negative value
      unsigned - (1 <<< bit_size)
    else
      # Positive number
      unsigned
    end
  end

  # Take exactly N bytes from binary
  defp take_bytes(binary, count) when byte_size(binary) >= count do
    <<bytes::binary-size(count), rest::binary>> = binary
    {:ok, bytes, rest}
  end

  defp take_bytes(_binary, _count) do
    {:error, :truncated_input}
  end

  # Decode values until end marker is encountered
  defp decode_until_end(<<@tag_end, rest::binary>>, acc) do
    {:ok, acc, rest}
  end

  defp decode_until_end(<<>>, _acc) do
    {:error, :missing_end_marker}
  end

  defp decode_until_end(binary, acc) do
    case decode(binary) do
      {:ok, value, rest} ->
        decode_until_end(rest, [value | acc])

      error ->
        error
    end
  end

  # Decode dictionary key-value pairs until end marker
  defp decode_dictionary_pairs(<<@tag_end, rest::binary>>, acc) do
    {:ok, acc, rest}
  end

  defp decode_dictionary_pairs(<<>>, _acc) do
    {:error, :missing_end_marker}
  end

  defp decode_dictionary_pairs(binary, acc) do
    with {:ok, key, rest} <- decode(binary),
         {:ok, value, rest} <- decode(rest) do
      decode_dictionary_pairs(rest, [{key, value} | acc])
    end
  end
end
