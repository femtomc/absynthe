defmodule Absynthe.Preserves.Encoder.Text do
  @moduledoc """
  Text format encoder for Preserves values.

  This module encodes Preserves values to human-readable text format, following
  the Preserves text syntax specification.

  ## Text Syntax Overview

  ### Atoms
  - Booleans: `#t` (true), `#f` (false)
  - Integers: decimal format (e.g., `42`, `-17`)
  - Doubles: `3.14`, `-0.5`, `1e10`, `#inf`, `#-inf`, `#nan`
  - Strings: `"hello"` with JSON escapes
  - ByteStrings: `#x"deadbeef"` (hex format)
  - Symbols: `foo` or `|quoted symbol|` if needed

  ### Compounds
  - Records: `<label field1 field2 ...>`
  - Sequences: `[1 2 3]`
  - Sets: `\#{1 2 3}`
  - Dictionaries: `{key: value ...}` or `{"key": value}`

  ### Special
  - Embedded: `#!value`
  - Annotated: `@annotation value`

  ## Examples

      iex> alias Absynthe.Preserves.{Value, Encoder.Text}
      iex> Text.encode!(Value.boolean(true))
      "#t"

      iex> alias Absynthe.Preserves.{Value, Encoder.Text}
      iex> Text.encode!(Value.integer(42))
      "42"

      iex> alias Absynthe.Preserves.{Value, Encoder.Text}
      iex> Text.encode!(Value.string("hello"))
      "\\"hello\\""
  """

  alias Absynthe.Preserves.Value
  alias Absynthe.Preserves.Encoder.Binary

  @doc """
  Encodes a Preserves value to text format.

  Returns `{:ok, string}` on success or `{:error, reason}` on failure.

  ## Examples

      iex> alias Absynthe.Preserves.{Value, Encoder.Text}
      iex> Text.encode(Value.boolean(false))
      {:ok, "#f"}

      iex> alias Absynthe.Preserves.{Value, Encoder.Text}
      iex> Text.encode(Value.sequence([Value.integer(1), Value.integer(2)]))
      {:ok, "[1 2]"}
  """
  @spec encode(Value.t()) :: {:ok, String.t()} | {:error, term()}
  def encode(value) do
    {:ok, encode!(value)}
  rescue
    e -> {:error, {:encoding_error, e}}
  end

  @doc """
  Encodes a Preserves value to text format, raising on error.

  ## Examples

      iex> alias Absynthe.Preserves.{Value, Encoder.Text}
      iex> Text.encode!(Value.boolean(true))
      "#t"

      iex> alias Absynthe.Preserves.{Value, Encoder.Text}
      iex> Text.encode!(Value.symbol("hello"))
      "hello"
  """
  @spec encode!(Value.t()) :: String.t()
  def encode!({:boolean, true}), do: "#t"
  def encode!({:boolean, false}), do: "#f"

  # Special IEEE754 double values
  def encode!({:double, :infinity}), do: "#inf"
  def encode!({:double, :neg_infinity}), do: "#-inf"
  def encode!({:double, :nan}), do: "#nan"

  # Regular doubles
  def encode!({:double, value}) when is_float(value) do
    cond do
      # Check for negative zero
      value == 0.0 and negative_zero?(value) -> "-0.0"
      # Regular float formatting
      true -> float_to_string(value)
    end
  end

  def encode!({:integer, value}) when is_integer(value) do
    Integer.to_string(value)
  end

  def encode!({:string, value}) when is_binary(value) do
    "\"" <> escape_string(value) <> "\""
  end

  def encode!({:binary, value}) when is_binary(value) do
    "#x\"" <> Base.encode16(value, case: :lower) <> "\""
  end

  def encode!({:symbol, value}) when is_binary(value) do
    if needs_quoting?(value) do
      "|" <> escape_symbol(value) <> "|"
    else
      value
    end
  end

  def encode!({:record, {label, fields}}) do
    label_str = encode!(label)
    fields_str = Enum.map_join(fields, " ", &encode!/1)

    if fields_str == "" do
      "<" <> label_str <> ">"
    else
      "<" <> label_str <> " " <> fields_str <> ">"
    end
  end

  def encode!({:sequence, items}) when is_list(items) do
    items_str = Enum.map_join(items, " ", &encode!/1)
    "[" <> items_str <> "]"
  end

  def encode!({:set, items}) when is_struct(items, MapSet) do
    # Sort for canonical output using binary Repr ordering
    sorted_items =
      items
      |> MapSet.to_list()
      |> Enum.map(fn item -> {Binary.encode!(item), item} end)
      |> Enum.sort_by(fn {encoded, _item} -> encoded end)
      |> Enum.map_join(" ", fn {_encoded, item} -> encode!(item) end)

    "\#{" <> sorted_items <> "}"
  end

  def encode!({:dictionary, items}) when is_map(items) do
    # Sort by key binary representation for canonical output
    sorted_pairs =
      items
      |> Enum.to_list()
      |> Enum.map(fn {k, v} -> {Binary.encode!(k), k, v} end)
      |> Enum.sort_by(fn {encoded_key, _k, _v} -> encoded_key end)

    pairs_str =
      sorted_pairs
      |> Enum.map_join(" ", fn {_encoded_key, k, v} ->
        key_str = encode!(k)
        val_str = encode!(v)

        # Use shorthand for symbol keys
        case k do
          {:symbol, sym} when is_binary(sym) ->
            if needs_quoting?(sym) do
              key_str <> ": " <> val_str
            else
              sym <> ": " <> val_str
            end

          _ ->
            key_str <> ": " <> val_str
        end
      end)

    "{" <> pairs_str <> "}"
  end

  def encode!({:embedded, payload}) do
    codec = Absynthe.Preserves.EmbeddedCodec.get_codec()

    case codec.encode(payload) do
      {:ok, encoded_value} ->
        "#!" <> encode!(encoded_value)

      {:error, reason} ->
        raise ArgumentError,
              "Failed to encode embedded value: #{inspect(reason)}. " <>
                "Payload: #{inspect(payload)}"
    end
  end

  def encode!({:annotated, annotation, value}) do
    "@" <> encode!(annotation) <> " " <> encode!(value)
  end

  def encode!(value) do
    raise ArgumentError, "Cannot encode value: #{inspect(value)}"
  end

  # Private helpers

  defp negative_zero?(x) when is_float(x) do
    x == 0.0 and :math.atan2(x, -1.0) < 0
  end

  defp float_to_string(value) do
    # Use Erlang's float_to_binary with shortest representation
    str = :erlang.float_to_binary(value, [:short])

    # Ensure it has a decimal point (for valid Preserves float syntax)
    if String.contains?(str, ".") or String.contains?(str, "e") do
      str
    else
      str <> ".0"
    end
  end

  defp escape_string(str) do
    str
    |> String.graphemes()
    |> Enum.map(&escape_char/1)
    |> Enum.join()
  end

  defp escape_char("\""), do: "\\\""
  defp escape_char("\\"), do: "\\\\"
  defp escape_char("\n"), do: "\\n"
  defp escape_char("\r"), do: "\\r"
  defp escape_char("\t"), do: "\\t"

  defp escape_char(<<codepoint::utf8>>) when codepoint < 0x20 do
    "\\u" <> String.pad_leading(Integer.to_string(codepoint, 16), 4, "0")
  end

  defp escape_char(char), do: char

  defp escape_symbol(str) do
    str
    |> String.graphemes()
    |> Enum.map(fn
      "|" -> "\\|"
      "\\" -> "\\\\"
      char -> char
    end)
    |> Enum.join()
  end

  defp needs_quoting?(""), do: true

  defp needs_quoting?(str) do
    # Check if first char is valid symbol start
    <<first::utf8, rest::binary>> = str

    if not symbol_start?(first) do
      true
    else
      # Check all remaining chars
      not all_symbol_chars?(rest)
    end
  end

  defp symbol_start?(char) when char in ?a..?z, do: true
  defp symbol_start?(char) when char in ?A..?Z, do: true
  defp symbol_start?(char) when char in [?_, ?+, ?-, ?*, ?/, ?!, ??, ?$, ?%, ?&, ?=, ?~], do: true
  defp symbol_start?(_), do: false

  defp symbol_char?(char) when char in ?0..?9, do: true
  defp symbol_char?(?.), do: true
  defp symbol_char?(char), do: symbol_start?(char)

  defp all_symbol_chars?(<<>>), do: true

  defp all_symbol_chars?(<<char::utf8, rest::binary>>) do
    symbol_char?(char) and all_symbol_chars?(rest)
  end
end
