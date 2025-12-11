defmodule Absynthe.Preserves.Decoder.Text do
  @moduledoc """
  Text format decoder for Preserves values.

  This module implements a parser for the Preserves text syntax, which is a superset
  of JSON with additional features for representing the full Preserves data model.

  ## Text Syntax Overview

  ### Atoms
  - Booleans: `#t` (true), `#f` (false)
  - Integers: `42`, `-17`, `0x2a` (hex), `0o52` (octal), `0b101010` (binary)
  - Doubles: `3.14`, `-0.5`, `1e10`, `1.5e-3`, `#inf`, `#-inf`, `#nan`
  - Strings: `"hello"`, `"line1\\nline2"` (JSON escapes)
  - ByteStrings: `#"..."` or `#x"deadbeef"` (hex) or `#[base64data]`
  - Symbols: `foo`, `my-symbol`, `+`, `-`, or `|quoted symbol|`

  ### Compounds
  - Records: `<label field1 field2 ...>` e.g., `<point 10 20>`
  - Sequences: `[1 2 3]` or `[1, 2, 3]` (commas optional, JSON compatible)
  - Sets: `\#{1 2 3}`
  - Dictionaries: `{key1: value1 key2: value2}` or `{"key": value}` (JSON compatible)

  ### Special
  - Embedded: `#!value`
  - Comments: `;` to end of line, or `#;value` (comment out next value)

  ## Examples

      iex> Absynthe.Preserves.Decoder.Text.decode("#t")
      {:ok, {:boolean, true}, ""}

      iex> Absynthe.Preserves.Decoder.Text.decode("42")
      {:ok, {:integer, 42}, ""}

      iex> Absynthe.Preserves.Decoder.Text.decode("[1 2 3]")
      {:ok, {:sequence, [{:integer, 1}, {:integer, 2}, {:integer, 3}]}, ""}

      iex> Absynthe.Preserves.Decoder.Text.decode("<point 10 20>")
      {:ok, {:record, {{:symbol, "point"}, [{:integer, 10}, {:integer, 20}]}}, ""}

      iex> Absynthe.Preserves.Decoder.Text.decode_all!("42")
      {:integer, 42}
  """

  alias Absynthe.Preserves.Value

  @type parse_result :: {:ok, Value.t(), String.t()} | {:error, String.t(), non_neg_integer()}

  # Public API

  @doc """
  Decodes a Preserves value from text format.

  Returns `{:ok, value, rest}` on success, where `value` is the parsed Preserves value
  and `rest` is the remaining unparsed input.

  Returns `{:error, reason, position}` on failure.

  ## Examples

      iex> Absynthe.Preserves.Decoder.Text.decode("42")
      {:ok, {:integer, 42}, ""}

      iex> Absynthe.Preserves.Decoder.Text.decode("#t more")
      {:ok, {:boolean, true}, " more"}
  """
  @spec decode(String.t()) :: parse_result()
  def decode(input) when is_binary(input) do
    case parse_value(input, 0) do
      {:ok, value, rest, _pos} ->
        {:ok, value, rest}

      {:error, reason, pos} ->
        {:error, reason, pos}
    end
  end

  @doc """
  Decodes a Preserves value from text format, raising on error.

  Returns `{value, rest}` on success.

  ## Examples

      iex> Absynthe.Preserves.Decoder.Text.decode!("42")
      {{:integer, 42}, ""}
  """
  @spec decode!(String.t()) :: {Value.t(), String.t()}
  def decode!(input) when is_binary(input) do
    case decode(input) do
      {:ok, value, rest} -> {value, rest}
      {:error, reason, pos} -> raise "Parse error at position #{pos}: #{reason}"
    end
  end

  @doc """
  Decodes a Preserves value from text format, ensuring only whitespace/comments remain.

  Returns `{:ok, value}` if successful and only whitespace/comments remain.
  Returns `{:error, reason, position}` on failure or if non-whitespace content remains.

  ## Examples

      iex> Absynthe.Preserves.Decoder.Text.decode_all("42")
      {:ok, {:integer, 42}}

      iex> Absynthe.Preserves.Decoder.Text.decode_all("42  ; comment")
      {:ok, {:integer, 42}}
  """
  @spec decode_all(String.t()) :: {:ok, Value.t()} | {:error, String.t(), non_neg_integer()}
  def decode_all(input) when is_binary(input) do
    case parse_value(input, 0) do
      {:ok, value, rest, pos} ->
        case skip_whitespace_and_comments(rest, pos) do
          {"", _pos} -> {:ok, value}
          {remaining, pos} -> {:error, "Unexpected content after value: #{inspect(remaining)}", pos}
        end

      {:error, reason, pos} ->
        {:error, reason, pos}
    end
  end

  @doc """
  Decodes a Preserves value from text format, ensuring only whitespace/comments remain.

  Returns the parsed value on success, raises on error.

  ## Examples

      iex> Absynthe.Preserves.Decoder.Text.decode_all!("42")
      {:integer, 42}
  """
  @spec decode_all!(String.t()) :: Value.t()
  def decode_all!(input) when is_binary(input) do
    case decode_all(input) do
      {:ok, value} -> value
      {:error, reason, pos} -> raise "Parse error at position #{pos}: #{reason}"
    end
  end

  # Whitespace and comment handling

  defp skip_whitespace_and_comments(input, pos) do
    case skip_ws_and_comments_step(input, pos) do
      {^input, ^pos} -> {input, pos}
      {new_input, new_pos} -> skip_whitespace_and_comments(new_input, new_pos)
    end
  end

  defp skip_ws_and_comments_step(input, pos) do
    input
    |> skip_whitespace(pos)
    |> then(fn {input, pos} -> skip_line_comment(input, pos) end)
    |> then(fn {input, pos} -> skip_value_comment(input, pos) end)
  end

  defp skip_whitespace(input, pos) do
    case input do
      <<char::utf8, rest::binary>> when char in [?\s, ?\t, ?\n, ?\r, ?,] ->
        skip_whitespace(rest, pos + 1)

      _ ->
        {input, pos}
    end
  end

  defp skip_line_comment(<<";" <> rest::binary>>, pos) do
    case String.split(rest, "\n", parts: 2) do
      [comment, remainder] -> {remainder, pos + 1 + byte_size(comment) + 1}
      [comment] -> {"", pos + 1 + byte_size(comment)}
    end
  end

  defp skip_line_comment(input, pos), do: {input, pos}

  defp skip_value_comment(<<"#;" <> rest::binary>>, pos) do
    # Parse and discard the next value
    case parse_value(rest, pos + 2) do
      {:ok, _value, remaining, new_pos} -> {remaining, new_pos}
      {:error, _reason, _pos} -> {rest, pos + 2}
    end
  end

  defp skip_value_comment(input, pos), do: {input, pos}

  # Main value parser - dispatches based on first character

  defp parse_value(input, pos) do
    {input, pos} = skip_whitespace_and_comments(input, pos)

    case input do
      <<"#" <> _::binary>> ->
        parse_hash_prefix(input, pos)

      <<"<" <> _::binary>> ->
        parse_record(input, pos)

      <<"[" <> _::binary>> ->
        parse_sequence(input, pos)

      <<"{" <> _::binary>> ->
        parse_dictionary(input, pos)

      <<"\"" <> _::binary>> ->
        parse_string(input, pos)

      <<"|" <> _::binary>> ->
        parse_quoted_symbol(input, pos)

      <<char::utf8, _::binary>> when char in ?0..?9 or char == ?- ->
        parse_number(input, pos)

      <<char::utf8, _::binary>> ->
        # Check if it's a valid symbol start character
        if is_symbol_start(char) do
          parse_symbol(input, pos)
        else
          {:error, "Unexpected character: #{<<char::utf8>>}", pos}
        end

      "" ->
        {:error, "Unexpected end of input", pos}
    end
  end

  # Hash-prefixed values: #t, #f, #inf, #-inf, #nan, #!, #", #x", #[, #{

  defp parse_hash_prefix(input, pos) do
    case input do
      <<"#t" <> rest::binary>> ->
        {:ok, Value.boolean(true), rest, pos + 2}

      <<"#f" <> rest::binary>> ->
        {:ok, Value.boolean(false), rest, pos + 2}

      <<"#inf" <> rest::binary>> ->
        {:ok, Value.double(pos_infinity()), rest, pos + 4}

      <<"#-inf" <> rest::binary>> ->
        {:ok, Value.double(neg_infinity()), rest, pos + 5}

      <<"#nan" <> rest::binary>> ->
        {:ok, Value.double(nan()), rest, pos + 4}

      <<"#!" <> rest::binary>> ->
        parse_embedded(rest, pos + 2)

      <<"#\"" <> _::binary>> ->
        parse_bytestring(input, pos)

      <<"#x\"" <> _::binary>> ->
        parse_hex_bytestring(input, pos)

      <<"#[" <> _::binary>> ->
        parse_base64_bytestring(input, pos)

      <<?\#, ?\{, _::binary>> ->
        parse_set(input, pos)

      <<"#;" <> _::binary>> ->
        {:error, "Value comment #; cannot appear here (already handled)", pos}

      _ ->
        {:error, "Invalid # prefix: #{String.slice(input, 0..10)}", pos}
    end
  end

  # Numbers: integers and doubles

  defp parse_number(input, pos) do
    cond do
      String.starts_with?(input, "0x") or String.starts_with?(input, "-0x") ->
        parse_hex_integer(input, pos)

      String.starts_with?(input, "0o") or String.starts_with?(input, "-0o") ->
        parse_octal_integer(input, pos)

      String.starts_with?(input, "0b") or String.starts_with?(input, "-0b") ->
        parse_binary_integer(input, pos)

      true ->
        parse_decimal_number(input, pos)
    end
  end

  defp parse_hex_integer(input, pos) do
    {sign, rest} =
      case input do
        <<"-" <> r::binary>> -> {-1, r}
        r -> {1, r}
      end

    <<"0x" <> hex_rest::binary>> = rest
    {digits, remainder} = take_while(hex_rest, &is_hex_digit/1)

    if digits == "" do
      {:error, "Expected hex digits after 0x", pos}
    else
      case Integer.parse(digits, 16) do
        {value, ""} ->
          new_pos = pos + byte_size(input) - byte_size(remainder)
          {:ok, Value.integer(sign * value), remainder, new_pos}

        _ ->
          {:error, "Invalid hex integer", pos}
      end
    end
  end

  defp parse_octal_integer(input, pos) do
    {sign, rest} =
      case input do
        <<"-" <> r::binary>> -> {-1, r}
        r -> {1, r}
      end

    <<"0o" <> oct_rest::binary>> = rest
    {digits, remainder} = take_while(oct_rest, &is_octal_digit/1)

    if digits == "" do
      {:error, "Expected octal digits after 0o", pos}
    else
      case Integer.parse(digits, 8) do
        {value, ""} ->
          new_pos = pos + byte_size(input) - byte_size(remainder)
          {:ok, Value.integer(sign * value), remainder, new_pos}

        _ ->
          {:error, "Invalid octal integer", pos}
      end
    end
  end

  defp parse_binary_integer(input, pos) do
    {sign, rest} =
      case input do
        <<"-" <> r::binary>> -> {-1, r}
        r -> {1, r}
      end

    <<"0b" <> bin_rest::binary>> = rest
    {digits, remainder} = take_while(bin_rest, &is_binary_digit/1)

    if digits == "" do
      {:error, "Expected binary digits after 0b", pos}
    else
      case Integer.parse(digits, 2) do
        {value, ""} ->
          new_pos = pos + byte_size(input) - byte_size(remainder)
          {:ok, Value.integer(sign * value), remainder, new_pos}

        _ ->
          {:error, "Invalid binary integer", pos}
      end
    end
  end

  defp parse_decimal_number(input, pos) do
    {num_str, remainder} = take_while(input, &is_number_char/1)

    if num_str == "" or num_str == "-" do
      {:error, "Invalid number", pos}
    else
      new_pos = pos + byte_size(num_str)

      # Check if it's a float or integer
      if String.contains?(num_str, ".") or String.contains?(num_str, "e") or
           String.contains?(num_str, "E") do
        case Float.parse(num_str) do
          {value, ""} -> {:ok, Value.double(value), remainder, new_pos}
          _ -> {:error, "Invalid float: #{num_str}", pos}
        end
      else
        case Integer.parse(num_str) do
          {value, ""} -> {:ok, Value.integer(value), remainder, new_pos}
          _ -> {:error, "Invalid integer: #{num_str}", pos}
        end
      end
    end
  end

  defp is_number_char(char) when char in ?0..?9, do: true
  defp is_number_char(?-), do: true
  defp is_number_char(?+), do: true
  defp is_number_char(?.), do: true
  defp is_number_char(?e), do: true
  defp is_number_char(?E), do: true
  defp is_number_char(_), do: false

  defp is_hex_digit(char) when char in ?0..?9, do: true
  defp is_hex_digit(char) when char in ?a..?f, do: true
  defp is_hex_digit(char) when char in ?A..?F, do: true
  defp is_hex_digit(_), do: false

  defp is_octal_digit(char) when char in ?0..?7, do: true
  defp is_octal_digit(_), do: false

  defp is_binary_digit(char) when char in [?0, ?1], do: true
  defp is_binary_digit(_), do: false

  # Strings

  defp parse_string(<<"\"" <> rest::binary>>, pos) do
    parse_string_content(rest, "", pos + 1, pos + 1)
  end

  defp parse_string_content(input, acc, start_pos, current_pos) do
    case input do
      <<"\"" <> rest::binary>> ->
        {:ok, Value.string(acc), rest, current_pos + 1}

      <<"\\" <> rest::binary>> ->
        case parse_escape(rest, current_pos + 1) do
          {:ok, char, remainder, new_pos} ->
            parse_string_content(remainder, acc <> char, start_pos, new_pos)

          {:error, reason, pos} ->
            {:error, reason, pos}
        end

      <<char::utf8, rest::binary>> ->
        parse_string_content(rest, acc <> <<char::utf8>>, start_pos, current_pos + 1)

      "" ->
        {:error, "Unterminated string", start_pos - 1}
    end
  end

  defp parse_escape(input, pos) do
    case input do
      <<"n" <> rest::binary>> -> {:ok, "\n", rest, pos + 1}
      <<"r" <> rest::binary>> -> {:ok, "\r", rest, pos + 1}
      <<"t" <> rest::binary>> -> {:ok, "\t", rest, pos + 1}
      <<"\\" <> rest::binary>> -> {:ok, "\\", rest, pos + 1}
      <<"\"" <> rest::binary>> -> {:ok, "\"", rest, pos + 1}
      <<"/" <> rest::binary>> -> {:ok, "/", rest, pos + 1}
      <<"b" <> rest::binary>> -> {:ok, "\b", rest, pos + 1}
      <<"f" <> rest::binary>> -> {:ok, "\f", rest, pos + 1}
      <<"u", hex::binary-size(4), rest::binary>> ->
        case Integer.parse(hex, 16) do
          {codepoint, ""} -> {:ok, <<codepoint::utf8>>, rest, pos + 5}
          _ -> {:error, "Invalid unicode escape \\u#{hex}", pos - 1}
        end

      _ ->
        {:error, "Invalid escape sequence", pos - 1}
    end
  end

  # ByteStrings

  defp parse_bytestring(<<"#\"" <> rest::binary>>, pos) do
    parse_bytestring_content(rest, <<>>, pos + 2, pos + 2)
  end

  defp parse_bytestring_content(input, acc, start_pos, current_pos) do
    case input do
      <<"\"" <> rest::binary>> ->
        {:ok, Value.binary(acc), rest, current_pos + 1}

      <<"\\" <> rest::binary>> ->
        case parse_escape(rest, current_pos + 1) do
          {:ok, char, remainder, new_pos} ->
            parse_bytestring_content(remainder, acc <> char, start_pos, new_pos)

          {:error, reason, pos} ->
            {:error, reason, pos}
        end

      <<byte::8, rest::binary>> ->
        parse_bytestring_content(rest, acc <> <<byte>>, start_pos, current_pos + 1)

      "" ->
        {:error, "Unterminated bytestring", start_pos - 2}
    end
  end

  defp parse_hex_bytestring(<<"#x\"" <> rest::binary>>, pos) do
    case String.split(rest, "\"", parts: 2) do
      [hex_content, remainder] ->
        hex_clean = hex_content |> String.replace(~r/\s/, "")

        if rem(byte_size(hex_clean), 2) != 0 do
          {:error, "Hex bytestring must have even number of hex digits", pos}
        else
          case Base.decode16(hex_clean, case: :mixed) do
            {:ok, bytes} ->
              new_pos = pos + 3 + byte_size(hex_content) + 1
              {:ok, Value.binary(bytes), remainder, new_pos}

            :error ->
              {:error, "Invalid hex bytestring", pos}
          end
        end

      _ ->
        {:error, "Unterminated hex bytestring", pos}
    end
  end

  defp parse_base64_bytestring(<<"#[" <> rest::binary>>, pos) do
    case String.split(rest, "]", parts: 2) do
      [b64_content, remainder] ->
        b64_clean = b64_content |> String.replace(~r/\s/, "")

        case Base.decode64(b64_clean) do
          {:ok, bytes} ->
            new_pos = pos + 2 + byte_size(b64_content) + 1
            {:ok, Value.binary(bytes), remainder, new_pos}

          :error ->
            {:error, "Invalid base64 bytestring", pos}
        end

      _ ->
        {:error, "Unterminated base64 bytestring", pos}
    end
  end

  # Symbols

  defp parse_symbol(input, pos) do
    {sym_str, remainder} = take_while(input, &is_symbol_char/1)

    if sym_str == "" do
      {:error, "Expected symbol", pos}
    else
      new_pos = pos + byte_size(sym_str)
      {:ok, Value.symbol(sym_str), remainder, new_pos}
    end
  end

  defp parse_quoted_symbol(<<"|" <> rest::binary>>, pos) do
    parse_quoted_symbol_content(rest, "", pos + 1, pos + 1)
  end

  defp parse_quoted_symbol_content(input, acc, start_pos, current_pos) do
    case input do
      <<"|" <> rest::binary>> ->
        {:ok, Value.symbol(acc), rest, current_pos + 1}

      <<"\\" <> rest::binary>> ->
        case parse_escape(rest, current_pos + 1) do
          {:ok, char, remainder, new_pos} ->
            parse_quoted_symbol_content(remainder, acc <> char, start_pos, new_pos)

          {:error, reason, pos} ->
            {:error, reason, pos}
        end

      <<char::utf8, rest::binary>> ->
        parse_quoted_symbol_content(rest, acc <> <<char::utf8>>, start_pos, current_pos + 1)

      "" ->
        {:error, "Unterminated quoted symbol", start_pos - 1}
    end
  end

  # Note: < and > are excluded from bare symbols since they're record delimiters
  # Use |...| quoted symbol syntax for symbols containing these characters
  defp is_symbol_start(char) when char in ?a..?z, do: true
  defp is_symbol_start(char) when char in ?A..?Z, do: true
  defp is_symbol_start(?_), do: true
  defp is_symbol_start(?+), do: true
  defp is_symbol_start(?-), do: true
  defp is_symbol_start(?*), do: true
  defp is_symbol_start(?/), do: true
  defp is_symbol_start(?!), do: true
  defp is_symbol_start(??), do: true
  defp is_symbol_start(?$), do: true
  defp is_symbol_start(?%), do: true
  defp is_symbol_start(?&), do: true
  defp is_symbol_start(?=), do: true
  defp is_symbol_start(?~), do: true
  defp is_symbol_start(_), do: false

  defp is_symbol_char(char) when char in ?0..?9, do: true
  defp is_symbol_char(?.), do: true
  defp is_symbol_char(char), do: is_symbol_start(char)

  # Records: <label field1 field2 ...>

  defp parse_record(<<"<" <> rest::binary>>, pos) do
    {rest, pos} = skip_whitespace_and_comments(rest, pos + 1)

    # Parse label
    case parse_value(rest, pos) do
      {:ok, label, rest, pos} ->
        parse_record_fields(rest, pos, label, [])

      {:error, reason, pos} ->
        {:error, "Invalid record label: #{reason}", pos}
    end
  end

  defp parse_record_fields(input, pos, label, fields) do
    {input, pos} = skip_whitespace_and_comments(input, pos)

    case input do
      <<">" <> rest::binary>> ->
        {:ok, Value.record(label, Enum.reverse(fields)), rest, pos + 1}

      "" ->
        {:error, "Unterminated record", pos}

      _ ->
        case parse_value(input, pos) do
          {:ok, field, rest, new_pos} ->
            parse_record_fields(rest, new_pos, label, [field | fields])

          {:error, reason, pos} ->
            {:error, "Invalid record field: #{reason}", pos}
        end
    end
  end

  # Sequences: [...]

  defp parse_sequence(<<"[" <> rest::binary>>, pos) do
    parse_sequence_items(rest, pos + 1, [])
  end

  defp parse_sequence_items(input, pos, items) do
    {input, pos} = skip_whitespace_and_comments(input, pos)

    case input do
      <<"]" <> rest::binary>> ->
        {:ok, Value.sequence(Enum.reverse(items)), rest, pos + 1}

      "" ->
        {:error, "Unterminated sequence", pos}

      _ ->
        case parse_value(input, pos) do
          {:ok, item, rest, new_pos} ->
            parse_sequence_items(rest, new_pos, [item | items])

          {:error, reason, pos} ->
            {:error, "Invalid sequence item: #{reason}", pos}
        end
    end
  end

  # Sets: #{...}

  defp parse_set(<<?\#, ?\{, rest::binary>>, pos) do
    parse_set_items(rest, pos + 2, [])
  end

  defp parse_set_items(input, pos, items) do
    {input, pos} = skip_whitespace_and_comments(input, pos)

    case input do
      <<"}" <> rest::binary>> ->
        {:ok, Value.set(items), rest, pos + 1}

      "" ->
        {:error, "Unterminated set", pos}

      _ ->
        case parse_value(input, pos) do
          {:ok, item, rest, new_pos} ->
            parse_set_items(rest, new_pos, [item | items])

          {:error, reason, pos} ->
            {:error, "Invalid set item: #{reason}", pos}
        end
    end
  end

  # Dictionaries: {...}

  defp parse_dictionary(<<"{" <> rest::binary>>, pos) do
    parse_dict_entries(rest, pos + 1, [])
  end

  defp parse_dict_entries(input, pos, entries) do
    {input, pos} = skip_whitespace_and_comments(input, pos)

    case input do
      <<"}" <> rest::binary>> ->
        {:ok, Value.dictionary(Enum.reverse(entries)), rest, pos + 1}

      "" ->
        {:error, "Unterminated dictionary", pos}

      _ ->
        case parse_dict_entry(input, pos) do
          {:ok, entry, rest, new_pos} ->
            parse_dict_entries(rest, new_pos, [entry | entries])

          {:error, reason, pos} ->
            {:error, "Invalid dictionary entry: #{reason}", pos}
        end
    end
  end

  defp parse_dict_entry(input, pos) do
    # Try to parse symbol shorthand first (e.g., name: "value" where name is a symbol key)
    case try_parse_symbol_key(input, pos) do
      {:ok, key, rest, pos} ->
        # Found symbol: shorthand, now parse the value
        {rest, pos} = skip_whitespace_and_comments(rest, pos)

        case parse_value(rest, pos) do
          {:ok, value, rest, pos} ->
            {:ok, {key, value}, rest, pos}

          {:error, reason, pos} ->
            {:error, "Invalid dictionary value: #{reason}", pos}
        end

      :not_symbol_key ->
        # Parse key as a normal value
        case parse_value(input, pos) do
          {:ok, key, rest, pos} ->
            {rest, pos} = skip_whitespace_and_comments(rest, pos)

            # Expect colon
            case rest do
              <<":" <> rest::binary>> ->
                {rest, pos} = skip_whitespace_and_comments(rest, pos + 1)

                # Parse value
                case parse_value(rest, pos) do
                  {:ok, value, rest, pos} ->
                    {:ok, {key, value}, rest, pos}

                  {:error, reason, pos} ->
                    {:error, "Invalid dictionary value: #{reason}", pos}
                end

              _ ->
                {:error, "Expected ':' after dictionary key", pos}
            end

          {:error, reason, pos} ->
            {:error, "Invalid dictionary key: #{reason}", pos}
        end
    end
  end

  # Try to parse a symbol followed immediately by a colon (shorthand syntax)
  # Returns {:ok, symbol_value, rest_after_colon, pos} or :not_symbol_key
  defp try_parse_symbol_key(input, pos) do
    # Look for pattern: symbol_chars followed by colon
    case input do
      <<char::utf8, _::binary>> when char in ?a..?z or char in ?A..?Z or char == ?_ ->
        # Collect symbol characters (but NOT colon for this purpose)
        {sym_str, rest} = take_symbol_key_chars(input, "")

        case rest do
          <<":" <> rest::binary>> when sym_str != "" ->
            {:ok, Value.symbol(sym_str), rest, pos + byte_size(sym_str) + 1}

          _ ->
            :not_symbol_key
        end

      _ ->
        :not_symbol_key
    end
  end

  # Take symbol characters except colon (for key shorthand parsing)
  defp take_symbol_key_chars(<<char::utf8, rest::binary>>, acc)
       when char in ?a..?z or char in ?A..?Z or char in ?0..?9 or char == ?_ or char == ?- do
    take_symbol_key_chars(rest, acc <> <<char::utf8>>)
  end

  defp take_symbol_key_chars(input, acc), do: {acc, input}

  # Embedded values: #!value

  defp parse_embedded(input, pos) do
    case parse_value(input, pos) do
      {:ok, value, rest, new_pos} ->
        {:ok, Value.embedded(value), rest, new_pos}

      {:error, reason, pos} ->
        {:error, "Invalid embedded value: #{reason}", pos}
    end
  end

  # Helper functions

  defp take_while(input, predicate) do
    take_while_acc(input, predicate, "")
  end

  defp take_while_acc(<<char::utf8, rest::binary>>, predicate, acc) do
    if predicate.(char) do
      take_while_acc(rest, predicate, acc <> <<char::utf8>>)
    else
      {acc, <<char::utf8, rest::binary>>}
    end
  end

  defp take_while_acc("", _predicate, acc), do: {acc, ""}

  # Special float values
  # Note: BEAM cannot represent IEEE754 infinity/NaN as native floats,
  # so we use atoms as per Preserves convention for Erlang/Elixir
  defp pos_infinity, do: :infinity
  defp neg_infinity, do: :neg_infinity
  defp nan, do: :nan
end
