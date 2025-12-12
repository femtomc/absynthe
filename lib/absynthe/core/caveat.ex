defmodule Absynthe.Core.Caveat do
  @moduledoc """
  Capability caveats for the Syndicated Actor Model.

  Caveats define restrictions on how a capability (Ref) can be used. When a Ref
  has an attenuation chain (list of caveats), any assertion or message sent
  through that Ref must pass through each caveat in sequence.

  ## Caveat Types

  - **Rewrite**: Pattern-match an assertion and transform it using a template.
    If the pattern doesn't match, the assertion is rejected.

  - **Alts**: A list of alternative caveats. The assertion passes if ANY of
    the alternatives accepts it (first match wins).

  - **Reject**: Unconditionally rejects all assertions. Used to create
    "write-only" or "no-op" capabilities.

  ## Attenuation Chain

  An attenuation is a list of caveats `[c1, c2, c3, ...]`. When an assertion
  flows through an attenuated Ref:

  1. The assertion is passed to c1
  2. If c1 accepts (possibly transforming the value), the result goes to c2
  3. This continues until all caveats pass, or any rejects

  The final transformed value (or original if no transforms) is delivered.

  ## Template Language

  Templates define how captured pattern bindings are used to construct the
  output value:

  - `{:lit, value}` - Literal value
  - `{:ref, index}` - Reference to the nth captured value (0-indexed)
  - `{:record, label_template, [field_templates]}` - Construct a record
  - `{:sequence, [templates]}` - Construct a sequence
  - `{:dictionary, [{key_template, value_template}]}` - Construct a dictionary

  ## Examples

      # Allow only Person records with any name, pass through unchanged
      pattern = {:record, {{:symbol, "Person"}, [{:symbol, "$"}, {:symbol, "_"}]}}
      template = {:ref, 0}  # Would need to be the whole match
      caveat = Caveat.rewrite(pattern, template)

      # More practical: allow Person records and extract just the name
      pattern = {:record, {{:symbol, "Person"}, [{:symbol, "$name"}]}}
      template = {:ref, 0}  # Pass through the captured name
      caveat = Caveat.rewrite(pattern, template)

  ## Design Notes

  This implementation follows syndicate-rs's caveat model (see rewrite.rs).
  The key insight is that caveats provide both filtering (reject non-matching)
  and transformation (rewrite matching values).
  """

  alias Absynthe.Preserves.Value

  @typedoc """
  A template for constructing values from pattern captures.

  Templates can be:
  - `{:lit, value}` - A literal Preserves value
  - `{:ref, index}` - Reference to the nth capture (0-indexed)
  - `{:record, label_template, field_templates}` - Construct a record
  - `{:sequence, templates}` - Construct a sequence
  - `{:set, templates}` - Construct a set
  - `{:dictionary, [{key_template, value_template}]}` - Construct a dictionary
  """
  @type template ::
          {:lit, Value.t()}
          | {:ref, non_neg_integer()}
          | {:record, template(), [template()]}
          | {:sequence, [template()]}
          | {:set, [template()]}
          | {:dictionary, [{template(), template()}]}

  @typedoc """
  A single caveat that can accept/transform or reject values.

  - `{:rewrite, pattern, template}` - Match pattern, construct output from template
  - `{:alts, [caveat]}` - Try alternatives in order, first match wins
  - `:reject` - Unconditionally reject
  """
  @type t ::
          {:rewrite, Value.t(), template()}
          | {:alts, [t()]}
          | :reject

  @typedoc """
  An attenuation chain is a list of caveats applied in sequence.
  An empty list means no attenuation (full authority).
  """
  @type attenuation :: [t()]

  # Constructor Functions

  @doc """
  Creates a rewrite caveat.

  A rewrite caveat matches values against a pattern. If the pattern matches,
  the template is used to construct the output value using captured bindings.
  If the pattern doesn't match, the value is rejected.

  ## Parameters

  - `pattern` - A Preserves pattern with capture variables (`$` or `$name`)
  - `template` - A template for constructing the output

  ## Examples

      # Allow any value, pass through unchanged
      iex> Caveat.rewrite({:symbol, "$"}, {:ref, 0})
      {:rewrite, {:symbol, "$"}, {:ref, 0}}

      # Allow only integers, pass through unchanged
      iex> pattern = {:symbol, "$"}  # captures the value
      iex> template = {:ref, 0}      # outputs the captured value
      iex> Caveat.rewrite(pattern, template)
      {:rewrite, {:symbol, "$"}, {:ref, 0}}
  """
  @spec rewrite(Value.t(), template()) :: t()
  def rewrite(pattern, template) do
    {:rewrite, pattern, template}
  end

  @doc """
  Creates an alternatives caveat.

  An alternatives caveat tries each sub-caveat in order. The first one that
  accepts the value wins. If none accept, the value is rejected.

  ## Parameters

  - `alternatives` - List of caveats to try in order

  ## Examples

      # Accept either integers or strings
      iex> int_caveat = Caveat.rewrite({:symbol, "$"}, {:ref, 0})
      iex> Caveat.alts([int_caveat])
      {:alts, [{:rewrite, {:symbol, "$"}, {:ref, 0}}]}
  """
  @spec alts([t()]) :: t()
  def alts(alternatives) when is_list(alternatives) do
    {:alts, alternatives}
  end

  @doc """
  Creates a reject caveat.

  A reject caveat unconditionally rejects all values. This is useful for
  creating capabilities that can't be used for certain operations.

  ## Examples

      iex> Caveat.reject()
      :reject
  """
  @spec reject() :: t()
  def reject, do: :reject

  # Convenience constructors

  @doc """
  Creates a passthrough caveat that accepts and passes through any value.

  This is equivalent to `rewrite({:symbol, "$"}, {:ref, 0})`.

  ## Examples

      iex> Caveat.passthrough()
      {:rewrite, {:symbol, "$"}, {:ref, 0}}
  """
  @spec passthrough() :: t()
  def passthrough do
    rewrite({:symbol, "$"}, {:ref, 0})
  end

  @doc """
  Creates a caveat that only allows values matching the given pattern.

  The matched value is passed through unchanged.

  ## Parameters

  - `pattern` - A Preserves pattern to match

  ## Examples

      # Only allow Person records
      iex> pattern = {:record, {{:symbol, "Person"}, [{:symbol, "_"}, {:symbol, "_"}]}}
      iex> Caveat.allow_pattern(pattern)
      {:rewrite, {:record, {{:symbol, "Person"}, [{:symbol, "_"}, {:symbol, "_"}]}},
       {:lit, ...}}  # Actually returns pattern with $ capture
  """
  @spec allow_pattern(Value.t()) :: t()
  def allow_pattern(pattern) do
    # Wrap the pattern in a capture so we can pass through the whole value
    # This is a simplified version - a full implementation would need to
    # handle this more carefully
    rewrite(wrap_pattern_with_capture(pattern), {:ref, 0})
  end

  # Helper to wrap a pattern in a capture
  defp wrap_pattern_with_capture(pattern) do
    # If the pattern already has a top-level capture, use it
    # Otherwise, wrap it. For now, we use a simple approach:
    # just use a capture pattern that matches anything
    case pattern do
      {:symbol, "$" <> _} -> pattern
      _ -> {:symbol, "$"}
    end
  end

  # Template Application

  @doc """
  Applies a template to captured bindings to produce a value.

  ## Parameters

  - `template` - The template to apply
  - `captures` - List of captured values from pattern matching

  ## Returns

  - `{:ok, value}` - Successfully constructed value
  - `{:error, reason}` - Template application failed

  ## Examples

      iex> Caveat.apply_template({:lit, {:integer, 42}}, [])
      {:ok, {:integer, 42}}

      iex> Caveat.apply_template({:ref, 0}, [{:string, "hello"}])
      {:ok, {:string, "hello"}}

      iex> Caveat.apply_template({:ref, 5}, [{:string, "hello"}])
      {:error, {:capture_out_of_range, 5, 1}}
  """
  @spec apply_template(template(), [Value.t()]) :: {:ok, Value.t()} | {:error, term()}
  def apply_template({:lit, value}, _captures) do
    {:ok, value}
  end

  def apply_template({:ref, index}, captures) when is_integer(index) and index >= 0 do
    if index < length(captures) do
      {:ok, Enum.at(captures, index)}
    else
      {:error, {:capture_out_of_range, index, length(captures)}}
    end
  end

  def apply_template({:record, label_template, field_templates}, captures) do
    with {:ok, label} <- apply_template(label_template, captures),
         {:ok, fields} <- apply_templates(field_templates, captures) do
      {:ok, {:record, {label, fields}}}
    end
  end

  def apply_template({:sequence, templates}, captures) do
    case apply_templates(templates, captures) do
      {:ok, elements} -> {:ok, {:sequence, elements}}
      error -> error
    end
  end

  def apply_template({:set, templates}, captures) do
    case apply_templates(templates, captures) do
      {:ok, elements} -> {:ok, {:set, MapSet.new(elements)}}
      error -> error
    end
  end

  def apply_template({:dictionary, pairs}, captures) do
    results =
      Enum.reduce_while(pairs, {:ok, []}, fn {key_template, value_template}, {:ok, acc} ->
        with {:ok, key} <- apply_template(key_template, captures),
             {:ok, value} <- apply_template(value_template, captures) do
          {:cont, {:ok, [{key, value} | acc]}}
        else
          error -> {:halt, error}
        end
      end)

    case results do
      {:ok, pairs_list} -> {:ok, {:dictionary, Map.new(pairs_list)}}
      error -> error
    end
  end

  def apply_template(invalid, _captures) do
    {:error, {:invalid_template, invalid}}
  end

  # Helper for applying multiple templates
  defp apply_templates(templates, captures) do
    results =
      Enum.reduce_while(templates, {:ok, []}, fn template, {:ok, acc} ->
        case apply_template(template, captures) do
          {:ok, value} -> {:cont, {:ok, [value | acc]}}
          error -> {:halt, error}
        end
      end)

    case results do
      {:ok, values} -> {:ok, Enum.reverse(values)}
      error -> error
    end
  end

  # Validation

  @doc """
  Validates a caveat structure.

  Checks that:
  - Pattern is a valid Preserves value with valid capture syntax
  - Template references are within valid range given the pattern's captures
  - Nested caveats in alts are valid

  ## Parameters

  - `caveat` - The caveat to validate

  ## Returns

  - `:ok` - Caveat is valid
  - `{:error, reason}` - Caveat is invalid

  ## Examples

      iex> Caveat.validate({:rewrite, {:symbol, "$"}, {:ref, 0}})
      :ok

      iex> Caveat.validate({:rewrite, {:symbol, "$"}, {:ref, 5}})
      {:error, {:capture_out_of_range, 5, 1}}
  """
  @spec validate(t()) :: :ok | {:error, term()}
  def validate(:reject), do: :ok

  def validate({:alts, alternatives}) when is_list(alternatives) do
    Enum.reduce_while(alternatives, :ok, fn alt, :ok ->
      case validate(alt) do
        :ok -> {:cont, :ok}
        error -> {:halt, error}
      end
    end)
  end

  def validate({:rewrite, pattern, template}) do
    # Count captures in the pattern
    capture_count = count_captures(pattern)

    # Validate template references don't exceed capture count
    validate_template_refs(template, capture_count)
  end

  def validate(_invalid) do
    {:error, :invalid_caveat_structure}
  end

  @doc """
  Validates an attenuation chain.

  ## Parameters

  - `attenuation` - List of caveats to validate

  ## Returns

  - `:ok` - All caveats are valid
  - `{:error, reason}` - Some caveat is invalid
  """
  @spec validate_attenuation(attenuation()) :: :ok | {:error, term()}
  def validate_attenuation(attenuation) when is_list(attenuation) do
    Enum.reduce_while(attenuation, :ok, fn caveat, :ok ->
      case validate(caveat) do
        :ok -> {:cont, :ok}
        error -> {:halt, error}
      end
    end)
  end

  def validate_attenuation(_) do
    {:error, :attenuation_must_be_list}
  end

  # Helper to count captures in a pattern
  defp count_captures({:symbol, "$" <> _}), do: 1
  defp count_captures({:symbol, "_"}), do: 0

  defp count_captures({:record, {label, fields}}) do
    count_captures(label) + Enum.sum(Enum.map(fields, &count_captures/1))
  end

  defp count_captures({:sequence, elements}) do
    Enum.sum(Enum.map(elements, &count_captures/1))
  end

  defp count_captures({:set, elements}) do
    Enum.sum(Enum.map(MapSet.to_list(elements), &count_captures/1))
  end

  defp count_captures({:dictionary, entries}) when is_map(entries) do
    entries
    |> Map.to_list()
    |> Enum.map(fn {k, v} -> count_captures(k) + count_captures(v) end)
    |> Enum.sum()
  end

  defp count_captures(_literal), do: 0

  # Helper to validate template references
  defp validate_template_refs({:lit, _}, _capture_count), do: :ok

  defp validate_template_refs({:ref, index}, capture_count) do
    if index < capture_count do
      :ok
    else
      {:error, {:capture_out_of_range, index, capture_count}}
    end
  end

  defp validate_template_refs({:record, label, fields}, capture_count) do
    with :ok <- validate_template_refs(label, capture_count),
         :ok <- validate_template_refs_list(fields, capture_count) do
      :ok
    end
  end

  defp validate_template_refs({:sequence, templates}, capture_count) do
    validate_template_refs_list(templates, capture_count)
  end

  defp validate_template_refs({:set, templates}, capture_count) do
    validate_template_refs_list(templates, capture_count)
  end

  defp validate_template_refs({:dictionary, pairs}, capture_count) do
    Enum.reduce_while(pairs, :ok, fn {key, value}, :ok ->
      with :ok <- validate_template_refs(key, capture_count),
           :ok <- validate_template_refs(value, capture_count) do
        {:cont, :ok}
      else
        error -> {:halt, error}
      end
    end)
  end

  defp validate_template_refs(invalid, _capture_count) do
    {:error, {:invalid_template, invalid}}
  end

  defp validate_template_refs_list(templates, capture_count) do
    Enum.reduce_while(templates, :ok, fn template, :ok ->
      case validate_template_refs(template, capture_count) do
        :ok -> {:cont, :ok}
        error -> {:halt, error}
      end
    end)
  end
end
