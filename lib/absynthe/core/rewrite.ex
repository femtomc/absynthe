defmodule Absynthe.Core.Rewrite do
  @moduledoc """
  Value rewriting for capability attenuation in the Syndicated Actor Model.

  This module implements the core logic for applying caveats to values. When
  a value (assertion or message) is sent through an attenuated Ref, the
  attenuation chain is applied to determine:

  1. Whether the value is allowed (passes all caveats)
  2. What transformed value should be delivered

  ## Rewriting Process

  Given a value and an attenuation chain `[c1, c2, c3]`:

  1. Apply c1 to the value
     - If rejected: overall result is `:rejected`
     - If accepted: get transformed value v1
  2. Apply c2 to v1
     - If rejected: overall result is `:rejected`
     - If accepted: get transformed value v2
  3. Apply c3 to v2
     - If rejected: overall result is `:rejected`
     - If accepted: get transformed value v3
  4. Return `{:ok, v3}` as the final result

  ## Caveat Application

  Each caveat type is applied differently:

  - **Rewrite**: Pattern-match the value. If match succeeds, use captures to
    instantiate the template. If match fails, reject.

  - **Alts**: Try each alternative in order. First one that accepts wins.
    If none accept, reject.

  - **Reject**: Always reject.

  ## Examples

      # Simple passthrough
      iex> caveat = Caveat.passthrough()
      iex> Rewrite.apply_caveat({:integer, 42}, caveat)
      {:ok, {:integer, 42}}

      # Reject all
      iex> Rewrite.apply_caveat({:integer, 42}, :reject)
      :rejected

      # Rewrite that filters to specific pattern
      iex> pattern = {:record, {{:symbol, "Person"}, [{:symbol, "$"}]}}
      iex> template = {:ref, 0}  # Extract just the name
      iex> caveat = Caveat.rewrite(pattern, template)
      iex> Rewrite.apply_caveat(
      ...>   {:record, {{:symbol, "Person"}, [{:string, "Alice"}]}},
      ...>   caveat
      ...> )
      {:ok, {:string, "Alice"}}

  ## Design Notes

  This implementation follows syndicate-rs's rewrite.rs logic. The key insight
  is that attenuation provides both access control (filtering) and information
  hiding (transformation) in a unified mechanism.
  """

  alias Absynthe.Core.Caveat
  alias Absynthe.Dataspace.Pattern

  @doc """
  Applies an attenuation chain to a value.

  The chain is processed in order. Each caveat receives the output of the
  previous one. If any caveat rejects, the overall result is rejection.

  ## Parameters

  - `value` - The Preserves value to rewrite
  - `attenuation` - List of caveats to apply in sequence

  ## Returns

  - `{:ok, rewritten_value}` - Value passed all caveats
  - `:rejected` - Value was rejected by some caveat

  ## Examples

      # Empty attenuation = full authority (passthrough)
      iex> Rewrite.apply_attenuation({:integer, 42}, [])
      {:ok, {:integer, 42}}

      # Single passthrough caveat
      iex> Rewrite.apply_attenuation({:integer, 42}, [Caveat.passthrough()])
      {:ok, {:integer, 42}}

      # Reject caveat
      iex> Rewrite.apply_attenuation({:integer, 42}, [:reject])
      :rejected
  """
  @spec apply_attenuation(Absynthe.Preserves.Value.t(), Caveat.attenuation()) ::
          {:ok, Absynthe.Preserves.Value.t()} | :rejected
  def apply_attenuation(value, []) do
    # Empty attenuation = no restrictions
    {:ok, value}
  end

  def apply_attenuation(value, attenuation) when is_list(attenuation) do
    Enum.reduce_while(attenuation, {:ok, value}, fn caveat, {:ok, current_value} ->
      case apply_caveat(current_value, caveat) do
        {:ok, new_value} -> {:cont, {:ok, new_value}}
        :rejected -> {:halt, :rejected}
      end
    end)
  end

  def apply_attenuation(value, nil) do
    # nil attenuation = no restrictions (same as empty list)
    # This handles the common case where Ref.attenuation is nil
    {:ok, value}
  end

  @doc """
  Applies a single caveat to a value.

  ## Parameters

  - `value` - The Preserves value to rewrite
  - `caveat` - The caveat to apply

  ## Returns

  - `{:ok, rewritten_value}` - Caveat accepted the value
  - `:rejected` - Caveat rejected the value

  ## Examples

      iex> Rewrite.apply_caveat({:integer, 42}, :reject)
      :rejected

      iex> Rewrite.apply_caveat({:integer, 42}, Caveat.passthrough())
      {:ok, {:integer, 42}}
  """
  @spec apply_caveat(Absynthe.Preserves.Value.t(), Caveat.t()) ::
          {:ok, Absynthe.Preserves.Value.t()} | :rejected
  def apply_caveat(_value, :reject) do
    :rejected
  end

  def apply_caveat(value, {:alts, alternatives}) do
    # Try each alternative in order, first match wins
    Enum.reduce_while(alternatives, :rejected, fn alt, :rejected ->
      case apply_caveat(value, alt) do
        {:ok, _} = success -> {:halt, success}
        :rejected -> {:cont, :rejected}
      end
    end)
  end

  def apply_caveat(value, {:rewrite, pattern, template}) do
    # Compile the pattern and try to match
    compiled_pattern = Pattern.compile(pattern)

    case Pattern.match(compiled_pattern, value) do
      {:ok, captures} ->
        # Pattern matched - apply template with captures
        case Caveat.apply_template(template, captures) do
          {:ok, rewritten} -> {:ok, rewritten}
          {:error, _reason} -> :rejected
        end

      :no_match ->
        :rejected
    end
  end

  @doc """
  Checks if a value would be accepted by an attenuation chain without
  actually computing the rewritten value.

  This is useful for quick rejection checks before doing more expensive
  operations.

  ## Parameters

  - `value` - The Preserves value to check
  - `attenuation` - The attenuation chain to check against

  ## Returns

  - `true` - Value would be accepted
  - `false` - Value would be rejected

  ## Examples

      iex> Rewrite.accepts?({:integer, 42}, [])
      true

      iex> Rewrite.accepts?({:integer, 42}, [:reject])
      false
  """
  @spec accepts?(Absynthe.Preserves.Value.t(), Caveat.attenuation()) :: boolean()
  def accepts?(value, attenuation) do
    case apply_attenuation(value, attenuation) do
      {:ok, _} -> true
      :rejected -> false
    end
  end

  @doc """
  Composes two attenuation chains into one.

  When you attenuate an already-attenuated Ref, the new caveat is added to
  the front of the chain (applied first).

  ## Parameters

  - `outer` - The new (outer) attenuation to apply first
  - `inner` - The existing (inner) attenuation to apply second

  ## Returns

  The composed attenuation chain.

  ## Examples

      iex> outer = [Caveat.passthrough()]
      iex> inner = [Caveat.passthrough()]
      iex> Rewrite.compose(outer, inner)
      [Caveat.passthrough(), Caveat.passthrough()]
  """
  @spec compose(Caveat.attenuation(), Caveat.attenuation() | nil) :: Caveat.attenuation()
  def compose(outer, nil), do: outer
  def compose(outer, inner), do: outer ++ inner
end
