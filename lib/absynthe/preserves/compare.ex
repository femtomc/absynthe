defmodule Absynthe.Preserves.Compare do
  @moduledoc """
  Total ordering for Preserves values.

  This module implements the canonical total ordering for Preserves values as specified
  in the Preserves data model. The ordering is designed to be stable, deterministic,
  and consistent across implementations.

  ## Ordering Hierarchy

  The top-level ordering is: **Atom < Compound < Embedded**

  ### Atoms (in order)
  1. Boolean (false < true)
  2. Double (IEEE 754 total order: -∞ < negative < -0 < +0 < positive < +∞ < NaN)
  3. SignedInteger (numeric order)
  4. String (lexicographic by UTF-8 bytes)
  5. ByteString (lexicographic)
  6. Symbol (lexicographic by UTF-8 bytes)

  ### Compounds (in order)
  1. Record (compare by: arity, then label, then fields left-to-right)
  2. Sequence (lexicographic by elements)
  3. Set (lexicographic by sorted elements)
  4. Dictionary (lexicographic by sorted key-value pairs, keys first)

  ### Embedded
  Embedded values are compared using Elixir's default term ordering.

  ## Special Float Cases

  - Negative zero (-0.0) is less than positive zero (+0.0)
  - NaN is greater than all other doubles (including infinity)
  - NaN equals itself for ordering purposes (compare(NaN, NaN) == :eq)

  ## Examples

      iex> alias Absynthe.Preserves.{Value, Compare}
      iex> Compare.compare(Value.boolean(false), Value.boolean(true))
      :lt

      iex> alias Absynthe.Preserves.{Value, Compare}
      iex> Compare.compare(Value.integer(42), Value.double(3.14))
      :gt

      iex> alias Absynthe.Preserves.{Value, Compare}
      iex> Compare.lt?(Value.string("apple"), Value.string("banana"))
      true

      iex> alias Absynthe.Preserves.{Value, Compare}
      iex> a = Value.sequence([Value.integer(1), Value.integer(2)])
      iex> b = Value.sequence([Value.integer(1), Value.integer(3)])
      iex> Compare.compare(a, b)
      :lt
  """

  alias Absynthe.Preserves.Value

  @typedoc """
  Comparison result: less than, equal, or greater than.
  """
  @type comparison_result :: :lt | :eq | :gt

  # Main comparison function

  @doc """
  Compares two Preserves values according to the total ordering.

  Returns:
  - `:lt` if `a` is less than `b`
  - `:eq` if `a` equals `b`
  - `:gt` if `a` is greater than `b`

  ## Examples

      iex> alias Absynthe.Preserves.{Value, Compare}
      iex> Compare.compare(Value.integer(1), Value.integer(2))
      :lt

      iex> alias Absynthe.Preserves.{Value, Compare}
      iex> Compare.compare(Value.string("hello"), Value.string("hello"))
      :eq

      iex> alias Absynthe.Preserves.{Value, Compare}
      iex> Compare.compare(Value.boolean(true), Value.integer(42))
      :lt
  """
  @spec compare(Value.t(), Value.t()) :: comparison_result()
  def compare(a, a), do: :eq

  # Top-level ordering: Atom < Compound < Embedded
  def compare(a, b) do
    case {value_category(a), value_category(b)} do
      {:atom, :atom} -> compare_atoms(a, b)
      {:atom, :compound} -> :lt
      {:atom, :embedded} -> :lt
      {:compound, :atom} -> :gt
      {:compound, :compound} -> compare_compounds(a, b)
      {:compound, :embedded} -> :lt
      {:embedded, :atom} -> :gt
      {:embedded, :compound} -> :gt
      {:embedded, :embedded} -> compare_embedded(a, b)
    end
  end

  # Boolean comparison helpers

  @doc """
  Returns true if `a` is less than `b`.

  ## Examples

      iex> alias Absynthe.Preserves.{Value, Compare}
      iex> Compare.lt?(Value.integer(1), Value.integer(2))
      true

      iex> alias Absynthe.Preserves.{Value, Compare}
      iex> Compare.lt?(Value.integer(2), Value.integer(1))
      false
  """
  @spec lt?(Value.t(), Value.t()) :: boolean()
  def lt?(a, b), do: compare(a, b) == :lt

  @doc """
  Returns true if `a` is less than or equal to `b`.

  ## Examples

      iex> alias Absynthe.Preserves.{Value, Compare}
      iex> Compare.lte?(Value.integer(1), Value.integer(1))
      true

      iex> alias Absynthe.Preserves.{Value, Compare}
      iex> Compare.lte?(Value.integer(2), Value.integer(1))
      false
  """
  @spec lte?(Value.t(), Value.t()) :: boolean()
  def lte?(a, b), do: compare(a, b) in [:lt, :eq]

  @doc """
  Returns true if `a` is greater than `b`.

  ## Examples

      iex> alias Absynthe.Preserves.{Value, Compare}
      iex> Compare.gt?(Value.integer(2), Value.integer(1))
      true

      iex> alias Absynthe.Preserves.{Value, Compare}
      iex> Compare.gt?(Value.integer(1), Value.integer(2))
      false
  """
  @spec gt?(Value.t(), Value.t()) :: boolean()
  def gt?(a, b), do: compare(a, b) == :gt

  @doc """
  Returns true if `a` is greater than or equal to `b`.

  ## Examples

      iex> alias Absynthe.Preserves.{Value, Compare}
      iex> Compare.gte?(Value.integer(2), Value.integer(2))
      true

      iex> alias Absynthe.Preserves.{Value, Compare}
      iex> Compare.gte?(Value.integer(1), Value.integer(2))
      false
  """
  @spec gte?(Value.t(), Value.t()) :: boolean()
  def gte?(a, b), do: compare(a, b) in [:gt, :eq]

  @doc """
  Returns true if `a` equals `b` according to Preserves ordering.

  ## Examples

      iex> alias Absynthe.Preserves.{Value, Compare}
      iex> Compare.eq?(Value.integer(42), Value.integer(42))
      true

      iex> alias Absynthe.Preserves.{Value, Compare}
      iex> Compare.eq?(Value.integer(42), Value.integer(43))
      false
  """
  @spec eq?(Value.t(), Value.t()) :: boolean()
  def eq?(a, b), do: compare(a, b) == :eq

  # Min/Max functions

  @doc """
  Returns the smaller of two Preserves values.

  ## Examples

      iex> alias Absynthe.Preserves.{Value, Compare}
      iex> Compare.min(Value.integer(1), Value.integer(2))
      {:integer, 1}

      iex> alias Absynthe.Preserves.{Value, Compare}
      iex> Compare.min(Value.string("banana"), Value.string("apple"))
      {:string, "apple"}
  """
  @spec min(Value.t(), Value.t()) :: Value.t()
  def min(a, b) do
    case compare(a, b) do
      :lt -> a
      :eq -> a
      :gt -> b
    end
  end

  @doc """
  Returns the larger of two Preserves values.

  ## Examples

      iex> alias Absynthe.Preserves.{Value, Compare}
      iex> Compare.max(Value.integer(1), Value.integer(2))
      {:integer, 2}

      iex> alias Absynthe.Preserves.{Value, Compare}
      iex> Compare.max(Value.string("banana"), Value.string("apple"))
      {:string, "banana"}
  """
  @spec max(Value.t(), Value.t()) :: Value.t()
  def max(a, b) do
    case compare(a, b) do
      :lt -> b
      :eq -> a
      :gt -> a
    end
  end

  # Sorting function

  @doc """
  Sorts a list of Preserves values in ascending order.

  ## Examples

      iex> alias Absynthe.Preserves.{Value, Compare}
      iex> values = [Value.integer(3), Value.integer(1), Value.integer(2)]
      iex> Compare.sort(values)
      [{:integer, 1}, {:integer, 2}, {:integer, 3}]

      iex> alias Absynthe.Preserves.{Value, Compare}
      iex> values = [Value.sequence([]), Value.boolean(true), Value.integer(42)]
      iex> Compare.sort(values)
      [{:boolean, true}, {:integer, 42}, {:sequence, []}]
  """
  @spec sort([Value.t()]) :: [Value.t()]
  def sort(values) when is_list(values) do
    Enum.sort(values, fn a, b -> compare(a, b) != :gt end)
  end

  # Private helper functions

  # Categorize values into their top-level ordering groups
  defp value_category({:boolean, _}), do: :atom
  defp value_category({:double, _}), do: :atom
  defp value_category({:integer, _}), do: :atom
  defp value_category({:string, _}), do: :atom
  defp value_category({:binary, _}), do: :atom
  defp value_category({:symbol, _}), do: :atom
  defp value_category({:record, _}), do: :compound
  defp value_category({:sequence, _}), do: :compound
  defp value_category({:set, _}), do: :compound
  defp value_category({:dictionary, _}), do: :compound
  defp value_category({:embedded, _}), do: :embedded

  # Compare atomic values
  defp compare_atoms({tag_a, _}, {tag_b, _}) when tag_a != tag_b do
    compare_atom_tags(tag_a, tag_b)
  end

  defp compare_atoms({:boolean, a}, {:boolean, b}) do
    compare_booleans(a, b)
  end

  defp compare_atoms({:double, a}, {:double, b}) do
    compare_doubles(a, b)
  end

  defp compare_atoms({:integer, a}, {:integer, b}) do
    compare_integers(a, b)
  end

  defp compare_atoms({:string, a}, {:string, b}) do
    compare_strings(a, b)
  end

  defp compare_atoms({:binary, a}, {:binary, b}) do
    compare_binaries(a, b)
  end

  defp compare_atoms({:symbol, a}, {:symbol, b}) do
    compare_symbols(a, b)
  end

  # Atom type ordering: boolean < double < integer < string < binary < symbol
  defp compare_atom_tags(:boolean, _), do: :lt
  defp compare_atom_tags(:double, :boolean), do: :gt
  defp compare_atom_tags(:double, _), do: :lt
  defp compare_atom_tags(:integer, tag) when tag in [:boolean, :double], do: :gt
  defp compare_atom_tags(:integer, _), do: :lt
  defp compare_atom_tags(:string, tag) when tag in [:boolean, :double, :integer], do: :gt
  defp compare_atom_tags(:string, _), do: :lt
  defp compare_atom_tags(:binary, :symbol), do: :lt
  defp compare_atom_tags(:binary, _), do: :gt
  defp compare_atom_tags(:symbol, _), do: :gt

  # Boolean comparison: false < true
  defp compare_booleans(false, true), do: :lt
  defp compare_booleans(true, false), do: :gt
  defp compare_booleans(_, _), do: :eq

  # Double comparison with IEEE 754 total order
  # Handles: -∞ < negative < -0 < +0 < positive < +∞ < NaN
  defp compare_doubles(a, b) do
    cond do
      # NaN handling: NaN is greater than everything (including itself for ordering)
      is_nan(a) and is_nan(b) -> :eq
      is_nan(a) -> :gt
      is_nan(b) -> :lt

      # Infinity handling
      is_neg_infinity(a) and is_neg_infinity(b) -> :eq
      is_neg_infinity(a) -> :lt
      is_neg_infinity(b) -> :gt
      is_pos_infinity(a) and is_pos_infinity(b) -> :eq
      is_pos_infinity(a) -> :gt
      is_pos_infinity(b) -> :lt

      # Zero sign handling: -0.0 < +0.0
      is_negative_zero(a) and is_positive_zero(b) -> :lt
      is_positive_zero(a) and is_negative_zero(b) -> :gt

      # Regular comparison
      a < b -> :lt
      a > b -> :gt
      true -> :eq
    end
  end

  # Float helper predicates
  defp is_nan(x), do: x != x

  defp is_neg_infinity(x) do
    x < 0 and not is_nan(x) and x * 0 != 0
  end

  defp is_pos_infinity(x) do
    x > 0 and not is_nan(x) and x * 0 != 0
  end

  defp is_negative_zero(x) do
    x == 0.0 and :math.atan2(x, -1.0) < 0
  end

  defp is_positive_zero(x) do
    x == 0.0 and :math.atan2(x, -1.0) > 0
  end

  # Integer comparison
  defp compare_integers(a, b) do
    cond do
      a < b -> :lt
      a > b -> :gt
      true -> :eq
    end
  end

  # String comparison (lexicographic by UTF-8 bytes)
  defp compare_strings(a, b) do
    cond do
      a < b -> :lt
      a > b -> :gt
      true -> :eq
    end
  end

  # Binary comparison (lexicographic)
  defp compare_binaries(a, b) do
    cond do
      a < b -> :lt
      a > b -> :gt
      true -> :eq
    end
  end

  # Symbol comparison (lexicographic by UTF-8 bytes)
  defp compare_symbols(a, b) do
    cond do
      a < b -> :lt
      a > b -> :gt
      true -> :eq
    end
  end

  # Compare compound values
  defp compare_compounds({tag_a, _}, {tag_b, _}) when tag_a != tag_b do
    compare_compound_tags(tag_a, tag_b)
  end

  defp compare_compounds({:record, a}, {:record, b}) do
    compare_records(a, b)
  end

  defp compare_compounds({:sequence, a}, {:sequence, b}) do
    compare_sequences(a, b)
  end

  defp compare_compounds({:set, a}, {:set, b}) do
    compare_sets(a, b)
  end

  defp compare_compounds({:dictionary, a}, {:dictionary, b}) do
    compare_dictionaries(a, b)
  end

  # Compound type ordering: record < sequence < set < dictionary
  defp compare_compound_tags(:record, _), do: :lt
  defp compare_compound_tags(:sequence, :record), do: :gt
  defp compare_compound_tags(:sequence, _), do: :lt
  defp compare_compound_tags(:set, tag) when tag in [:record, :sequence], do: :gt
  defp compare_compound_tags(:set, _), do: :lt
  defp compare_compound_tags(:dictionary, _), do: :gt

  # Record comparison: arity, then label, then fields left-to-right
  defp compare_records({label_a, fields_a}, {label_b, fields_b}) do
    arity_a = length(fields_a)
    arity_b = length(fields_b)

    cond do
      arity_a < arity_b -> :lt
      arity_a > arity_b -> :gt
      true ->
        # Same arity, compare labels
        case compare(label_a, label_b) do
          :eq -> compare_sequences(fields_a, fields_b)
          result -> result
        end
    end
  end

  # Sequence comparison: lexicographic by elements
  defp compare_sequences([], []), do: :eq
  defp compare_sequences([], _), do: :lt
  defp compare_sequences(_, []), do: :gt
  defp compare_sequences([a | rest_a], [b | rest_b]) do
    case compare(a, b) do
      :eq -> compare_sequences(rest_a, rest_b)
      result -> result
    end
  end

  # Set comparison: lexicographic by sorted elements
  defp compare_sets(a, b) do
    list_a = a |> MapSet.to_list() |> sort()
    list_b = b |> MapSet.to_list() |> sort()
    compare_sequences(list_a, list_b)
  end

  # Dictionary comparison: lexicographic by sorted key-value pairs
  defp compare_dictionaries(a, b) do
    # Convert to sorted list of {key, value} pairs
    pairs_a = a |> Map.to_list() |> sort_pairs()
    pairs_b = b |> Map.to_list() |> sort_pairs()
    compare_pair_sequences(pairs_a, pairs_b)
  end

  # Sort key-value pairs by key
  defp sort_pairs(pairs) do
    Enum.sort(pairs, fn {k1, _}, {k2, _} -> compare(k1, k2) != :gt end)
  end

  # Compare sequences of key-value pairs
  defp compare_pair_sequences([], []), do: :eq
  defp compare_pair_sequences([], _), do: :lt
  defp compare_pair_sequences(_, []), do: :gt
  defp compare_pair_sequences([{k1, v1} | rest_a], [{k2, v2} | rest_b]) do
    case compare(k1, k2) do
      :eq ->
        case compare(v1, v2) do
          :eq -> compare_pair_sequences(rest_a, rest_b)
          result -> result
        end
      result -> result
    end
  end

  # Embedded comparison: use Elixir's default term ordering
  defp compare_embedded({:embedded, a}, {:embedded, b}) do
    cond do
      a < b -> :lt
      a > b -> :gt
      true -> :eq
    end
  end
end
