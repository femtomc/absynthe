defmodule Absynthe.Preserves.Pattern do
  import Kernel, except: [match?: 2]

  @moduledoc """
  Pattern matching with captures for Preserves values.

  This module provides pattern matching functionality for `Absynthe.Preserves.Value`
  structures, supporting literal matching, wildcards, captures, and compound patterns.

  ## Pattern Types

  - **Literal values** - Any `Value.t()` matches exactly
  - **Discard** - `:_` matches anything, captures nothing
  - **Capture** - `{:capture, name}` matches anything, binds to `name`
  - **Compound patterns** - Records, sequences, sets, and dictionaries with nested patterns

  ## Examples

      # Match exact value
      iex> pattern = Absynthe.Preserves.Value.integer(42)
      iex> value = Absynthe.Preserves.Value.integer(42)
      iex> Absynthe.Preserves.Pattern.match(pattern, value)
      {:ok, %{}}

      # Discard - match anything
      iex> pattern = Absynthe.Preserves.Pattern.discard()
      iex> value = Absynthe.Preserves.Value.string("anything")
      iex> Absynthe.Preserves.Pattern.match(pattern, value)
      {:ok, %{}}

      # Capture
      iex> pattern = Absynthe.Preserves.Pattern.capture(:x)
      iex> value = Absynthe.Preserves.Value.integer(42)
      iex> Absynthe.Preserves.Pattern.match(pattern, value)
      {:ok, %{x: {:integer, 42}}}

      # Record pattern with captures
      iex> pattern = Absynthe.Preserves.Value.record(
      ...>   Absynthe.Preserves.Value.symbol("person"),
      ...>   [Absynthe.Preserves.Pattern.capture(:name), Absynthe.Preserves.Pattern.discard()]
      ...> )
      iex> value = Absynthe.Preserves.Value.record(
      ...>   Absynthe.Preserves.Value.symbol("person"),
      ...>   [Absynthe.Preserves.Value.string("Alice"), Absynthe.Preserves.Value.integer(30)]
      ...> )
      iex> Absynthe.Preserves.Pattern.match(pattern, value)
      {:ok, %{name: {:string, "Alice"}}}

      # Sequence pattern (must match length exactly)
      iex> pattern = Absynthe.Preserves.Value.sequence([
      ...>   Absynthe.Preserves.Pattern.capture(:first),
      ...>   Absynthe.Preserves.Pattern.discard(),
      ...>   Absynthe.Preserves.Pattern.capture(:last)
      ...> ])
      iex> value = Absynthe.Preserves.Value.sequence([
      ...>   Absynthe.Preserves.Value.integer(1),
      ...>   Absynthe.Preserves.Value.integer(2),
      ...>   Absynthe.Preserves.Value.integer(3)
      ...> ])
      iex> Absynthe.Preserves.Pattern.match(pattern, value)
      {:ok, %{first: {:integer, 1}, last: {:integer, 3}}}
  """

  alias Absynthe.Preserves.Value

  @typedoc """
  A pattern that can match against Preserves values.

  Patterns can be:
  - A literal `Value.t()` that matches exactly
  - `:_` (discard) that matches anything without capturing
  - `{:capture, atom()}` that matches anything and binds to a variable
  - A compound pattern (record, sequence, set, dictionary) containing nested patterns
  """
  @type pattern ::
          Value.t()
          | :_
          | {:capture, atom()}

  @typedoc """
  Variable bindings from a successful pattern match.

  Maps variable names (atoms) to the Preserves values they matched.
  """
  @type bindings :: %{atom() => Value.t()}

  # Pattern Constructors

  @doc """
  Creates a discard pattern that matches anything without capturing.

  ## Examples

      iex> Absynthe.Preserves.Pattern.discard()
      :_
  """
  @spec discard() :: :_
  def discard, do: :_

  @doc """
  Creates a capture pattern that matches anything and binds to a variable.

  ## Examples

      iex> Absynthe.Preserves.Pattern.capture(:x)
      {:capture, :x}

      iex> pattern = Absynthe.Preserves.Pattern.capture(:name)
      iex> value = Absynthe.Preserves.Value.string("Alice")
      iex> Absynthe.Preserves.Pattern.match(pattern, value)
      {:ok, %{name: {:string, "Alice"}}}
  """
  @spec capture(atom()) :: {:capture, atom()}
  def capture(name) when is_atom(name), do: {:capture, name}

  @doc """
  Creates a bind pattern that both matches a pattern AND captures the result.

  This is useful when you want to match a specific structure but also capture
  the entire matched value.

  ## Examples

      iex> # Match an integer and capture it as :num
      iex> pattern = Absynthe.Preserves.Pattern.bind(:num, Absynthe.Preserves.Value.integer(42))
      iex> value = Absynthe.Preserves.Value.integer(42)
      iex> Absynthe.Preserves.Pattern.match(pattern, value)
      {:ok, %{num: {:integer, 42}}}

      iex> # Match a record structure and capture it
      iex> record_pattern = Absynthe.Preserves.Value.record(
      ...>   Absynthe.Preserves.Value.symbol("point"),
      ...>   [Absynthe.Preserves.Pattern.capture(:x), Absynthe.Preserves.Pattern.capture(:y)]
      ...> )
      iex> pattern = Absynthe.Preserves.Pattern.bind(:point, record_pattern)
      iex> value = Absynthe.Preserves.Value.record(
      ...>   Absynthe.Preserves.Value.symbol("point"),
      ...>   [Absynthe.Preserves.Value.integer(10), Absynthe.Preserves.Value.integer(20)]
      ...> )
      iex> {:ok, bindings} = Absynthe.Preserves.Pattern.match(pattern, value)
      iex> bindings[:point]
      {:record, {{:symbol, "point"}, [{:integer, 10}, {:integer, 20}]}}
      iex> bindings[:x]
      {:integer, 10}
      iex> bindings[:y]
      {:integer, 20}
  """
  @spec bind(atom(), pattern()) :: {:bind, atom(), pattern()}
  def bind(name, pattern) when is_atom(name) do
    {:bind, name, pattern}
  end

  # Core Pattern Matching

  @doc """
  Matches a pattern against a value, returning bindings on success.

  Returns `{:ok, bindings}` if the pattern matches, or `:error` if it doesn't.

  ## Matching Rules

  - **Discard (`:_`)**: Always matches, produces no bindings
  - **Capture (`{:capture, name}`)**: Always matches, binds value to `name`
  - **Bind (`{:bind, name, pattern}`)**: Matches pattern, binds result to `name`
  - **Literal values**: Must match exactly (using structural equality)
  - **Record**: Label must match exactly, arity must match, fields match positionally
  - **Sequence**: Length must match, elements match positionally
  - **Set**: Pattern set must be subset of value set (each pattern element matches some value element)
  - **Dictionary**: Pattern keys must be subset of value keys, corresponding values must match

  ## Examples

      # Exact match
      iex> Absynthe.Preserves.Pattern.match(
      ...>   Absynthe.Preserves.Value.integer(42),
      ...>   Absynthe.Preserves.Value.integer(42)
      ...> )
      {:ok, %{}}

      # No match
      iex> Absynthe.Preserves.Pattern.match(
      ...>   Absynthe.Preserves.Value.integer(42),
      ...>   Absynthe.Preserves.Value.integer(43)
      ...> )
      :error

      # Capture
      iex> Absynthe.Preserves.Pattern.match(
      ...>   {:capture, :x},
      ...>   Absynthe.Preserves.Value.string("hello")
      ...> )
      {:ok, %{x: {:string, "hello"}}}

      # Record with mixed patterns
      iex> pattern = Absynthe.Preserves.Value.record(
      ...>   Absynthe.Preserves.Value.symbol("user"),
      ...>   [
      ...>     {:capture, :id},
      ...>     Absynthe.Preserves.Value.string("active"),
      ...>     :_
      ...>   ]
      ...> )
      iex> value = Absynthe.Preserves.Value.record(
      ...>   Absynthe.Preserves.Value.symbol("user"),
      ...>   [
      ...>     Absynthe.Preserves.Value.integer(123),
      ...>     Absynthe.Preserves.Value.string("active"),
      ...>     Absynthe.Preserves.Value.boolean(true)
      ...>   ]
      ...> )
      iex> Absynthe.Preserves.Pattern.match(pattern, value)
      {:ok, %{id: {:integer, 123}}}
  """
  @spec match(pattern(), Value.t()) :: {:ok, bindings()} | :error
  def match(pattern, value) do
    case do_match(pattern, value, %{}) do
      {:ok, bindings} -> {:ok, bindings}
      :error -> :error
    end
  end

  @doc """
  Returns true if the pattern matches the value.

  This is a boolean convenience function that doesn't return bindings.

  ## Examples

      iex> Absynthe.Preserves.Pattern.match?(
      ...>   Absynthe.Preserves.Value.integer(42),
      ...>   Absynthe.Preserves.Value.integer(42)
      ...> )
      true

      iex> Absynthe.Preserves.Pattern.match?(
      ...>   Absynthe.Preserves.Value.integer(42),
      ...>   Absynthe.Preserves.Value.integer(43)
      ...> )
      false

      iex> Absynthe.Preserves.Pattern.match?(
      ...>   :_,
      ...>   Absynthe.Preserves.Value.string("anything")
      ...> )
      true
  """
  @spec match?(pattern(), Value.t()) :: boolean()
  def match?(pat, val) do
    match(pat, val) != :error
  end

  @doc """
  Filters a list of values to only those matching the pattern.

  ## Examples

      iex> pattern = Absynthe.Preserves.Value.record(
      ...>   Absynthe.Preserves.Value.symbol("user"),
      ...>   [:_, Absynthe.Preserves.Value.string("active")]
      ...> )
      iex> values = [
      ...>   Absynthe.Preserves.Value.record(
      ...>     Absynthe.Preserves.Value.symbol("user"),
      ...>     [Absynthe.Preserves.Value.integer(1), Absynthe.Preserves.Value.string("active")]
      ...>   ),
      ...>   Absynthe.Preserves.Value.record(
      ...>     Absynthe.Preserves.Value.symbol("user"),
      ...>     [Absynthe.Preserves.Value.integer(2), Absynthe.Preserves.Value.string("inactive")]
      ...>   ),
      ...>   Absynthe.Preserves.Value.record(
      ...>     Absynthe.Preserves.Value.symbol("user"),
      ...>     [Absynthe.Preserves.Value.integer(3), Absynthe.Preserves.Value.string("active")]
      ...>   )
      ...> ]
      iex> Absynthe.Preserves.Pattern.matches(pattern, values)
      [
        {:record, {{:symbol, "user"}, [{:integer, 1}, {:string, "active"}]}},
        {:record, {{:symbol, "user"}, [{:integer, 3}, {:string, "active"}]}}
      ]
  """
  @spec matches(pattern(), [Value.t()]) :: [Value.t()]
  def matches(pat, values) when is_list(values) do
    Enum.filter(values, fn val -> __MODULE__.match?(pat, val) end)
  end

  @doc """
  Finds the first matching value and returns it with bindings.

  Returns `{:ok, value, bindings}` for the first match, or `:error` if no match found.

  ## Examples

      iex> pattern = {:capture, :x}
      iex> values = [
      ...>   Absynthe.Preserves.Value.integer(1),
      ...>   Absynthe.Preserves.Value.integer(2),
      ...>   Absynthe.Preserves.Value.integer(3)
      ...> ]
      iex> Absynthe.Preserves.Pattern.find_match(pattern, values)
      {:ok, {:integer, 1}, %{x: {:integer, 1}}}

      iex> pattern = Absynthe.Preserves.Value.integer(42)
      iex> values = [
      ...>   Absynthe.Preserves.Value.integer(1),
      ...>   Absynthe.Preserves.Value.integer(2)
      ...> ]
      iex> Absynthe.Preserves.Pattern.find_match(pattern, values)
      :error
  """
  @spec find_match(pattern(), [Value.t()]) :: {:ok, Value.t(), bindings()} | :error
  def find_match(pattern, values) when is_list(values) do
    Enum.find_value(values, :error, fn value ->
      case match(pattern, value) do
        {:ok, bindings} -> {:ok, value, bindings}
        :error -> nil
      end
    end)
  end

  # Pattern Utilities

  @doc """
  Returns true if the term is a valid pattern.

  ## Examples

      iex> Absynthe.Preserves.Pattern.pattern?(:_)
      true

      iex> Absynthe.Preserves.Pattern.pattern?({:capture, :x})
      true

      iex> Absynthe.Preserves.Pattern.pattern?(Absynthe.Preserves.Value.integer(42))
      true

      iex> Absynthe.Preserves.Pattern.pattern?("not a pattern")
      false
  """
  @spec pattern?(term()) :: boolean()
  def pattern?(:_), do: true
  def pattern?({:capture, name}) when is_atom(name), do: true
  def pattern?({:bind, name, pattern}) when is_atom(name), do: pattern?(pattern)

  def pattern?({:record, {label, fields}}) do
    pattern?(label) and is_list(fields) and Enum.all?(fields, &pattern?/1)
  end

  def pattern?({:sequence, items}) when is_list(items) do
    Enum.all?(items, &pattern?/1)
  end

  def pattern?({:set, items}) when is_struct(items, MapSet) do
    Enum.all?(items, &pattern?/1)
  end

  def pattern?({:dictionary, map}) when is_map(map) do
    Enum.all?(map, fn {k, v} -> pattern?(k) and pattern?(v) end)
  end

  def pattern?({:boolean, _}), do: true
  def pattern?({:integer, _}), do: true
  def pattern?({:double, _}), do: true
  def pattern?({:string, _}), do: true
  def pattern?({:binary, _}), do: true
  def pattern?({:symbol, _}), do: true
  def pattern?({:embedded, _}), do: true
  def pattern?(_), do: false

  @doc """
  Extracts all capture variable names from a pattern.

  Returns a list of unique variable names (atoms) in the order they appear.

  ## Examples

      iex> pattern = {:capture, :x}
      iex> Absynthe.Preserves.Pattern.variables(pattern)
      [:x]

      iex> pattern = Absynthe.Preserves.Value.sequence([
      ...>   {:capture, :first},
      ...>   :_,
      ...>   {:capture, :last}
      ...> ])
      iex> Absynthe.Preserves.Pattern.variables(pattern)
      [:first, :last]

      iex> pattern = Absynthe.Preserves.Value.record(
      ...>   Absynthe.Preserves.Value.symbol("point"),
      ...>   [{:capture, :x}, {:capture, :y}]
      ...> )
      iex> Absynthe.Preserves.Pattern.variables(pattern)
      [:x, :y]

      iex> pattern = Absynthe.Preserves.Pattern.bind(:point,
      ...>   Absynthe.Preserves.Value.record(
      ...>     Absynthe.Preserves.Value.symbol("point"),
      ...>     [{:capture, :x}, {:capture, :y}]
      ...>   )
      ...> )
      iex> Absynthe.Preserves.Pattern.variables(pattern)
      [:point, :x, :y]
  """
  @spec variables(pattern()) :: [atom()]
  def variables(pattern) do
    pattern
    |> collect_variables([])
    |> Enum.reverse()
    |> Enum.uniq()
  end

  # Private Implementation

  defp collect_variables(:_, acc), do: acc
  defp collect_variables({:capture, name}, acc), do: [name | acc]

  defp collect_variables({:bind, name, pattern}, acc) do
    acc
    |> then(&[name | &1])
    |> then(&collect_variables(pattern, &1))
  end

  defp collect_variables({:record, {label, fields}}, acc) do
    acc
    |> then(&collect_variables(label, &1))
    |> then(&Enum.reduce(fields, &1, fn field, a -> collect_variables(field, a) end))
  end

  defp collect_variables({:sequence, items}, acc) do
    Enum.reduce(items, acc, fn item, a -> collect_variables(item, a) end)
  end

  defp collect_variables({:set, items}, acc) do
    Enum.reduce(items, acc, fn item, a -> collect_variables(item, a) end)
  end

  defp collect_variables({:dictionary, map}, acc) do
    Enum.reduce(map, acc, fn {k, v}, a ->
      a
      |> then(&collect_variables(k, &1))
      |> then(&collect_variables(v, &1))
    end)
  end

  defp collect_variables(_literal, acc), do: acc

  # Core matching implementation

  defp do_match(:_, _value, bindings) do
    {:ok, bindings}
  end

  defp do_match({:capture, name}, value, bindings) do
    case Map.get(bindings, name) do
      nil ->
        {:ok, Map.put(bindings, name, value)}

      ^value ->
        {:ok, bindings}

      _different ->
        :error
    end
  end

  defp do_match({:bind, name, pattern}, value, bindings) do
    case do_match(pattern, value, bindings) do
      {:ok, new_bindings} ->
        case Map.get(new_bindings, name) do
          nil ->
            {:ok, Map.put(new_bindings, name, value)}

          ^value ->
            {:ok, new_bindings}

          _different ->
            :error
        end

      :error ->
        :error
    end
  end

  defp do_match({:record, {label_pattern, field_patterns}}, {:record, {label_value, field_values}}, bindings) do
    if length(field_patterns) != length(field_values) do
      :error
    else
      with {:ok, bindings} <- do_match(label_pattern, label_value, bindings) do
        match_list(field_patterns, field_values, bindings)
      end
    end
  end

  defp do_match({:sequence, pattern_items}, {:sequence, value_items}, bindings) do
    if length(pattern_items) != length(value_items) do
      :error
    else
      match_list(pattern_items, value_items, bindings)
    end
  end

  defp do_match({:set, pattern_items}, {:set, value_items}, bindings) do
    # Pattern set must be a subset - each pattern element must match at least one value element
    # This is more complex as we need to find a matching for the pattern elements
    match_set(MapSet.to_list(pattern_items), MapSet.to_list(value_items), bindings)
  end

  defp do_match({:dictionary, pattern_map}, {:dictionary, value_map}, bindings) do
    # All pattern keys must exist in value map, and their values must match
    Enum.reduce_while(pattern_map, {:ok, bindings}, fn {pattern_key, pattern_value}, {:ok, acc_bindings} ->
      # Find matching key in value map
      case find_matching_key(pattern_key, value_map, acc_bindings) do
        {:ok, _value_key, value_value, new_bindings} ->
          case do_match(pattern_value, value_value, new_bindings) do
            {:ok, result_bindings} -> {:cont, {:ok, result_bindings}}
            :error -> {:halt, :error}
          end

        :error ->
          {:halt, :error}
      end
    end)
  end

  defp do_match(pattern, value, bindings) when pattern == value do
    {:ok, bindings}
  end

  defp do_match(_pattern, _value, _bindings) do
    :error
  end

  defp match_list(patterns, values, bindings) do
    Enum.zip(patterns, values)
    |> Enum.reduce_while({:ok, bindings}, fn {pattern, value}, {:ok, acc} ->
      case do_match(pattern, value, acc) do
        {:ok, new_bindings} -> {:cont, {:ok, new_bindings}}
        :error -> {:halt, :error}
      end
    end)
  end

  defp match_set([], _value_items, bindings) do
    {:ok, bindings}
  end

  defp match_set([pattern | rest_patterns], value_items, bindings) do
    # Try to match this pattern against each value item (with backtracking)
    # We need to try all possible matchings because the greedy approach can fail
    try_match_set_with_backtracking(pattern, rest_patterns, value_items, bindings)
  end

  defp try_match_set_with_backtracking(pattern, rest_patterns, value_items, bindings) do
    # Try matching pattern against each value item
    Enum.find_value(value_items, :error, fn value ->
      case do_match(pattern, value, bindings) do
        {:ok, new_bindings} ->
          # Successfully matched this pattern to this value
          # Try to match the rest of the patterns with remaining values
          remaining_values = List.delete(value_items, value)
          case match_set(rest_patterns, remaining_values, new_bindings) do
            {:ok, final_bindings} -> {:ok, final_bindings}
            :error -> nil  # Continue trying other values
          end

        :error ->
          nil  # Continue trying other values
      end
    end)
  end

  defp find_matching_key(pattern_key, value_map, bindings) do
    Enum.find_value(value_map, :error, fn {val_key, val_value} ->
      case do_match(pattern_key, val_key, bindings) do
        {:ok, new_bindings} -> {:ok, val_key, val_value, new_bindings}
        :error -> nil
      end
    end)
  end
end
