defmodule Absynthe.Dataspace.Pattern do
  @moduledoc """
  Pattern compilation and matching for dataspace subscriptions.

  This module implements the pattern language used in the Syndicated Actor Model
  to define what assertions an observer is interested in. Patterns are Preserves
  values with special symbols that denote wildcards and captures.

  ## Pattern Language

  Patterns are Preserves values with special meaning for certain symbols:

  - `{:symbol, "_"}` - **Wildcard**: Matches anything but does not capture the value
  - `{:symbol, "$"}` - **Capture**: Matches anything and extracts the value for the observer
  - `{:record, {label, fields}}` - Matches record structure with the given label and field patterns
  - Other values - Match literally (must be equal according to Preserves equality)

  ## Compilation Process

  When a pattern is compiled, it is transformed into an efficient representation
  consisting of:

  - **Constraints**: Paths and values that must match exactly (used for indexing)
  - **Captures**: Paths where values should be extracted
  - **Original**: The original pattern for reference

  ## Path Representation

  Paths are lists of path elements that describe how to navigate to a value within
  a Preserves structure:

  - `:label` - The label of a record
  - `{:field, n}` - The nth field of a record (0-indexed)
  - `{:key, k}` - The value associated with key k in a dictionary
  - `n` (integer) - The nth element of a sequence (0-indexed)

  ## Examples

      # Pattern: Match any Person record, capture the age
      iex> pattern = {:record, {{:symbol, "Person"}, [{:symbol, "_"}, {:symbol, "$"}]}}
      iex> compiled = Pattern.compile(pattern)
      iex> compiled.constraints
      [{[:label], {:symbol, "Person"}}]
      iex> compiled.captures
      [[{:field, 1}]]

      # Pattern: Match Message with "chat" type, capture the content
      iex> pattern = {:record, {{:symbol, "Message"}, [{:string, "chat"}, {:symbol, "$"}]}}
      iex> compiled = Pattern.compile(pattern)
      iex> {:ok, [content]} = Pattern.match(compiled,
      ...>   {:record, {{:symbol, "Message"}, [{:string, "chat"}, {:string, "hello"}]}})
      iex> content
      {:string, "hello"}

      # Pattern: Nested captures
      iex> pattern = {:record, {{:symbol, "Outer"}, [{:record, {{:symbol, "Inner"}, [{:symbol, "$"}]}}]}}
      iex> compiled = Pattern.compile(pattern)
      iex> value = {:record, {{:symbol, "Outer"}, [{:record, {{:symbol, "Inner"}, [{:integer, 42}]}}]}}
      iex> {:ok, [captured]} = Pattern.match(compiled, value)
      iex> captured
      {:integer, 42}
  """

  alias Absynthe.Preserves.Value

  @type path :: [path_element()]
  @type path_element ::
          :label
          | {:field, non_neg_integer()}
          | {:key, Value.t()}
          | non_neg_integer()

  @type t :: %__MODULE__{
          constraints: [{path(), Value.t()}],
          captures: [path()],
          original: Value.t()
        }

  defstruct constraints: [], captures: [], original: nil

  @wildcard {:symbol, "_"}

  @doc """
  Compiles a pattern into an efficient matching structure.

  The compilation process walks the pattern recursively, building:
  - Constraints for exact matches (used for indexing)
  - Capture paths for extracting values
  - References to the original pattern

  ## Parameters

  - `pattern` - A Preserves value representing the pattern

  ## Returns

  A compiled pattern structure containing constraints and capture paths.

  ## Examples

      iex> Pattern.compile({:symbol, "$"})
      %Pattern{constraints: [], captures: [[]], original: {:symbol, "$"}}

      iex> Pattern.compile({:integer, 42})
      %Pattern{constraints: [{[], {:integer, 42}}], captures: [], original: {:integer, 42}}

      iex> Pattern.compile({:record, {{:symbol, "Foo"}, [{:symbol, "$"}]}})
      %Pattern{
        constraints: [{[:label], {:symbol, "Foo"}}],
        captures: [[{:field, 0}]],
        original: {:record, {{:symbol, "Foo"}, [{:symbol, "$"}]}}
      }
  """
  @spec compile(Value.t()) :: t()
  def compile(pattern) do
    {constraints, captures} = compile_pattern(pattern, [])

    %__MODULE__{
      constraints: constraints,
      captures: captures,
      original: pattern
    }
  end

  @doc """
  Matches a value against a compiled pattern.

  If the value matches the pattern (all constraints are satisfied), returns
  the captured values in the order they appear in the pattern. Otherwise,
  returns `:no_match`.

  ## Parameters

  - `compiled` - A compiled pattern
  - `value` - A Preserves value to match against

  ## Returns

  - `{:ok, captures}` - List of captured values if match succeeds
  - `:no_match` - If the value doesn't match the pattern

  ## Examples

      iex> pattern = Pattern.compile({:symbol, "$"})
      iex> Pattern.match(pattern, {:integer, 42})
      {:ok, [{:integer, 42}]}

      iex> pattern = Pattern.compile({:integer, 42})
      iex> Pattern.match(pattern, {:integer, 42})
      {:ok, []}

      iex> pattern = Pattern.compile({:integer, 42})
      iex> Pattern.match(pattern, {:integer, 99})
      :no_match
  """
  @spec match(t(), Value.t()) :: {:ok, [Value.t()]} | :no_match
  def match(%__MODULE__{constraints: constraints, captures: capture_paths}, value) do
    # First check all constraints
    constraints_satisfied =
      Enum.all?(constraints, fn {path, expected} ->
        case extract_path(value, path) do
          {:ok, actual} -> actual == expected
          :error -> false
        end
      end)

    if constraints_satisfied do
      # Extract all captures
      captures =
        Enum.map(capture_paths, fn path ->
          case extract_path(value, path) do
            {:ok, captured} -> captured
            :error -> nil
          end
        end)

      # If any capture failed, no match
      if Enum.any?(captures, &is_nil/1) do
        :no_match
      else
        {:ok, captures}
      end
    else
      :no_match
    end
  end

  @doc """
  Extracts a value at the given path from a Preserves value.

  Navigates through the structure following the path elements:
  - `:label` - Extract the label from a record
  - `{:field, n}` - Extract the nth field from a record
  - `{:key, k}` - Extract the value for key k from a dictionary
  - `n` - Extract the nth element from a sequence

  ## Parameters

  - `value` - The Preserves value to extract from
  - `path` - The path to follow

  ## Returns

  - `{:ok, value}` - The extracted value
  - `:error` - If the path is invalid for the structure

  ## Examples

      iex> value = {:record, {{:symbol, "Person"}, [{:string, "Alice"}, {:integer, 30}]}}
      iex> Pattern.extract_path(value, [:label])
      {:ok, {:symbol, "Person"}}

      iex> Pattern.extract_path(value, [{:field, 0}])
      {:ok, {:string, "Alice"}}

      iex> Pattern.extract_path(value, [{:field, 1}])
      {:ok, {:integer, 30}}

      iex> Pattern.extract_path(value, [{:field, 2}])
      :error
  """
  @spec extract_path(Value.t(), path()) :: {:ok, Value.t()} | :error
  def extract_path(value, []), do: {:ok, value}

  def extract_path({:record, {label, _fields}}, [:label | rest]) do
    extract_path(label, rest)
  end

  def extract_path({:record, {_label, fields}}, [{:field, index} | rest])
      when is_integer(index) and index >= 0 do
    if index < length(fields) do
      extract_path(Enum.at(fields, index), rest)
    else
      :error
    end
  end

  def extract_path({:sequence, elements}, [index | rest])
      when is_integer(index) and index >= 0 do
    if index < length(elements) do
      extract_path(Enum.at(elements, index), rest)
    else
      :error
    end
  end

  def extract_path({:dictionary, entries}, [{:key, key} | rest]) do
    case Enum.find(entries, fn {k, _v} -> k == key end) do
      {^key, value} -> extract_path(value, rest)
      nil -> :error
    end
  end

  def extract_path(_value, _path), do: :error

  @doc """
  Returns the paths that should be indexed for efficient lookup.

  Index paths are the constraint paths with their expected values. These are
  used by the Skeleton index to efficiently find matching assertions.

  ## Parameters

  - `compiled` - A compiled pattern

  ## Returns

  A list of tuples containing paths and their expected values.

  ## Examples

      iex> pattern = Pattern.compile({:record, {{:symbol, "Person"}, [{:string, "Alice"}, {:symbol, "$"}]}})
      iex> Pattern.index_paths(pattern)
      [{[:label], {:symbol, "Person"}}, {[{:field, 0}], {:string, "Alice"}}]

      iex> pattern = Pattern.compile({:symbol, "$"})
      iex> Pattern.index_paths(pattern)
      []
  """
  @spec index_paths(t()) :: [{path(), Value.t()}]
  def index_paths(%__MODULE__{constraints: constraints}) do
    constraints
  end

  # Private compilation functions

  @spec compile_pattern(Value.t(), path()) :: {[{path(), Value.t()}], [path()]}
  defp compile_pattern(@wildcard, _path) do
    # Wildcard: no constraints, no captures
    {[], []}
  end

  defp compile_pattern({:symbol, "$" <> _}, path) do
    # Capture (either "$" or "$name"): no constraints, add current path to captures
    {[], [Enum.reverse(path)]}
  end

  defp compile_pattern({:record, {label, fields}}, path) do
    # Compile the label
    {label_constraints, label_captures} = compile_pattern(label, [:label | path])

    # Compile each field
    {field_constraints, field_captures} =
      fields
      |> Enum.with_index()
      |> Enum.reduce({[], []}, fn {field, index}, {constraints_acc, captures_acc} ->
        {field_constraints, field_captures} = compile_pattern(field, [{:field, index} | path])
        {constraints_acc ++ field_constraints, captures_acc ++ field_captures}
      end)

    # Combine label and field results
    {label_constraints ++ field_constraints, label_captures ++ field_captures}
  end

  defp compile_pattern({:sequence, elements}, path) do
    # Compile each element
    elements
    |> Enum.with_index()
    |> Enum.reduce({[], []}, fn {element, index}, {constraints_acc, captures_acc} ->
      {elem_constraints, elem_captures} = compile_pattern(element, [index | path])
      {constraints_acc ++ elem_constraints, captures_acc ++ elem_captures}
    end)
  end

  defp compile_pattern({:dictionary, entries}, path) do
    # Compile each dictionary entry
    Enum.reduce(entries, {[], []}, fn {key, value}, {constraints_acc, captures_acc} ->
      {value_constraints, value_captures} = compile_pattern(value, [{:key, key} | path])
      {constraints_acc ++ value_constraints, captures_acc ++ value_captures}
    end)
  end

  defp compile_pattern({:set, elements}, path) do
    # Sets are treated similarly to sequences for pattern matching
    elements
    |> Enum.with_index()
    |> Enum.reduce({[], []}, fn {element, index}, {constraints_acc, captures_acc} ->
      {elem_constraints, elem_captures} = compile_pattern(element, [index | path])
      {constraints_acc ++ elem_constraints, captures_acc ++ elem_captures}
    end)
  end

  defp compile_pattern(literal, path) do
    # Literal value: add as constraint at current path
    {[{Enum.reverse(path), literal}], []}
  end
end
