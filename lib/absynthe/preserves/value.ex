defmodule Absynthe.Preserves.Value do
  @moduledoc """
  Core tagged union type representing all Preserves values.

  Preserves is a data model similar to JSON or EDN, but with richer atomic types
  and support for embedded references. This module provides the fundamental
  Value type and operations for working with Preserves data.

  ## Value Types

  - **Atomic values**: boolean, integer, double, string, binary, symbol
  - **Compound values**: record, sequence, set, dictionary
  - **Special**: embedded (domain-specific references)

  ## Examples

      iex> value = Absynthe.Preserves.Value.integer(42)
      {:integer, 42}

      iex> value = Absynthe.Preserves.Value.record(
      ...>   Absynthe.Preserves.Value.symbol("person"),
      ...>   [Absynthe.Preserves.Value.string("Alice")]
      ...> )
      {:record, {{:symbol, "person"}, [{:string, "Alice"}]}}

      iex> Absynthe.Preserves.Value.from_native(42)
      {:integer, 42}

      iex> Absynthe.Preserves.Value.from_native([1, 2, 3])
      {:sequence, [{:integer, 1}, {:integer, 2}, {:integer, 3}]}
  """

  @typedoc """
  The core Preserves Value type - a tagged union of all possible value types.

  Note: For doubles, special IEEE754 values are represented as atoms:
  - `:infinity` - positive infinity
  - `:neg_infinity` - negative infinity
  - `:nan` - Not a Number
  """
  @type t ::
          {:boolean, boolean()}
          | {:integer, integer()}
          | {:double, float() | :infinity | :neg_infinity | :nan}
          | {:string, String.t()}
          | {:binary, binary()}
          | {:symbol, String.t()}
          | {:record, {label :: t(), fields :: [t()]}}
          | {:sequence, [t()]}
          | {:set, MapSet.t(t())}
          | {:dictionary, %{t() => t()}}
          | {:embedded, term()}
          | {:annotated, annotation :: t(), value :: t()}

  # Constructor Functions

  @doc """
  Creates a boolean Value.

  ## Examples

      iex> Absynthe.Preserves.Value.boolean(true)
      {:boolean, true}

      iex> Absynthe.Preserves.Value.boolean(false)
      {:boolean, false}
  """
  @spec boolean(boolean()) :: t()
  def boolean(value) when is_boolean(value), do: {:boolean, value}

  @doc """
  Creates an integer Value.

  ## Examples

      iex> Absynthe.Preserves.Value.integer(42)
      {:integer, 42}

      iex> Absynthe.Preserves.Value.integer(-1_000_000)
      {:integer, -1000000}
  """
  @spec integer(integer()) :: t()
  def integer(value) when is_integer(value), do: {:integer, value}

  @doc """
  Creates a double (float) Value.

  ## Examples

      iex> Absynthe.Preserves.Value.double(3.14)
      {:double, 3.14}

      iex> Absynthe.Preserves.Value.double(-0.5)
      {:double, -0.5}

      iex> Absynthe.Preserves.Value.double(:infinity)
      {:double, :infinity}
  """
  @spec double(float() | :infinity | :neg_infinity | :nan) :: t()
  def double(value) when is_float(value), do: {:double, value}
  def double(:infinity), do: {:double, :infinity}
  def double(:neg_infinity), do: {:double, :neg_infinity}
  def double(:nan), do: {:double, :nan}

  @doc """
  Creates a string Value.

  ## Examples

      iex> Absynthe.Preserves.Value.string("hello")
      {:string, "hello"}

      iex> Absynthe.Preserves.Value.string("Unicode: 你好")
      {:string, "Unicode: 你好"}
  """
  @spec string(String.t()) :: t()
  def string(value) when is_binary(value), do: {:string, value}

  @doc """
  Creates a binary (byte sequence) Value.

  ## Examples

      iex> Absynthe.Preserves.Value.binary(<<1, 2, 3>>)
      {:binary, <<1, 2, 3>>}

      iex> Absynthe.Preserves.Value.binary(<<0xFF, 0xFE>>)
      {:binary, <<255, 254>>}
  """
  @spec binary(binary()) :: t()
  def binary(value) when is_binary(value), do: {:binary, value}

  @doc """
  Creates a symbol Value.

  Symbols are identifier-like strings that appear unquoted in text syntax.

  ## Examples

      iex> Absynthe.Preserves.Value.symbol("name")
      {:symbol, "name"}

      iex> Absynthe.Preserves.Value.symbol("my-identifier")
      {:symbol, "my-identifier"}
  """
  @spec symbol(String.t()) :: t()
  def symbol(name) when is_binary(name), do: {:symbol, name}

  @doc """
  Creates a record Value.

  A record is a labeled tuple with a label (which is itself a Value) and
  a list of field Values.

  ## Examples

      iex> label = Absynthe.Preserves.Value.symbol("point")
      iex> fields = [
      ...>   Absynthe.Preserves.Value.integer(10),
      ...>   Absynthe.Preserves.Value.integer(20)
      ...> ]
      iex> Absynthe.Preserves.Value.record(label, fields)
      {:record, {{:symbol, "point"}, [{:integer, 10}, {:integer, 20}]}}
  """
  @spec record(t(), [t()]) :: t()
  def record(label, fields) when is_list(fields) do
    {:record, {label, fields}}
  end

  @doc """
  Creates a sequence (ordered collection) Value.

  ## Examples

      iex> items = [
      ...>   Absynthe.Preserves.Value.integer(1),
      ...>   Absynthe.Preserves.Value.integer(2),
      ...>   Absynthe.Preserves.Value.integer(3)
      ...> ]
      iex> Absynthe.Preserves.Value.sequence(items)
      {:sequence, [{:integer, 1}, {:integer, 2}, {:integer, 3}]}
  """
  @spec sequence([t()]) :: t()
  def sequence(items) when is_list(items), do: {:sequence, items}

  @doc """
  Creates a set (unordered unique collection) Value.

  ## Examples

      iex> items = [
      ...>   Absynthe.Preserves.Value.integer(1),
      ...>   Absynthe.Preserves.Value.integer(2),
      ...>   Absynthe.Preserves.Value.integer(1)
      ...> ]
      iex> Absynthe.Preserves.Value.set(items)
      {:set, MapSet.new([{:integer, 1}, {:integer, 2}])}
  """
  @spec set([t()]) :: t()
  def set(items) when is_list(items), do: {:set, MapSet.new(items)}

  @doc """
  Creates a dictionary (key-value map) Value.

  ## Examples

      iex> pairs = [
      ...>   {Absynthe.Preserves.Value.symbol("name"), Absynthe.Preserves.Value.string("Alice")},
      ...>   {Absynthe.Preserves.Value.symbol("age"), Absynthe.Preserves.Value.integer(30)}
      ...> ]
      iex> Absynthe.Preserves.Value.dictionary(pairs)
      {:dictionary, %{{:symbol, "name"} => {:string, "Alice"}, {:symbol, "age"} => {:integer, 30}}}
  """
  @spec dictionary([{t(), t()}]) :: t()
  def dictionary(pairs) when is_list(pairs), do: {:dictionary, Map.new(pairs)}

  @doc """
  Creates an embedded (domain-specific reference) Value.

  ## Examples

      iex> Absynthe.Preserves.Value.embedded({:my_ref, 123})
      {:embedded, {:my_ref, 123}}

      iex> Absynthe.Preserves.Value.embedded(%{type: :actor, id: "abc"})
      {:embedded, %{type: :actor, id: "abc"}}
  """
  @spec embedded(term()) :: t()
  def embedded(ref), do: {:embedded, ref}

  @doc """
  Creates an annotated Value with a single annotation.

  Per Preserves spec, each annotation wraps a value. For multiple annotations,
  nest the annotated values: `annotated(ann1, annotated(ann2, value))`.

  ## Examples

      iex> value = Absynthe.Preserves.Value.integer(42)
      iex> annotation = Absynthe.Preserves.Value.symbol("important")
      iex> Absynthe.Preserves.Value.annotated(annotation, value)
      {:annotated, {:symbol, "important"}, {:integer, 42}}
  """
  @spec annotated(t(), t()) :: t()
  def annotated(annotation, value), do: {:annotated, annotation, value}

  # Type Guard Functions

  @doc """
  Returns true if the value is a boolean Value.

  ## Examples

      iex> Absynthe.Preserves.Value.is_boolean?({:boolean, true})
      true

      iex> Absynthe.Preserves.Value.is_boolean?({:integer, 42})
      false
  """
  @spec is_boolean?(t()) :: boolean()
  def is_boolean?({:boolean, _}), do: true
  def is_boolean?(_), do: false

  @doc """
  Returns true if the value is an integer Value.

  ## Examples

      iex> Absynthe.Preserves.Value.is_integer?({:integer, 42})
      true

      iex> Absynthe.Preserves.Value.is_integer?({:double, 3.14})
      false
  """
  @spec is_integer?(t()) :: boolean()
  def is_integer?({:integer, _}), do: true
  def is_integer?(_), do: false

  @doc """
  Returns true if the value is a double Value.

  ## Examples

      iex> Absynthe.Preserves.Value.is_double?({:double, 3.14})
      true

      iex> Absynthe.Preserves.Value.is_double?({:integer, 42})
      false
  """
  @spec is_double?(t()) :: boolean()
  def is_double?({:double, _}), do: true
  def is_double?(_), do: false

  @doc """
  Returns true if the value is a string Value.

  ## Examples

      iex> Absynthe.Preserves.Value.is_string?({:string, "hello"})
      true

      iex> Absynthe.Preserves.Value.is_string?({:symbol, "hello"})
      false
  """
  @spec is_string?(t()) :: boolean()
  def is_string?({:string, _}), do: true
  def is_string?(_), do: false

  @doc """
  Returns true if the value is a binary Value.

  ## Examples

      iex> Absynthe.Preserves.Value.is_binary?({:binary, <<1, 2, 3>>})
      true

      iex> Absynthe.Preserves.Value.is_binary?({:string, "hello"})
      false
  """
  @spec is_binary?(t()) :: boolean()
  def is_binary?({:binary, _}), do: true
  def is_binary?(_), do: false

  @doc """
  Returns true if the value is a symbol Value.

  ## Examples

      iex> Absynthe.Preserves.Value.is_symbol?({:symbol, "name"})
      true

      iex> Absynthe.Preserves.Value.is_symbol?({:string, "name"})
      false
  """
  @spec is_symbol?(t()) :: boolean()
  def is_symbol?({:symbol, _}), do: true
  def is_symbol?(_), do: false

  @doc """
  Returns true if the value is a record Value.

  ## Examples

      iex> value = Absynthe.Preserves.Value.record(
      ...>   Absynthe.Preserves.Value.symbol("point"),
      ...>   [Absynthe.Preserves.Value.integer(10)]
      ...> )
      iex> Absynthe.Preserves.Value.is_record?(value)
      true

      iex> Absynthe.Preserves.Value.is_record?({:sequence, []})
      false
  """
  @spec is_record?(t()) :: boolean()
  def is_record?({:record, _}), do: true
  def is_record?(_), do: false

  @doc """
  Returns true if the value is a sequence Value.

  ## Examples

      iex> Absynthe.Preserves.Value.is_sequence?({:sequence, []})
      true

      iex> Absynthe.Preserves.Value.is_sequence?({:set, MapSet.new()})
      false
  """
  @spec is_sequence?(t()) :: boolean()
  def is_sequence?({:sequence, _}), do: true
  def is_sequence?(_), do: false

  @doc """
  Returns true if the value is a set Value.

  ## Examples

      iex> Absynthe.Preserves.Value.is_set?({:set, MapSet.new()})
      true

      iex> Absynthe.Preserves.Value.is_set?({:sequence, []})
      false
  """
  @spec is_set?(t()) :: boolean()
  def is_set?({:set, _}), do: true
  def is_set?(_), do: false

  @doc """
  Returns true if the value is a dictionary Value.

  ## Examples

      iex> Absynthe.Preserves.Value.is_dictionary?({:dictionary, %{}})
      true

      iex> Absynthe.Preserves.Value.is_dictionary?({:set, MapSet.new()})
      false
  """
  @spec is_dictionary?(t()) :: boolean()
  def is_dictionary?({:dictionary, _}), do: true
  def is_dictionary?(_), do: false

  @doc """
  Returns true if the value is an embedded Value.

  ## Examples

      iex> Absynthe.Preserves.Value.is_embedded?({:embedded, :some_ref})
      true

      iex> Absynthe.Preserves.Value.is_embedded?({:symbol, "name"})
      false
  """
  @spec is_embedded?(t()) :: boolean()
  def is_embedded?({:embedded, _}), do: true
  def is_embedded?(_), do: false

  @doc """
  Returns true if the value is an atomic Value.

  Atomic values are: boolean, integer, double, string, binary, symbol.

  ## Examples

      iex> Absynthe.Preserves.Value.is_atom?({:integer, 42})
      true

      iex> Absynthe.Preserves.Value.is_atom?({:string, "hello"})
      true

      iex> Absynthe.Preserves.Value.is_atom?({:sequence, []})
      false
  """
  @spec is_atom?(t()) :: boolean()
  def is_atom?({:boolean, _}), do: true
  def is_atom?({:integer, _}), do: true
  def is_atom?({:double, _}), do: true
  def is_atom?({:string, _}), do: true
  def is_atom?({:binary, _}), do: true
  def is_atom?({:symbol, _}), do: true
  def is_atom?(_), do: false

  @doc """
  Returns true if the value is a compound Value.

  Compound values are: record, sequence, set, dictionary.

  ## Examples

      iex> Absynthe.Preserves.Value.is_compound?({:sequence, []})
      true

      iex> Absynthe.Preserves.Value.is_compound?({:dictionary, %{}})
      true

      iex> Absynthe.Preserves.Value.is_compound?({:integer, 42})
      false
  """
  @spec is_compound?(t()) :: boolean()
  def is_compound?({:record, _}), do: true
  def is_compound?({:sequence, _}), do: true
  def is_compound?({:set, _}), do: true
  def is_compound?({:dictionary, _}), do: true
  def is_compound?(_), do: false

  # Accessor Functions

  @doc """
  Extracts the inner value from a tagged Value.

  ## Examples

      iex> Absynthe.Preserves.Value.unwrap({:integer, 42})
      42

      iex> Absynthe.Preserves.Value.unwrap({:string, "hello"})
      "hello"

      iex> Absynthe.Preserves.Value.unwrap({:sequence, [{:integer, 1}]})
      [{:integer, 1}]
  """
  @spec unwrap(t()) :: term()
  def unwrap({_tag, value}), do: value

  @doc """
  Gets the label from a record Value.

  ## Examples

      iex> record = Absynthe.Preserves.Value.record(
      ...>   Absynthe.Preserves.Value.symbol("point"),
      ...>   [Absynthe.Preserves.Value.integer(10)]
      ...> )
      iex> Absynthe.Preserves.Value.record_label(record)
      {:symbol, "point"}
  """
  @spec record_label(t()) :: t()
  def record_label({:record, {label, _fields}}), do: label

  @doc """
  Gets the fields from a record Value.

  ## Examples

      iex> record = Absynthe.Preserves.Value.record(
      ...>   Absynthe.Preserves.Value.symbol("point"),
      ...>   [Absynthe.Preserves.Value.integer(10), Absynthe.Preserves.Value.integer(20)]
      ...> )
      iex> Absynthe.Preserves.Value.record_fields(record)
      [{:integer, 10}, {:integer, 20}]
  """
  @spec record_fields(t()) :: [t()]
  def record_fields({:record, {_label, fields}}), do: fields

  @doc """
  Gets the number of fields in a record Value.

  ## Examples

      iex> record = Absynthe.Preserves.Value.record(
      ...>   Absynthe.Preserves.Value.symbol("point"),
      ...>   [Absynthe.Preserves.Value.integer(10), Absynthe.Preserves.Value.integer(20)]
      ...> )
      iex> Absynthe.Preserves.Value.record_arity(record)
      2
  """
  @spec record_arity(t()) :: non_neg_integer()
  def record_arity({:record, {_label, fields}}), do: length(fields)

  # Convenience Functions

  @doc """
  Converts native Elixir terms to Preserves Values.

  ## Conversion Rules

  - `true`/`false` -> boolean
  - integers -> integer
  - floats -> double
  - UTF-8 strings -> string
  - non-UTF-8 binaries -> binary
  - atoms -> symbol (except true/false/nil)
  - lists -> sequence
  - MapSet -> set
  - maps -> dictionary
  - tuples -> record with symbol label based on size (e.g., `tuple2`, `tuple3`)

  ## Examples

      iex> Absynthe.Preserves.Value.from_native(true)
      {:boolean, true}

      iex> Absynthe.Preserves.Value.from_native(42)
      {:integer, 42}

      iex> Absynthe.Preserves.Value.from_native(3.14)
      {:double, 3.14}

      iex> Absynthe.Preserves.Value.from_native("hello")
      {:string, "hello"}

      iex> Absynthe.Preserves.Value.from_native(:name)
      {:symbol, "name"}

      iex> Absynthe.Preserves.Value.from_native([1, 2, 3])
      {:sequence, [{:integer, 1}, {:integer, 2}, {:integer, 3}]}

      iex> Absynthe.Preserves.Value.from_native(MapSet.new([1, 2]))
      {:set, MapSet.new([{:integer, 1}, {:integer, 2}])}

      iex> Absynthe.Preserves.Value.from_native(%{name: "Alice"})
      {:dictionary, %{{:symbol, "name"} => {:string, "Alice"}}}

      iex> Absynthe.Preserves.Value.from_native({1, 2})
      {:record, {{:symbol, "tuple2"}, [{:integer, 1}, {:integer, 2}]}}
  """
  @spec from_native(term()) :: t()
  def from_native(true), do: boolean(true)
  def from_native(false), do: boolean(false)
  def from_native(nil), do: symbol("nil")
  def from_native(value) when is_integer(value), do: integer(value)
  def from_native(value) when is_float(value), do: double(value)

  def from_native(value) when is_binary(value) do
    if String.valid?(value) do
      string(value)
    else
      binary(value)
    end
  end

  def from_native(value) when is_atom(value) do
    symbol(Atom.to_string(value))
  end

  def from_native(value) when is_list(value) do
    sequence(Enum.map(value, &from_native/1))
  end

  def from_native(%MapSet{} = value) do
    set(Enum.map(value, &from_native/1))
  end

  def from_native(value) when is_map(value) do
    pairs =
      Enum.map(value, fn {k, v} ->
        {from_native(k), from_native(v)}
      end)

    dictionary(pairs)
  end

  def from_native(value) when is_tuple(value) do
    size = tuple_size(value)
    label = symbol("tuple#{size}")
    fields = value |> Tuple.to_list() |> Enum.map(&from_native/1)
    record(label, fields)
  end
end
