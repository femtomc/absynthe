defmodule Absynthe do
  @moduledoc """
  Absynthe - Syndicated Actor Model for Elixir

  Absynthe is an implementation of Tony Garnock-Jones' Syndicated Actor Model,
  providing a powerful concurrency model based on actors, assertions, and dataspaces.

  ## Core Concepts

  ### Actors
  Actors are independent concurrent processes that communicate through assertions
  and messages. Each actor maintains its own state and executes turns atomically.

  ### Assertions
  Assertions are claims that actors make about the world. Unlike messages, assertions
  persist until explicitly retracted. Multiple actors can make the same assertion,
  and the assertion remains present until all have retracted it (reference counting).

  ### Dataspaces
  Dataspaces are coordination spaces where actors publish assertions and subscribe
  to patterns. The dataspace routes assertions to interested observers and manages
  the assertion lifecycle.

  ### Turns
  Turns are atomic transaction units. All changes within a turn either commit
  together or are discarded. This provides strong consistency guarantees.

  ### Facets
  Facets represent conversational contexts within an actor. They provide scoping
  for assertions and lifecycle management through fate-sharing.

  ## Quick Start

      # Create Preserves values
      person = Absynthe.record(:Person, ["Alice", 30])

      # Encode/decode data
      binary = Absynthe.encode!(person)
      decoded = Absynthe.decode!(binary)

      # Create a dataspace
      dataspace = Absynthe.new_dataspace()

  ## Modules

  - `Absynthe.Preserves` - Data serialization format
  - `Absynthe.Core.Actor` - Actor process implementation
  - `Absynthe.Core.Turn` - Transaction handling
  - `Absynthe.Core.Facet` - Conversational scopes
  - `Absynthe.Core.Entity` - Entity protocol
  - `Absynthe.Dataspace.Dataspace` - Assertion routing
  - `Absynthe.Dataflow.Field` - Reactive cells
  - `Absynthe.Dataflow.Graph` - Dependency tracking

  ## References

  - [Syndicate Lang](https://syndicate-lang.org/)
  - [PhD Dissertation](https://syndicate-lang.org/tonyg-dissertation/html/)
  - [Preserves Spec](https://preserves.gitlab.io/preserves/preserves.html)
  """

  alias Absynthe.Preserves.Value
  alias Absynthe.Core.Turn

  # ============================================================================
  # Preserves API
  # ============================================================================

  @doc """
  Creates a Preserves symbol value.

  Symbols are atomic identifiers, similar to atoms in Elixir. They are
  commonly used as record labels and dictionary keys.

  ## Parameters

  - `name` - String or atom name for the symbol

  ## Examples

      iex> Absynthe.symbol("Person")
      {:symbol, "Person"}

      iex> Absynthe.symbol(:greeting)
      {:symbol, "greeting"}
  """
  @spec symbol(String.t() | atom()) :: Value.t()
  def symbol(name) when is_atom(name), do: {:symbol, Atom.to_string(name)}
  def symbol(name) when is_binary(name), do: {:symbol, name}

  @doc """
  Creates a Preserves record value.

  Records are labeled compound values, similar to tagged tuples. They have
  a label (typically a symbol) and a list of fields.

  ## Parameters

  - `label` - The record label (symbol or any Preserves value)
  - `fields` - List of field values

  ## Examples

      iex> Absynthe.record(:Person, ["Alice", 30])
      {:record, {{:symbol, "Person"}, [{:string, "Alice"}, {:integer, 30}]}}

      iex> Absynthe.record(Absynthe.symbol("Point"), [1, 2])
      {:record, {{:symbol, "Point"}, [{:integer, 1}, {:integer, 2}]}}
  """
  @spec record(atom() | String.t() | Value.t(), [term()]) :: Value.t()
  def record(label, fields) when is_atom(label) do
    record(symbol(label), fields)
  end

  def record(label, fields) when is_binary(label) do
    record(symbol(label), fields)
  end

  def record({:symbol, _} = label, fields) do
    {:record, {label, Enum.map(fields, &to_preserves/1)}}
  end

  def record(label, fields) do
    {:record, {label, Enum.map(fields, &to_preserves/1)}}
  end

  @doc """
  Encodes a Preserves value to binary format.

  ## Parameters

  - `value` - The Preserves value to encode
  - `format` - The encoding format (`:binary` or `:text`, default: `:binary`)

  ## Examples

      iex> Absynthe.encode!({:integer, 42})
      <<0xB0, 0x01, 0x2A>>

      iex> Absynthe.encode!({:boolean, true})
      <<0x81>>
  """
  @spec encode!(Value.t(), :binary | :text) :: binary()
  def encode!(value, format \\ :binary)

  def encode!(value, :binary) do
    Absynthe.Preserves.Encoder.Binary.encode!(value)
  end

  def encode!(value, :text) do
    Absynthe.Preserves.Encoder.Text.encode!(value)
  end

  @doc """
  Decodes binary data to a Preserves value.

  ## Parameters

  - `data` - Binary data to decode
  - `format` - The encoding format (`:binary` or `:text`, default: `:binary`)

  ## Examples

      iex> binary = Absynthe.encode!({:integer, 42})
      iex> Absynthe.decode!(binary)
      {:integer, 42}
  """
  @spec decode!(binary(), :binary | :text) :: Value.t()
  def decode!(data, format \\ :binary)

  def decode!(data, :binary) do
    Absynthe.Preserves.Decoder.Binary.decode_all!(data)
  end

  def decode!(data, :text) do
    case Absynthe.Preserves.Decoder.Text.decode(data) do
      {:ok, value, _rest} -> value
      {:error, reason, _pos} -> raise "Decode error: #{reason}"
    end
  end

  # ============================================================================
  # Dataspace API
  # ============================================================================

  @doc """
  Creates a new empty dataspace.

  A dataspace is a coordination space where actors publish assertions and
  subscribe to patterns. It routes assertions to interested observers.

  ## Examples

      iex> dataspace = Absynthe.new_dataspace()
      iex> Absynthe.Dataspace.Dataspace.assertions(dataspace)
      []
  """
  @spec new_dataspace(keyword()) :: Absynthe.Dataspace.Dataspace.t()
  def new_dataspace(opts \\ []) do
    Absynthe.Dataspace.Dataspace.new(opts)
  end

  # ============================================================================
  # DSL Macros
  # ============================================================================

  @doc """
  Creates an assertion action within a turn.

  Use this within a turn to assert a value. The assertion persists until
  explicitly retracted.

  ## Parameters

  - `target` - The entity ref to receive the assertion
  - `assertion` - The value to assert

  ## Examples

      turn = Absynthe.assert_value(turn, dataspace_ref, {:string, "hello"})
  """
  @spec assert_value(Turn.t(), Absynthe.Core.Ref.t(), Value.t()) :: Turn.t()
  def assert_value(%Turn{} = turn, target, assertion) do
    handle = Absynthe.Assertions.Handle.new(:erlang.unique_integer([:positive]))
    action = Absynthe.Protocol.Event.assert(target, assertion, handle)
    Turn.add_action(turn, action)
  end

  @doc """
  Creates a retract action within a turn.

  Use this within a turn to retract a previously asserted value by its handle.

  ## Parameters

  - `turn` - The current turn
  - `target` - The entity ref that holds the assertion
  - `handle` - The handle of the assertion to retract

  ## Examples

      turn = Absynthe.retract_value(turn, dataspace_ref, handle)
  """
  @spec retract_value(Turn.t(), Absynthe.Core.Ref.t(), Absynthe.Assertions.Handle.t()) :: Turn.t()
  def retract_value(%Turn{} = turn, target, handle) do
    action = Absynthe.Protocol.Event.retract(target, handle)
    Turn.add_action(turn, action)
  end

  @doc """
  Creates a message action within a turn.

  Use this within a turn to send a message to an entity. Unlike assertions,
  messages are one-shot and do not persist.

  ## Parameters

  - `turn` - The current turn
  - `target` - The entity ref to receive the message
  - `message` - The message value to send

  ## Examples

      turn = Absynthe.send_message(turn, actor_ref, {:string, "ping"})
  """
  @spec send_message(Turn.t(), Absynthe.Core.Ref.t(), Value.t()) :: Turn.t()
  def send_message(%Turn{} = turn, target, message) do
    action = Absynthe.Protocol.Event.message(target, message)
    Turn.add_action(turn, action)
  end

  @doc """
  Creates an Observe assertion for subscribing to patterns.

  An Observe assertion tells the dataspace to notify the observer whenever
  assertions matching the pattern are published or retracted.

  ## Parameters

  - `pattern` - The pattern to match against
  - `observer_ref` - The entity ref that should receive notifications

  ## Examples

      # Subscribe to all Person records
      observe = Absynthe.observe(
        Absynthe.record(:Observe, [
          Absynthe.record(:Person, [:_, :_]),  # Pattern with wildcards
          observer_ref
        ])
      )
  """
  @spec observe(Value.t(), Absynthe.Core.Ref.t()) :: Value.t()
  def observe(pattern, observer_ref) do
    record(:Observe, [pattern, {:embedded, observer_ref}])
  end

  @doc """
  Creates a wildcard pattern element.

  The wildcard matches any value at that position in the pattern.

  ## Examples

      # Match any Person record regardless of fields
      pattern = Absynthe.record(:Person, [Absynthe.wildcard(), Absynthe.wildcard()])
  """
  @spec wildcard() :: Value.t()
  def wildcard do
    {:symbol, "_"}
  end

  @doc """
  Creates a capture pattern element.

  Captures extract the value at that position during pattern matching.

  ## Parameters

  - `name` - Optional name for the capture (default: anonymous)

  ## Examples

      # Capture the name field from Person records
      pattern = Absynthe.record(:Person, [Absynthe.capture(:name), Absynthe.wildcard()])
  """
  @spec capture(atom() | nil) :: Value.t()
  def capture(name \\ nil) do
    if name do
      {:symbol, "$#{name}"}
    else
      {:symbol, "$"}
    end
  end

  # ============================================================================
  # Private Helpers
  # ============================================================================

  # Converts Elixir values to Preserves values
  defp to_preserves(value) when is_integer(value), do: {:integer, value}
  defp to_preserves(value) when is_float(value), do: {:double, value}
  defp to_preserves(value) when is_binary(value), do: {:string, value}
  defp to_preserves(value) when is_boolean(value), do: {:boolean, value}
  defp to_preserves(value) when is_atom(value), do: {:symbol, Atom.to_string(value)}
  defp to_preserves(value) when is_list(value), do: {:sequence, Enum.map(value, &to_preserves/1)}
  defp to_preserves(%MapSet{} = value), do: {:set, MapSet.new(value, &to_preserves/1)}

  defp to_preserves(%{} = value) do
    {:dictionary,
     Map.new(value, fn {k, v} ->
       {to_preserves(k), to_preserves(v)}
     end)}
  end

  # Already a Preserves value
  defp to_preserves({:integer, _} = v), do: v
  defp to_preserves({:double, _} = v), do: v
  defp to_preserves({:string, _} = v), do: v
  defp to_preserves({:binary, _} = v), do: v
  defp to_preserves({:boolean, _} = v), do: v
  defp to_preserves({:symbol, _} = v), do: v
  defp to_preserves({:record, _} = v), do: v
  defp to_preserves({:sequence, _} = v), do: v
  defp to_preserves({:set, _} = v), do: v
  defp to_preserves({:dictionary, _} = v), do: v
  defp to_preserves({:embedded, _} = v), do: v
end
