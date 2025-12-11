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
  alias Absynthe.Core.{Actor, Turn, Ref}
  alias Absynthe.Assertions.Handle

  # ============================================================================
  # Actor API
  # ============================================================================

  @doc """
  Starts a new actor process.

  Actors are the fundamental unit of computation in the Syndicated Actor Model.
  Each actor is an isolated process that hosts entities, processes events through
  turns, and manages assertions.

  ## Options

  - `:id` - Unique identifier for the actor (default: `self()`)
  - `:name` - Optional registered name for the GenServer

  ## Examples

      # Start an anonymous actor
      {:ok, actor} = Absynthe.start_actor(id: :my_actor)

      # Start a named actor
      {:ok, actor} = Absynthe.start_actor(id: :my_actor, name: MyApp.MainActor)

  ## Returns

  - `{:ok, pid}` - Successfully started actor
  - `{:error, reason}` - Failed to start
  """
  @spec start_actor(keyword()) :: GenServer.on_start()
  defdelegate start_actor(opts \\ []), to: Actor, as: :start_link

  @doc """
  Spawns a new entity within an actor's facet.

  Entities are the computational objects within actors. They implement the
  `Absynthe.Core.Entity` protocol to handle messages, assertions, and sync events.

  ## Parameters

  - `actor` - The actor PID or registered name
  - `facet_id` - The facet to spawn the entity in (use `:root` for the root facet)
  - `entity` - A struct implementing the `Absynthe.Core.Entity` protocol

  ## Examples

      defmodule Counter do
        defstruct count: 0
      end

      defimpl Absynthe.Core.Entity, for: Counter do
        def on_message(%{count: n} = counter, {:increment, amount}, turn) do
          {%{counter | count: n + amount}, turn}
        end
        def on_publish(entity, _assertion, _handle, turn), do: {entity, turn}
        def on_retract(entity, _handle, turn), do: {entity, turn}
        def on_sync(entity, _peer, turn), do: {entity, turn}
      end

      {:ok, actor} = Absynthe.start_actor(id: :counter_actor)
      {:ok, ref} = Absynthe.spawn_entity(actor, :root, %Counter{count: 0})

  ## Returns

  - `{:ok, ref}` - The entity reference for sending messages/assertions
  - `{:error, reason}` - Failed to spawn
  """
  @spec spawn_entity(GenServer.server(), Actor.facet_id(), struct()) ::
          {:ok, Ref.t()} | {:error, term()}
  defdelegate spawn_entity(actor, facet_id, entity), to: Actor

  @doc """
  Sends a message to an entity.

  Messages are ephemeral, one-shot communications. They are delivered once
  and do not persist. Use messages for commands and requests.

  ## Parameters

  - `actor` - The actor PID (for local entities) or any actor (for routing)
  - `ref` - The target entity reference
  - `message` - The message value (Preserves value)

  ## Examples

      Absynthe.send_to(actor, counter_ref, {:increment, 5})
      Absynthe.send_to(actor, greeter_ref, {:string, "Hello!"})

  ## Returns

  - `:ok` - Message queued for delivery
  """
  @spec send_to(GenServer.server(), Ref.t(), Value.t()) :: :ok
  defdelegate send_to(actor, ref, message), to: Actor, as: :send_message

  @doc """
  Asserts a value to an entity.

  Assertions are persistent facts that remain until explicitly retracted.
  They are the primary communication mechanism for dataspaces.

  ## Parameters

  - `actor` - The actor making the assertion
  - `ref` - The target entity reference (often a dataspace)
  - `assertion` - The value to assert

  ## Examples

      {:ok, handle} = Absynthe.assert_to(actor, dataspace_ref,
        Absynthe.record(:Status, [:online, "user-123"]))

      # Later, retract it
      :ok = Absynthe.retract_from(actor, handle)

  ## Returns

  - `{:ok, handle}` - Handle for later retraction
  """
  @spec assert_to(GenServer.server(), Ref.t(), Value.t()) :: {:ok, Handle.t()}
  defdelegate assert_to(actor, ref, assertion), to: Actor, as: :assert

  @doc """
  Retracts a previously made assertion.

  ## Parameters

  - `actor` - The actor that made the assertion
  - `handle` - The handle returned from `assert_to/3`

  ## Examples

      {:ok, handle} = Absynthe.assert_to(actor, ref, value)
      :ok = Absynthe.retract_from(actor, handle)

  ## Returns

  - `:ok` - Assertion retracted
  - `{:error, :not_found}` - Handle not found
  """
  @spec retract_from(GenServer.server(), Handle.t()) :: :ok | {:error, term()}
  defdelegate retract_from(actor, handle), to: Actor, as: :retract

  @doc """
  Creates a child facet within an actor.

  Facets provide scoped resource management. When a facet terminates,
  all its entities and child facets are cleaned up (fate-sharing).

  ## Parameters

  - `actor` - The actor PID
  - `parent_id` - The parent facet ID
  - `facet_id` - The new facet's ID

  ## Examples

      :ok = Absynthe.create_facet(actor, :root, :conversation_1)
      {:ok, ref} = Absynthe.spawn_entity(actor, :conversation_1, entity)

  ## Returns

  - `:ok` - Facet created
  - `{:error, reason}` - Failed to create
  """
  @spec create_facet(GenServer.server(), Actor.facet_id(), Actor.facet_id()) ::
          :ok | {:error, term()}
  defdelegate create_facet(actor, parent_id, facet_id), to: Actor

  @doc """
  Terminates a facet and all its contents.

  This implements fate-sharing: all entities and child facets within
  the terminated facet are also cleaned up.

  ## Parameters

  - `actor` - The actor PID
  - `facet_id` - The facet to terminate

  ## Returns

  - `:ok` - Facet terminated
  - `{:error, reason}` - Failed to terminate
  """
  @spec terminate_facet(GenServer.server(), Actor.facet_id()) :: :ok | {:error, term()}
  defdelegate terminate_facet(actor, facet_id), to: Actor

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

  - `pattern` - The pattern to match against (use `wildcard/0` and `capture/1` for matching)
  - `observer_ref` - The entity ref that should receive notifications

  ## Examples

      # Subscribe to all Person records
      observe_assertion = Absynthe.observe(
        Absynthe.record(:Person, [Absynthe.wildcard(), Absynthe.wildcard()]),
        observer_ref
      )

      # Assert the observation to the dataspace
      {:ok, _handle} = Absynthe.assert_to(actor, dataspace_ref, observe_assertion)
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
