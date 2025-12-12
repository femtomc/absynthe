defmodule Absynthe.Protocol.Event do
  @moduledoc """
  Event types for the Syndicated Actor Model.

  This module defines the fundamental event types that actors exchange
  in the Syndicated Actor Model:

  - `Assert` - publish an assertion to a ref
  - `Retract` - remove a previously published assertion
  - `Message` - send a one-shot message to a ref
  - `Sync` - synchronization barrier for consistency
  - `Spawn` - spawn a new entity within a turn

  ## Events Overview

  ### Assert
  Represents the publication of an assertion to a target entity. Assertions
  are stateful - they persist until explicitly retracted. Each assertion is
  tagged with a handle for later retraction.

  ### Retract
  Removes a previously published assertion, identified by its handle.

  ### Message
  Sends a one-shot, stateless message to a target entity. Unlike assertions,
  messages are delivered once and do not persist.

  ### Sync
  A synchronization barrier that ensures all pending events have been
  processed. The peer ref is notified when synchronization is complete.

  ### Spawn
  Spawns a new entity transactionally within a turn. The spawn is committed
  atomically with all other turn actions, enabling reliable creation of new
  entities within conversations.

  ## Examples

      # Create an assertion event
      assertion = {:string, "Hello"}
      ref = %Absynthe.Core.Ref{actor_id: 1, entity_id: 2}
      handle = %Absynthe.Assertions.Handle{id: 1}
      event = Absynthe.Protocol.Event.assert(ref, assertion, handle)

      # Create a message event
      message = {:record, {{:symbol, "Ping"}, []}}
      event = Absynthe.Protocol.Event.message(ref, message)

      # Check event type
      Absynthe.Protocol.Event.assert?(event)  # => true
      Absynthe.Protocol.Event.message?(event) # => false
  """

  defmodule Assert do
    @moduledoc """
    Assert an assertion to a target ref.

    An assertion represents a fact that is published to an entity and persists
    until explicitly retracted. The handle uniquely identifies this assertion
    for later retraction.

    When an observer's pattern includes captures (e.g., `$age` or `{:capture, :age}`),
    the captured values are included in the `captures` field as a list in the order
    they appear in the pattern.
    """

    @enforce_keys [:ref, :assertion, :handle]
    defstruct [:ref, :assertion, :handle, captures: []]

    @type t :: %__MODULE__{
            ref: Absynthe.Core.Ref.t(),
            assertion: Absynthe.Preserves.Value.t(),
            handle: Absynthe.Assertions.Handle.t(),
            captures: [Absynthe.Preserves.Value.t()]
          }
  end

  defmodule Retract do
    @moduledoc """
    Retract a previously asserted assertion.

    The retraction is identified by the handle that was used when the
    assertion was originally published. The ref identifies the entity
    that should process this retraction.
    """

    @enforce_keys [:ref, :handle]
    defstruct [:ref, :handle]

    @type t :: %__MODULE__{
            ref: Absynthe.Core.Ref.t(),
            handle: Absynthe.Assertions.Handle.t()
          }
  end

  defmodule Message do
    @moduledoc """
    Send a message to a target ref.

    Messages are one-shot, stateless communications. Unlike assertions,
    they do not persist and are processed exactly once.
    """

    @enforce_keys [:ref, :body]
    defstruct [:ref, :body]

    @type t :: %__MODULE__{
            ref: Absynthe.Core.Ref.t(),
            body: Absynthe.Preserves.Value.t()
          }
  end

  defmodule Sync do
    @moduledoc """
    Synchronization barrier.

    A sync event ensures that all events sent to the target ref before
    the sync have been processed. When processing is complete, the peer
    ref is notified.
    """

    @enforce_keys [:ref, :peer]
    defstruct [:ref, :peer]

    @type t :: %__MODULE__{
            ref: Absynthe.Core.Ref.t(),
            peer: Absynthe.Core.Ref.t()
          }
  end

  defmodule Spawn do
    @moduledoc """
    Spawn a new entity within a turn.

    This event allows entities to spawn new entities transactionally as part of
    a turn's execution. The spawn is committed atomically with all other turn
    actions, enabling reliable creation of new entities within conversations.

    ## Fields

    - `facet_id` - The facet to spawn the entity in
    - `entity` - The entity to spawn (must implement Absynthe.Core.Entity protocol)
    - `callback` - Optional callback function to receive the spawned entity ref.
      The callback is invoked synchronously during turn commit. If the callback
      raises an error, it will be logged but will not prevent the spawn from
      completing.
    """

    @enforce_keys [:facet_id, :entity]
    defstruct [:facet_id, :entity, :callback]

    @type t :: %__MODULE__{
            facet_id: term(),
            entity: term(),
            callback: (Absynthe.Core.Ref.t() -> any()) | nil
          }
  end

  @type t :: Assert.t() | Retract.t() | Message.t() | Sync.t() | Spawn.t()

  @doc """
  Creates an Assert event.

  ## Parameters

    - `ref` - The target entity reference
    - `assertion` - The Preserves value to assert
    - `handle` - Unique handle for later retraction
    - `captures` - (optional) List of captured values from pattern matching

  ## Examples

      iex> ref = %Absynthe.Core.Ref{actor_id: 1, entity_id: 2}
      iex> handle = %Absynthe.Assertions.Handle{id: 1}
      iex> event = Absynthe.Protocol.Event.assert(ref, {:string, "test"}, handle)
      iex> Absynthe.Protocol.Event.assert?(event)
      true

      # With captures
      iex> captures = [{:integer, 30}]
      iex> event = Absynthe.Protocol.Event.assert(ref, assertion, handle, captures)
      iex> event.captures
      [{:integer, 30}]
  """
  @spec assert(
          Absynthe.Core.Ref.t(),
          Absynthe.Preserves.Value.t(),
          Absynthe.Assertions.Handle.t(),
          [Absynthe.Preserves.Value.t()]
        ) :: Assert.t()
  def assert(ref, assertion, handle, captures \\ []) do
    %Assert{
      ref: ref,
      assertion: assertion,
      handle: handle,
      captures: captures
    }
  end

  @doc """
  Creates a Retract event.

  ## Parameters

    - `ref` - The target entity reference
    - `handle` - The handle of the assertion to retract

  ## Examples

      iex> ref = %Absynthe.Core.Ref{actor_id: 1, entity_id: 2}
      iex> handle = %Absynthe.Assertions.Handle{id: 1}
      iex> event = Absynthe.Protocol.Event.retract(ref, handle)
      iex> Absynthe.Protocol.Event.retract?(event)
      true
  """
  @spec retract(Absynthe.Core.Ref.t(), Absynthe.Assertions.Handle.t()) :: Retract.t()
  def retract(ref, handle) do
    %Retract{
      ref: ref,
      handle: handle
    }
  end

  @doc """
  Creates a Message event.

  ## Parameters

    - `ref` - The target entity reference
    - `body` - The Preserves value to send as a message

  ## Examples

      iex> ref = %Absynthe.Core.Ref{actor_id: 1, entity_id: 2}
      iex> event = Absynthe.Protocol.Event.message(ref, {:string, "ping"})
      iex> Absynthe.Protocol.Event.message?(event)
      true
  """
  @spec message(Absynthe.Core.Ref.t(), Absynthe.Preserves.Value.t()) :: Message.t()
  def message(ref, body) do
    %Message{
      ref: ref,
      body: body
    }
  end

  @doc """
  Creates a Sync event.

  ## Parameters

    - `ref` - The target entity reference to synchronize with
    - `peer` - The peer reference to notify when sync is complete

  ## Examples

      iex> ref = %Absynthe.Core.Ref{actor_id: 1, entity_id: 2}
      iex> peer = %Absynthe.Core.Ref{actor_id: 1, entity_id: 3}
      iex> event = Absynthe.Protocol.Event.sync(ref, peer)
      iex> Absynthe.Protocol.Event.sync?(event)
      true
  """
  @spec sync(Absynthe.Core.Ref.t(), Absynthe.Core.Ref.t()) :: Sync.t()
  def sync(ref, peer) do
    %Sync{
      ref: ref,
      peer: peer
    }
  end

  @doc """
  Returns true if the event is an Assert event.

  ## Examples

      iex> event = %Absynthe.Protocol.Event.Assert{ref: ref, assertion: value, handle: handle}
      iex> Absynthe.Protocol.Event.assert?(event)
      true

      iex> event = %Absynthe.Protocol.Event.Message{ref: ref, body: value}
      iex> Absynthe.Protocol.Event.assert?(event)
      false
  """
  @spec assert?(t()) :: boolean()
  def assert?(%Assert{}), do: true
  def assert?(_), do: false

  @doc """
  Returns true if the event is a Retract event.

  ## Examples

      iex> event = %Absynthe.Protocol.Event.Retract{ref: ref, handle: handle}
      iex> Absynthe.Protocol.Event.retract?(event)
      true

      iex> event = %Absynthe.Protocol.Event.Message{ref: ref, body: value}
      iex> Absynthe.Protocol.Event.retract?(event)
      false
  """
  @spec retract?(t()) :: boolean()
  def retract?(%Retract{}), do: true
  def retract?(_), do: false

  @doc """
  Returns true if the event is a Message event.

  ## Examples

      iex> event = %Absynthe.Protocol.Event.Message{ref: ref, body: value}
      iex> Absynthe.Protocol.Event.message?(event)
      true

      iex> event = %Absynthe.Protocol.Event.Assert{ref: ref, assertion: value, handle: handle}
      iex> Absynthe.Protocol.Event.message?(event)
      false
  """
  @spec message?(t()) :: boolean()
  def message?(%Message{}), do: true
  def message?(_), do: false

  @doc """
  Returns true if the event is a Sync event.

  ## Examples

      iex> event = %Absynthe.Protocol.Event.Sync{ref: ref, peer: peer}
      iex> Absynthe.Protocol.Event.sync?(event)
      true

      iex> event = %Absynthe.Protocol.Event.Message{ref: ref, body: value}
      iex> Absynthe.Protocol.Event.sync?(event)
      false
  """
  @spec sync?(t()) :: boolean()
  def sync?(%Sync{}), do: true
  def sync?(_), do: false

  @doc """
  Creates a Spawn event.

  ## Parameters

    - `facet_id` - The facet to spawn the entity in
    - `entity` - The entity to spawn
    - `callback` - (optional) Callback function to receive the spawned ref

  ## Examples

      iex> entity = %MyEntity{}
      iex> event = Absynthe.Protocol.Event.spawn(:root, entity)
      iex> Absynthe.Protocol.Event.spawn?(event)
      true

      # With callback
      iex> callback = fn ref -> IO.puts("Spawned: \#{inspect(ref)}") end
      iex> event = Absynthe.Protocol.Event.spawn(:root, entity, callback)
      iex> event.callback
      #Function<...>
  """
  @spec spawn(term(), term(), (Absynthe.Core.Ref.t() -> any()) | nil) :: Spawn.t()
  def spawn(facet_id, entity, callback \\ nil) do
    %Spawn{
      facet_id: facet_id,
      entity: entity,
      callback: callback
    }
  end

  @doc """
  Returns true if the event is a Spawn event.

  ## Examples

      iex> event = %Absynthe.Protocol.Event.Spawn{facet_id: :root, entity: %{}}
      iex> Absynthe.Protocol.Event.spawn?(event)
      true

      iex> event = %Absynthe.Protocol.Event.Message{ref: ref, body: value}
      iex> Absynthe.Protocol.Event.spawn?(event)
      false
  """
  @spec spawn?(t()) :: boolean()
  def spawn?(%Spawn{}), do: true
  def spawn?(_), do: false

  @doc """
  Extracts the target ref from any event type.

  Most event types have a `ref` field that identifies the target entity.
  This function provides a convenient way to access it. Returns `nil` for
  Spawn events which don't have a target ref (they operate on facets).

  ## Examples

      iex> ref = %Absynthe.Core.Ref{actor_id: 1, entity_id: 2}
      iex> event = Absynthe.Protocol.Event.message(ref, {:string, "test"})
      iex> Absynthe.Protocol.Event.target_ref(event)
      %Absynthe.Core.Ref{actor_id: 1, entity_id: 2}

      iex> event = %Absynthe.Protocol.Event.Spawn{facet_id: :root, entity: %{}}
      iex> Absynthe.Protocol.Event.target_ref(event)
      nil
  """
  @spec target_ref(t()) :: Absynthe.Core.Ref.t() | nil
  def target_ref(%Assert{ref: ref}), do: ref
  def target_ref(%Retract{ref: ref}), do: ref
  def target_ref(%Message{ref: ref}), do: ref
  def target_ref(%Sync{ref: ref}), do: ref
  def target_ref(%Spawn{}), do: nil
end
