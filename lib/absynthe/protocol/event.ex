defmodule Absynthe.Protocol.Event do
  @moduledoc """
  Event types for the Syndicated Actor Model.

  This module defines the four fundamental event types that actors exchange
  in the Syndicated Actor Model:

  - `Assert` - publish an assertion to a ref
  - `Retract` - remove a previously published assertion
  - `Message` - send a one-shot message to a ref
  - `Sync` - synchronization barrier for consistency

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
    """

    @enforce_keys [:ref, :assertion, :handle]
    defstruct [:ref, :assertion, :handle]

    @type t :: %__MODULE__{
            ref: Absynthe.Core.Ref.t(),
            assertion: Absynthe.Preserves.Value.t(),
            handle: Absynthe.Assertions.Handle.t()
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

  @type t :: Assert.t() | Retract.t() | Message.t() | Sync.t()

  @doc """
  Creates an Assert event.

  ## Parameters

    - `ref` - The target entity reference
    - `assertion` - The Preserves value to assert
    - `handle` - Unique handle for later retraction

  ## Examples

      iex> ref = %Absynthe.Core.Ref{actor_id: 1, entity_id: 2}
      iex> handle = %Absynthe.Assertions.Handle{id: 1}
      iex> event = Absynthe.Protocol.Event.assert(ref, {:string, "test"}, handle)
      iex> Absynthe.Protocol.Event.assert?(event)
      true
  """
  @spec assert(
          Absynthe.Core.Ref.t(),
          Absynthe.Preserves.Value.t(),
          Absynthe.Assertions.Handle.t()
        ) :: Assert.t()
  def assert(ref, assertion, handle) do
    %Assert{
      ref: ref,
      assertion: assertion,
      handle: handle
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
  Extracts the target ref from any event type.

  All event types have a `ref` field that identifies the target entity.
  This function provides a convenient way to access it.

  ## Examples

      iex> ref = %Absynthe.Core.Ref{actor_id: 1, entity_id: 2}
      iex> event = Absynthe.Protocol.Event.message(ref, {:string, "test"})
      iex> Absynthe.Protocol.Event.target_ref(event)
      %Absynthe.Core.Ref{actor_id: 1, entity_id: 2}
  """
  @spec target_ref(t()) :: Absynthe.Core.Ref.t()
  def target_ref(%Assert{ref: ref}), do: ref
  def target_ref(%Retract{ref: ref}), do: ref
  def target_ref(%Message{ref: ref}), do: ref
  def target_ref(%Sync{ref: ref}), do: ref
end
