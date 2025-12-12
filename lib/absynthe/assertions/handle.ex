defmodule Absynthe.Assertions.Handle do
  @moduledoc """
  Globally unique identifier for assertions.

  Handles uniquely identify assertions across the entire actor system by
  combining an actor ID with a locally-monotonic counter. This ensures that
  two actors asserting to the same dataspace never collide, even if they
  happen to use the same local counter value.

  ## Structure

  A handle contains:
  - `actor_id` - The ID of the actor that created the assertion
  - `id` - A locally-unique counter within that actor

  The combination `{actor_id, id}` is globally unique, allowing handles to be
  safely used as keys in dataspaces shared by multiple actors.

  ## Examples

      iex> handle = Absynthe.Assertions.Handle.new(:my_actor, 1)
      iex> Absynthe.Assertions.Handle.to_integer(handle)
      1
      iex> Absynthe.Assertions.Handle.actor_id(handle)
      :my_actor

  Handles can be compared and used as map keys:

      iex> h1 = Absynthe.Assertions.Handle.new(:actor_a, 1)
      iex> h2 = Absynthe.Assertions.Handle.new(:actor_b, 1)
      iex> h1 == h2
      false
      iex> map = %{h1 => :value}
      iex> Map.has_key?(map, h1)
      true

  """

  defstruct [:actor_id, :id]

  @type t :: %__MODULE__{actor_id: term(), id: non_neg_integer()}

  @doc """
  Creates a new handle from an actor ID and counter value.

  The counter should be a non-negative integer, typically generated
  monotonically by the actor creating the assertion. The actor_id identifies
  which actor created this handle, ensuring global uniqueness.

  ## Parameters

    - `actor_id` - The ID of the actor creating the handle
    - `counter` - A non-negative integer used as the handle's local identifier

  ## Examples

      iex> handle = Absynthe.Assertions.Handle.new(:my_actor, 42)
      %Absynthe.Assertions.Handle{actor_id: :my_actor, id: 42}

  """
  @spec new(term(), non_neg_integer()) :: t()
  def new(actor_id, counter) when is_integer(counter) and counter >= 0 do
    %__MODULE__{actor_id: actor_id, id: counter}
  end

  @doc """
  Extracts the numeric (local) value from a handle.

  This is primarily useful for debugging and logging purposes.

  ## Parameters

    - `handle` - The handle to extract the value from

  ## Examples

      iex> handle = Absynthe.Assertions.Handle.new(:my_actor, 123)
      iex> Absynthe.Assertions.Handle.to_integer(handle)
      123

  """
  @spec to_integer(t()) :: non_neg_integer()
  def to_integer(%__MODULE__{id: id}), do: id

  @doc """
  Extracts the actor ID from a handle.

  This identifies which actor originally created the assertion.

  ## Parameters

    - `handle` - The handle to extract the actor ID from

  ## Examples

      iex> handle = Absynthe.Assertions.Handle.new(:my_actor, 123)
      iex> Absynthe.Assertions.Handle.actor_id(handle)
      :my_actor

  """
  @spec actor_id(t()) :: term()
  def actor_id(%__MODULE__{actor_id: actor_id}), do: actor_id
end
