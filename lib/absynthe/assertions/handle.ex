defmodule Absynthe.Assertions.Handle do
  @moduledoc """
  Unique identifier for assertions.

  Handles are generated monotonically within each actor and used to track
  and retract assertions. Each handle contains a unique numeric identifier
  that is actor-local and increases monotonically.

  ## Examples

      iex> handle = Absynthe.Assertions.Handle.new(1)
      iex> Absynthe.Assertions.Handle.to_integer(handle)
      1

  Handles can be compared and used as map keys:

      iex> h1 = Absynthe.Assertions.Handle.new(1)
      iex> h2 = Absynthe.Assertions.Handle.new(2)
      iex> h1 == h2
      false
      iex> map = %{h1 => :value}
      iex> Map.has_key?(map, h1)
      true

  """

  defstruct [:id]

  @type t :: %__MODULE__{id: non_neg_integer()}

  @doc """
  Creates a new handle from a counter value.

  The counter should be a non-negative integer, typically generated
  monotonically by the actor creating the assertion.

  ## Parameters

    - `counter` - A non-negative integer used as the handle's unique identifier

  ## Examples

      iex> handle = Absynthe.Assertions.Handle.new(42)
      %Absynthe.Assertions.Handle{id: 42}

  """
  @spec new(non_neg_integer()) :: t()
  def new(counter) when is_integer(counter) and counter >= 0 do
    %__MODULE__{id: counter}
  end

  @doc """
  Extracts the numeric value from a handle.

  This is primarily useful for debugging and logging purposes.

  ## Parameters

    - `handle` - The handle to extract the value from

  ## Examples

      iex> handle = Absynthe.Assertions.Handle.new(123)
      iex> Absynthe.Assertions.Handle.to_integer(handle)
      123

  """
  @spec to_integer(t()) :: non_neg_integer()
  def to_integer(%__MODULE__{id: id}), do: id
end
