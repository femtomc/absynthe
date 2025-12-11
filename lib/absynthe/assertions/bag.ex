defmodule Absynthe.Assertions.Bag do
  @moduledoc """
  Reference-counting multiset (bag) for assertion tracking in the Syndicated Actor Model.

  In the syndicated actor model, the same assertion can be made multiple times by different
  actors or even by the same actor. The `Bag` tracks how many times each assertion has been
  made using reference counting. When the count drops to zero, the assertion is truly retracted
  from the dataspace.

  ## Key Concepts

  ### Reference Counting
  Each unique assertion (identified by its value) has an associated count. When an assertion
  is added, the count increases. When removed, it decreases. The assertion is only considered
  "present" when the count is greater than zero.

  ### Handles
  Each individual assertion instance is identified by a unique `Absynthe.Assertions.Handle.t()`.
  Even if the same assertion value is added multiple times, each addition gets its own handle.
  This allows precise removal of specific assertion instances.

  ### Change Detection
  The `add/3` and `remove/2` functions return atoms indicating whether the operation caused
  a significant change:
  - `:new` - The assertion count transitioned from 0 to 1 (first occurrence)
  - `:existing` - The assertion count increased but was already > 0
  - `:removed` - The assertion count transitioned from 1 to 0 (last occurrence removed)
  - `:decremented` - The assertion count decreased but is still > 0
  - `:not_found` - Attempted to remove a handle that doesn't exist

  These transitions are crucial for the dataspace to know when to notify observers about
  assertion changes.

  ### Preserves Values
  Assertions are represented as `Absynthe.Preserves.Value.t()` terms. These are tagged
  tuples that can represent various types of data (atoms, compounds, embedded references).
  Elixir's structural equality is used for comparing assertion values.

  ## Performance Characteristics

  All primary operations are O(1):
  - `add/3` - O(1) average case (map insertion and lookup)
  - `remove/2` - O(1) average case (map lookup and deletion)
  - `member?/2` - O(1) (map lookup)
  - `count/2` - O(1) (map lookup)
  - `size/1` - O(n) where n is the number of unique assertions
  - `assertions/1` - O(n) where n is the number of unique assertions

  ## Examples

      iex> alias Absynthe.Assertions.{Bag, Handle}
      iex> alias Absynthe.Preserves.Value
      iex> bag = Bag.new()
      iex> assertion = Value.string("hello")
      iex> {:new, bag} = Bag.add(bag, assertion, Handle.new(1))
      iex> Bag.member?(bag, assertion)
      true
      iex> Bag.count(bag, assertion)
      1

  Adding the same assertion with a different handle:

      iex> alias Absynthe.Assertions.{Bag, Handle}
      iex> alias Absynthe.Preserves.Value
      iex> bag = Bag.new()
      iex> assertion = Value.string("hello")
      iex> {:new, bag} = Bag.add(bag, assertion, Handle.new(1))
      iex> {:existing, bag} = Bag.add(bag, assertion, Handle.new(2))
      iex> Bag.count(bag, assertion)
      2

  Removing assertions by handle:

      iex> alias Absynthe.Assertions.{Bag, Handle}
      iex> alias Absynthe.Preserves.Value
      iex> bag = Bag.new()
      iex> assertion = Value.string("hello")
      iex> {:new, bag} = Bag.add(bag, assertion, Handle.new(1))
      iex> {:existing, bag} = Bag.add(bag, assertion, Handle.new(2))
      iex> {:decremented, bag} = Bag.remove(bag, Handle.new(1))
      iex> Bag.count(bag, assertion)
      1
      iex> {:removed, bag} = Bag.remove(bag, Handle.new(2))
      iex> Bag.member?(bag, assertion)
      false

  Working with multiple assertions:

      iex> alias Absynthe.Assertions.{Bag, Handle}
      iex> alias Absynthe.Preserves.Value
      iex> bag = Bag.new()
      iex> {:new, bag} = Bag.add(bag, Value.string("hello"), Handle.new(1))
      iex> {:new, bag} = Bag.add(bag, Value.integer(42), Handle.new(2))
      iex> assertions = Bag.assertions(bag)
      iex> length(assertions)
      2
      iex> Bag.size(bag)
      2
  """

  alias Absynthe.Assertions.Handle
  alias Absynthe.Preserves.Value

  @typedoc """
  The bag data structure.

  Maintains two maps:
  - `assertions`: Maps assertion values to `{count, handles_set}` tuples
  - `handle_to_assertion`: Reverse index mapping handles to their assertion values for O(1) removal
  """
  @type t :: %__MODULE__{
          assertions: %{Value.t() => {pos_integer(), MapSet.t(Handle.t())}},
          handle_to_assertion: %{Handle.t() => Value.t()}
        }

  defstruct assertions: %{},
            handle_to_assertion: %{}

  @doc """
  Creates a new empty bag.

  ## Examples

      iex> bag = Absynthe.Assertions.Bag.new()
      iex> Absynthe.Assertions.Bag.size(bag)
      0

      iex> bag = Absynthe.Assertions.Bag.new()
      iex> Absynthe.Assertions.Bag.assertions(bag)
      []
  """
  @spec new() :: t()
  def new do
    %__MODULE__{}
  end

  @doc """
  Adds an assertion to the bag with a specific handle.

  The handle must be unique across all assertions in the bag. If you attempt to add
  an assertion with a handle that already exists, the behavior is undefined and will
  likely corrupt the bag's internal state.

  Returns a tuple indicating whether this is a new assertion or an existing one:
  - `{:new, bag}` - The assertion was not present (count was 0, now 1)
  - `{:existing, bag}` - The assertion was already present (count increased)

  ## Parameters

    - `bag` - The bag to add the assertion to
    - `assertion` - The assertion value to add
    - `handle` - A unique handle identifying this specific assertion instance

  ## Examples

      iex> alias Absynthe.Assertions.{Bag, Handle}
      iex> alias Absynthe.Preserves.Value
      iex> bag = Bag.new()
      iex> {status, _bag} = Bag.add(bag, Value.string("hello"), Handle.new(1))
      iex> status
      :new

      iex> alias Absynthe.Assertions.{Bag, Handle}
      iex> alias Absynthe.Preserves.Value
      iex> bag = Bag.new()
      iex> {:new, bag} = Bag.add(bag, Value.string("hello"), Handle.new(1))
      iex> {status, _bag} = Bag.add(bag, Value.string("hello"), Handle.new(2))
      iex> status
      :existing
  """
  @spec add(t(), Value.t(), Handle.t()) :: {:new | :existing, t()}
  def add(%__MODULE__{} = bag, assertion, %Handle{} = handle) do
    case Map.get(bag.assertions, assertion) do
      nil ->
        # First occurrence of this assertion
        new_assertions =
          Map.put(bag.assertions, assertion, {1, MapSet.new([handle])})

        new_handle_map =
          Map.put(bag.handle_to_assertion, handle, assertion)

        new_bag = %__MODULE__{
          assertions: new_assertions,
          handle_to_assertion: new_handle_map
        }

        {:new, new_bag}

      {count, handles} ->
        # Assertion already exists, increment count
        new_handles = MapSet.put(handles, handle)
        new_assertions = Map.put(bag.assertions, assertion, {count + 1, new_handles})

        new_handle_map =
          Map.put(bag.handle_to_assertion, handle, assertion)

        new_bag = %__MODULE__{
          assertions: new_assertions,
          handle_to_assertion: new_handle_map
        }

        {:existing, new_bag}
    end
  end

  @doc """
  Removes an assertion from the bag by its handle.

  Looks up which assertion the handle refers to and decrements its count. If the count
  reaches zero, the assertion is completely removed from the bag.

  Returns a tuple indicating the result:
  - `{:removed, bag}` - The last instance was removed (count went from 1 to 0)
  - `{:decremented, bag}` - An instance was removed but others remain (count decreased but still > 0)
  - `:not_found` - The handle was not found in the bag

  ## Parameters

    - `bag` - The bag to remove the assertion from
    - `handle` - The handle of the assertion instance to remove

  ## Examples

      iex> alias Absynthe.Assertions.{Bag, Handle}
      iex> alias Absynthe.Preserves.Value
      iex> bag = Bag.new()
      iex> {:new, bag} = Bag.add(bag, Value.string("hello"), Handle.new(1))
      iex> {status, _bag} = Bag.remove(bag, Handle.new(1))
      iex> status
      :removed

      iex> alias Absynthe.Assertions.{Bag, Handle}
      iex> alias Absynthe.Preserves.Value
      iex> bag = Bag.new()
      iex> {:new, bag} = Bag.add(bag, Value.string("hello"), Handle.new(1))
      iex> {:existing, bag} = Bag.add(bag, Value.string("hello"), Handle.new(2))
      iex> {status, _bag} = Bag.remove(bag, Handle.new(1))
      iex> status
      :decremented

      iex> alias Absynthe.Assertions.{Bag, Handle}
      iex> bag = Absynthe.Assertions.Bag.new()
      iex> Bag.remove(bag, Handle.new(999))
      :not_found
  """
  @spec remove(t(), Handle.t()) :: {:removed | :decremented, t()} | :not_found
  def remove(%__MODULE__{} = bag, %Handle{} = handle) do
    case Map.get(bag.handle_to_assertion, handle) do
      nil ->
        # Handle not found
        :not_found

      assertion ->
        # Handle found, get the current count and handles
        {count, handles} = Map.fetch!(bag.assertions, assertion)
        new_handles = MapSet.delete(handles, handle)
        new_handle_map = Map.delete(bag.handle_to_assertion, handle)

        if count == 1 do
          # Last instance being removed
          new_assertions = Map.delete(bag.assertions, assertion)

          new_bag = %__MODULE__{
            assertions: new_assertions,
            handle_to_assertion: new_handle_map
          }

          {:removed, new_bag}
        else
          # Still have other instances
          new_assertions = Map.put(bag.assertions, assertion, {count - 1, new_handles})

          new_bag = %__MODULE__{
            assertions: new_assertions,
            handle_to_assertion: new_handle_map
          }

          {:decremented, new_bag}
        end
    end
  end

  @doc """
  Checks if an assertion is present in the bag (count > 0).

  Note that this checks for the assertion's value, not a specific handle.
  An assertion is considered present if its count is greater than zero.

  ## Parameters

    - `bag` - The bag to check
    - `assertion` - The assertion value to look for

  ## Examples

      iex> alias Absynthe.Assertions.{Bag, Handle}
      iex> alias Absynthe.Preserves.Value
      iex> bag = Bag.new()
      iex> Bag.member?(bag, Value.string("hello"))
      false

      iex> alias Absynthe.Assertions.{Bag, Handle}
      iex> alias Absynthe.Preserves.Value
      iex> bag = Bag.new()
      iex> {:new, bag} = Bag.add(bag, Value.string("hello"), Handle.new(1))
      iex> Bag.member?(bag, Value.string("hello"))
      true

      iex> alias Absynthe.Assertions.{Bag, Handle}
      iex> alias Absynthe.Preserves.Value
      iex> bag = Bag.new()
      iex> {:new, bag} = Bag.add(bag, Value.string("hello"), Handle.new(1))
      iex> {:removed, bag} = Bag.remove(bag, Handle.new(1))
      iex> Bag.member?(bag, Value.string("hello"))
      false
  """
  @spec member?(t(), Value.t()) :: boolean()
  def member?(%__MODULE__{} = bag, assertion) do
    Map.has_key?(bag.assertions, assertion)
  end

  @doc """
  Gets all unique assertions currently in the bag.

  Returns a list of all assertion values that have a count greater than zero.
  The order of assertions in the returned list is undefined.

  ## Parameters

    - `bag` - The bag to get assertions from

  ## Examples

      iex> alias Absynthe.Assertions.{Bag, Handle}
      iex> alias Absynthe.Preserves.Value
      iex> bag = Bag.new()
      iex> Bag.assertions(bag)
      []

      iex> alias Absynthe.Assertions.{Bag, Handle}
      iex> alias Absynthe.Preserves.Value
      iex> bag = Bag.new()
      iex> {:new, bag} = Bag.add(bag, Value.string("hello"), Handle.new(1))
      iex> {:new, bag} = Bag.add(bag, Value.integer(42), Handle.new(2))
      iex> assertions = Bag.assertions(bag)
      iex> length(assertions)
      2
      iex> Enum.member?(assertions, Value.string("hello"))
      true
      iex> Enum.member?(assertions, Value.integer(42))
      true
  """
  @spec assertions(t()) :: [Value.t()]
  def assertions(%__MODULE__{} = bag) do
    Map.keys(bag.assertions)
  end

  @doc """
  Gets the reference count for a specific assertion.

  Returns the number of times this assertion has been added (and not yet removed).
  Returns 0 if the assertion is not in the bag.

  ## Parameters

    - `bag` - The bag to check
    - `assertion` - The assertion value to get the count for

  ## Examples

      iex> alias Absynthe.Assertions.{Bag, Handle}
      iex> alias Absynthe.Preserves.Value
      iex> bag = Bag.new()
      iex> Bag.count(bag, Value.string("hello"))
      0

      iex> alias Absynthe.Assertions.{Bag, Handle}
      iex> alias Absynthe.Preserves.Value
      iex> bag = Bag.new()
      iex> {:new, bag} = Bag.add(bag, Value.string("hello"), Handle.new(1))
      iex> Bag.count(bag, Value.string("hello"))
      1

      iex> alias Absynthe.Assertions.{Bag, Handle}
      iex> alias Absynthe.Preserves.Value
      iex> bag = Bag.new()
      iex> {:new, bag} = Bag.add(bag, Value.string("hello"), Handle.new(1))
      iex> {:existing, bag} = Bag.add(bag, Value.string("hello"), Handle.new(2))
      iex> {:existing, bag} = Bag.add(bag, Value.string("hello"), Handle.new(3))
      iex> Bag.count(bag, Value.string("hello"))
      3
  """
  @spec count(t(), Value.t()) :: non_neg_integer()
  def count(%__MODULE__{} = bag, assertion) do
    case Map.get(bag.assertions, assertion) do
      nil -> 0
      {count, _handles} -> count
    end
  end

  @doc """
  Gets the total number of assertion entries in the bag.

  This is the sum of all reference counts across all assertions. Note that this
  is different from the number of unique assertions (which is `length(assertions(bag))`).

  ## Parameters

    - `bag` - The bag to get the size of

  ## Examples

      iex> alias Absynthe.Assertions.{Bag, Handle}
      iex> bag = Bag.new()
      iex> Bag.size(bag)
      0

      iex> alias Absynthe.Assertions.{Bag, Handle}
      iex> alias Absynthe.Preserves.Value
      iex> bag = Bag.new()
      iex> {:new, bag} = Bag.add(bag, Value.string("hello"), Handle.new(1))
      iex> Bag.size(bag)
      1

      iex> alias Absynthe.Assertions.{Bag, Handle}
      iex> alias Absynthe.Preserves.Value
      iex> bag = Bag.new()
      iex> {:new, bag} = Bag.add(bag, Value.string("hello"), Handle.new(1))
      iex> {:existing, bag} = Bag.add(bag, Value.string("hello"), Handle.new(2))
      iex> {:new, bag} = Bag.add(bag, Value.integer(42), Handle.new(3))
      iex> Bag.size(bag)
      3
  """
  @spec size(t()) :: non_neg_integer()
  def size(%__MODULE__{} = bag) do
    bag.assertions
    |> Map.values()
    |> Enum.reduce(0, fn {count, _handles}, acc -> acc + count end)
  end
end
