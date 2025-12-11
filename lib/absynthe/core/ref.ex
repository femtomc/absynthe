defmodule Absynthe.Core.Ref do
  @moduledoc """
  Entity reference (capability) for the Syndicated Actor Model.

  A Ref is a capability that identifies an entity within the actor system.
  It encapsulates both the actor ID (which actor owns the entity) and the
  entity ID (which entity within that actor), allowing messages and assertions
  to be sent to specific entities.

  Refs are the fundamental mechanism for communication in the Syndicated Actor
  Model. They are opaque to users but internally track routing information
  needed to deliver messages and assertions to the correct entity.

  ## Design

  A Ref contains:
  - `actor_id` - Identifies which actor owns the entity
  - `entity_id` - Identifies the entity within that actor
  - `attenuation` - Optional capability attenuation (reserved for future use)

  Refs implement structural equality and can be used as map keys, stored in
  sets, and compared for ordering.

  ## Examples

      iex> ref = Absynthe.Core.Ref.new(:actor_123, :entity_456)
      %Absynthe.Core.Ref{actor_id: :actor_123, entity_id: :entity_456, attenuation: nil}

      iex> Absynthe.Core.Ref.actor_id(ref)
      :actor_123

      iex> Absynthe.Core.Ref.entity_id(ref)
      :entity_456

      iex> Absynthe.Core.Ref.local?(ref, :actor_123)
      true

      iex> Absynthe.Core.Ref.local?(ref, :actor_999)
      false

  ## Usage in the Actor System

  Refs are typically created by actors when spawning entities or by the
  dataspace when establishing subscriptions. Users interact with refs
  primarily by passing them to message-sending and assertion functions:

      # Send a message to an entity
      Absynthe.Core.Actor.send_message(ref, {:ping, self()})

      # Assert a fact about an entity
      Absynthe.Core.Actor.assert(ref, {:status, :online})

  ## Capability Attenuation (Future)

  The `attenuation` field is reserved for future capability attenuation
  features, where a Ref can be restricted to only allow certain operations
  or only access certain aspects of an entity. Currently, this field is
  always `nil` and is not used by the system.
  """

  @typedoc """
  A reference to an entity within the actor system.

  Contains routing information needed to send messages and assertions to
  the correct entity within the correct actor.
  """
  @type t :: %__MODULE__{
          actor_id: term(),
          entity_id: term(),
          attenuation: term() | nil
        }

  @enforce_keys [:actor_id, :entity_id]
  defstruct [:actor_id, :entity_id, attenuation: nil]

  # Constructor Functions

  @doc """
  Creates a new Ref pointing to an entity.

  ## Parameters

  - `actor_id` - The ID of the actor that owns the entity
  - `entity_id` - The ID of the entity within that actor
  - `attenuation` - Optional capability attenuation (defaults to `nil`)

  ## Examples

      iex> Absynthe.Core.Ref.new(:my_actor, :my_entity)
      %Absynthe.Core.Ref{actor_id: :my_actor, entity_id: :my_entity, attenuation: nil}

      iex> Absynthe.Core.Ref.new("actor-1", {:entity, :dataspace})
      %Absynthe.Core.Ref{actor_id: "actor-1", entity_id: {:entity, :dataspace}, attenuation: nil}

      iex> Absynthe.Core.Ref.new(:actor, :entity, [:read_only])
      %Absynthe.Core.Ref{actor_id: :actor, entity_id: :entity, attenuation: [:read_only]}
  """
  @spec new(term(), term(), term() | nil) :: t()
  def new(actor_id, entity_id, attenuation \\ nil) do
    %__MODULE__{
      actor_id: actor_id,
      entity_id: entity_id,
      attenuation: attenuation
    }
  end

  # Accessor Functions

  @doc """
  Returns the actor ID from a Ref.

  ## Examples

      iex> ref = Absynthe.Core.Ref.new(:my_actor, :my_entity)
      iex> Absynthe.Core.Ref.actor_id(ref)
      :my_actor
  """
  @spec actor_id(t()) :: term()
  def actor_id(%__MODULE__{actor_id: actor_id}), do: actor_id

  @doc """
  Returns the entity ID from a Ref.

  ## Examples

      iex> ref = Absynthe.Core.Ref.new(:my_actor, :my_entity)
      iex> Absynthe.Core.Ref.entity_id(ref)
      :my_entity
  """
  @spec entity_id(t()) :: term()
  def entity_id(%__MODULE__{entity_id: entity_id}), do: entity_id

  @doc """
  Returns the attenuation from a Ref, or `nil` if none is present.

  ## Examples

      iex> ref = Absynthe.Core.Ref.new(:my_actor, :my_entity)
      iex> Absynthe.Core.Ref.attenuation(ref)
      nil

      iex> ref = Absynthe.Core.Ref.new(:my_actor, :my_entity, [:read_only])
      iex> Absynthe.Core.Ref.attenuation(ref)
      [:read_only]
  """
  @spec attenuation(t()) :: term() | nil
  def attenuation(%__MODULE__{attenuation: attenuation}), do: attenuation

  # Query Functions

  @doc """
  Checks if a Ref points to an entity local to the given actor.

  This is useful for optimizing message delivery - local refs can be
  delivered directly without serialization or remote communication.

  ## Parameters

  - `ref` - The Ref to check
  - `current_actor_id` - The ID of the actor to check against

  ## Examples

      iex> ref = Absynthe.Core.Ref.new(:actor_123, :entity_456)
      iex> Absynthe.Core.Ref.local?(ref, :actor_123)
      true

      iex> Absynthe.Core.Ref.local?(ref, :actor_999)
      false

      iex> Absynthe.Core.Ref.local?(ref, nil)
      false
  """
  @spec local?(t(), term()) :: boolean()
  def local?(%__MODULE__{actor_id: actor_id}, current_actor_id) do
    actor_id == current_actor_id
  end

  @doc """
  Checks if a Ref points to an entity in a remote actor.

  This is the logical opposite of `local?/2`.

  ## Parameters

  - `ref` - The Ref to check
  - `current_actor_id` - The ID of the actor to check against

  ## Examples

      iex> ref = Absynthe.Core.Ref.new(:actor_123, :entity_456)
      iex> Absynthe.Core.Ref.remote?(ref, :actor_999)
      true

      iex> Absynthe.Core.Ref.remote?(ref, :actor_123)
      false
  """
  @spec remote?(t(), term()) :: boolean()
  def remote?(%__MODULE__{} = ref, current_actor_id) do
    not local?(ref, current_actor_id)
  end

  @doc """
  Checks if a Ref has any attenuation applied.

  ## Examples

      iex> ref = Absynthe.Core.Ref.new(:actor, :entity)
      iex> Absynthe.Core.Ref.attenuated?(ref)
      false

      iex> ref = Absynthe.Core.Ref.new(:actor, :entity, [:read_only])
      iex> Absynthe.Core.Ref.attenuated?(ref)
      true
  """
  @spec attenuated?(t()) :: boolean()
  def attenuated?(%__MODULE__{attenuation: nil}), do: false
  def attenuated?(%__MODULE__{attenuation: _}), do: true

  # Modification Functions

  @doc """
  Creates a new Ref with the given attenuation applied.

  This returns a new Ref; it does not modify the original.

  ## Parameters

  - `ref` - The Ref to attenuate
  - `attenuation` - The attenuation to apply

  ## Examples

      iex> ref = Absynthe.Core.Ref.new(:actor, :entity)
      iex> attenuated_ref = Absynthe.Core.Ref.with_attenuation(ref, [:read_only])
      iex> Absynthe.Core.Ref.attenuation(attenuated_ref)
      [:read_only]

      iex> Absynthe.Core.Ref.attenuation(ref)
      nil
  """
  @spec with_attenuation(t(), term()) :: t()
  def with_attenuation(%__MODULE__{} = ref, attenuation) do
    %__MODULE__{ref | attenuation: attenuation}
  end

  @doc """
  Creates a new Ref with the attenuation removed.

  This returns a new Ref; it does not modify the original.

  ## Examples

      iex> ref = Absynthe.Core.Ref.new(:actor, :entity, [:read_only])
      iex> unattenuated_ref = Absynthe.Core.Ref.without_attenuation(ref)
      iex> Absynthe.Core.Ref.attenuation(unattenuated_ref)
      nil

      iex> Absynthe.Core.Ref.attenuation(ref)
      [:read_only]
  """
  @spec without_attenuation(t()) :: t()
  def without_attenuation(%__MODULE__{} = ref) do
    %__MODULE__{ref | attenuation: nil}
  end
end
