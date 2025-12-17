defprotocol Absynthe.Core.Entity do
  @moduledoc """
  Protocol defining how entities respond to events in the Syndicated Actor Model.

  ## Type Specifications

  All callbacks return `{updated_entity, updated_turn}`:

      @type entity :: t()
      @type assertion :: Absynthe.Preserves.Value.t()
      @type handle :: Absynthe.Assertions.Handle.t()
      @type turn :: Absynthe.Core.Turn.t()
      @type ref :: Absynthe.Core.Ref.t()
      @type callback_result :: {t(), turn()}

  ## Overview

  An Entity in the Syndicated Actor Model is any data structure that can respond to
  events occurring within a dataspace. Entities are the computational units that
  implement application logic by reacting to changes in shared state (assertions)
  and messages.

  ## Entity Lifecycle

  Entities participate in the dataspace through four types of events:

  1. **Publication** (`on_publish/4`) - A new assertion has been added to the dataspace
     that matches a subscription this entity has registered. The entity can observe
     the assertion content and react accordingly.

  2. **Retraction** (`on_retract/3`) - A previously observed assertion has been
     removed from the dataspace. The entity is notified via the handle it received
     during publication.

  3. **Message** (`on_message/3`) - A message has been sent directly to this entity.
     Unlike assertions, messages are ephemeral and not part of the persistent
     dataspace state.

  4. **Synchronization** (`on_sync/3`) - A synchronization barrier has been reached.
     This allows entities to coordinate and ensure certain operations have completed
     before proceeding.

  ## Turn-Based Execution

  All entity callbacks receive a `Turn` struct as their final parameter. The Turn
  represents the current execution context and provides capabilities for the entity
  to interact with the dataspace:

  - Publishing and retracting assertions
  - Sending messages
  - Spawning new entities
  - Creating subscriptions

  The Turn ensures that all effects are properly sequenced and that the system
  maintains consistency.

  ## Implementing the Protocol

  To create a custom entity, implement this protocol for your data structure:

      defmodule MyEntity do
        defstruct [:state]

        defimpl Absynthe.Core.Entity do
          def on_publish(entity, assertion, handle, turn) do
            # React to new assertion
            IO.puts("Observed: \#{inspect(assertion)}")
            {entity, turn}
          end

          def on_retract(entity, handle, turn) do
            # React to retraction
            IO.puts("Retracted: \#{inspect(handle)}")
            {entity, turn}
          end

          def on_message(entity, message, turn) do
            # Handle message
            IO.puts("Received: \#{inspect(message)}")
            {entity, turn}
          end

          def on_sync(entity, peer_ref, turn) do
            # Synchronization point
            {entity, turn}
          end
        end
      end

  ## Default Implementation

  For simple use cases, you can use `Absynthe.Core.Entity.Default` which accepts
  functions for each callback:

      entity = %Absynthe.Core.Entity.Default{
        on_publish: fn entity, assertion, handle, turn ->
          # Custom logic
          {entity, turn}
        end
      }

  ## Return Values

  All callbacks must return a tuple of `{updated_entity, updated_turn}`. This allows
  entities to update their internal state and to chain dataspace operations through
  the Turn.
  """

  @fallback_to_any true

  @doc """
  Called when an assertion matching a subscription is published to the dataspace.

  ## Parameters

  - `entity` - The entity receiving the event
  - `assertion` - The assertion that was published
  - `handle` - A unique handle identifying this assertion for future retractions
  - `turn` - The current Turn context

  ## Returns

  A tuple of `{updated_entity, updated_turn}`.

  ## Example

      def on_publish(entity, {:user, user_id, name}, handle, turn) do
        new_entity = %{entity | users: Map.put(entity.users, handle, {user_id, name})}
        {new_entity, turn}
      end
  """
  @spec on_publish(t(), term(), Absynthe.Assertions.Handle.t(), Absynthe.Core.Turn.t()) ::
          {t(), Absynthe.Core.Turn.t()}
  def on_publish(entity, assertion, handle, turn)

  @doc """
  Called when a previously published assertion is retracted from the dataspace.

  ## Parameters

  - `entity` - The entity receiving the event
  - `handle` - The handle that was provided in the corresponding `on_publish` call
  - `turn` - The current Turn context

  ## Returns

  A tuple of `{updated_entity, updated_turn}`.

  ## Example

      def on_retract(entity, handle, turn) do
        new_entity = %{entity | users: Map.delete(entity.users, handle)}
        {new_entity, turn}
      end
  """
  @spec on_retract(t(), Absynthe.Assertions.Handle.t(), Absynthe.Core.Turn.t()) ::
          {t(), Absynthe.Core.Turn.t()}
  def on_retract(entity, handle, turn)

  @doc """
  Called when a message is sent to this entity.

  Messages are ephemeral and do not persist in the dataspace. They provide a way
  for entities to communicate directly without using assertions.

  ## Parameters

  - `entity` - The entity receiving the message
  - `message` - The message content
  - `turn` - The current Turn context

  ## Returns

  A tuple of `{updated_entity, updated_turn}`.

  ## Example

      def on_message(entity, {:command, :shutdown}, turn) do
        IO.puts("Shutting down...")
        {%{entity | running: false}, turn}
      end
  """
  @spec on_message(t(), term(), Absynthe.Core.Turn.t()) :: {t(), Absynthe.Core.Turn.t()}
  def on_message(entity, message, turn)

  @doc """
  Called when a synchronization barrier is reached.

  Synchronization allows entities to coordinate their actions and ensure that
  certain operations have completed before proceeding. The default behavior
  is to respond with a `{:symbol, "synced"}` message to the peer, creating a
  happens-before relationship.

  ## Parameters

  - `entity` - The entity being synchronized
  - `peer_ref` - Reference to the peer that initiated the sync
  - `turn` - The current Turn context

  ## Returns

  A tuple of `{updated_entity, updated_turn}`.

  ## Default Behavior

  The default implementation responds to `peer_ref` with a `{:symbol, "synced"}`
  message. Override this if you need custom sync behavior, such as forwarding
  the sync request to another entity.

  ## Example

      def on_sync(entity, peer_ref, turn) do
        # Default: respond with Synced message
        action = Event.message(peer_ref, {:symbol, "synced"})
        turn = Turn.add_action(turn, action)
        {entity, turn}
      end
  """
  @spec on_sync(t(), Absynthe.Core.Ref.t(), Absynthe.Core.Turn.t()) ::
          {t(), Absynthe.Core.Turn.t()}
  def on_sync(entity, peer_ref, turn)
end

defimpl Absynthe.Core.Entity, for: Any do
  @moduledoc """
  Default fallback implementation for the Entity protocol.

  Provides default implementations for all callbacks. Most callbacks are no-ops
  that return the entity and turn unchanged, but `on_sync/3` sends a
  `{:symbol, "synced"}` message to the peer to implement the standard sync
  protocol. This allows any struct to be used as an entity even if it doesn't
  implement specific event handlers.
  """

  @doc """
  Default no-op implementation for `on_publish`.

  Returns the entity and turn unchanged.
  """
  def on_publish(entity, _assertion, _handle, turn) do
    {entity, turn}
  end

  @doc """
  Default no-op implementation for `on_retract`.

  Returns the entity and turn unchanged.
  """
  def on_retract(entity, _handle, turn) do
    {entity, turn}
  end

  @doc """
  Default no-op implementation for `on_message`.

  Returns the entity and turn unchanged.
  """
  def on_message(entity, _message, turn) do
    {entity, turn}
  end

  @doc """
  Default implementation for `on_sync`.

  Responds to the peer with a `Synced` message. This is the standard sync
  protocol response that creates a happens-before relationship: the peer
  knows that the entity has processed the sync request when it receives
  the `{:symbol, "synced"}` message.
  """
  def on_sync(entity, peer_ref, turn) do
    action = Absynthe.Protocol.Event.message(peer_ref, {:symbol, "synced"})
    turn = Absynthe.Core.Turn.add_action(turn, action)
    {entity, turn}
  end
end

defmodule Absynthe.Core.Entity.Default do
  @moduledoc """
  A default entity implementation that delegates to provided functions.

  This struct allows you to create entities by providing functions for each
  callback without needing to define a new type and implement the protocol.

  ## Example

      entity = %Absynthe.Core.Entity.Default{
        on_publish: fn entity, assertion, handle, turn ->
          IO.puts("Published: \#{inspect(assertion)}")
          {entity, turn}
        end,
        on_message: fn entity, message, turn ->
          IO.puts("Message: \#{inspect(message)}")
          {entity, turn}
        end
      }

  ## Fields

  - `on_publish` - Function called for publication events. Defaults to no-op.
  - `on_retract` - Function called for retraction events. Defaults to no-op.
  - `on_message` - Function called for message events. Defaults to no-op.
  - `on_sync` - Function called for synchronization events. Defaults to sending a `{:symbol, "synced"}` message to the peer.

  Each function should have the signature:
  - `on_publish`: `(entity, assertion, handle, turn) -> {entity, turn}`
  - `on_retract`: `(entity, handle, turn) -> {entity, turn}`
  - `on_message`: `(entity, message, turn) -> {entity, turn}`
  - `on_sync`: `(entity, peer_ref, turn) -> {entity, turn}`
  """

  defstruct on_publish: nil,
            on_retract: nil,
            on_message: nil,
            on_sync: nil

  defimpl Absynthe.Core.Entity do
    def on_publish(entity, assertion, handle, turn) do
      if entity.on_publish do
        entity.on_publish.(entity, assertion, handle, turn)
      else
        {entity, turn}
      end
    end

    def on_retract(entity, handle, turn) do
      if entity.on_retract do
        entity.on_retract.(entity, handle, turn)
      else
        {entity, turn}
      end
    end

    def on_message(entity, message, turn) do
      if entity.on_message do
        entity.on_message.(entity, message, turn)
      else
        {entity, turn}
      end
    end

    def on_sync(entity, peer_ref, turn) do
      if entity.on_sync do
        entity.on_sync.(entity, peer_ref, turn)
      else
        # Default: respond with Synced message
        action = Absynthe.Protocol.Event.message(peer_ref, {:symbol, "synced"})
        turn = Absynthe.Core.Turn.add_action(turn, action)
        {entity, turn}
      end
    end
  end
end
