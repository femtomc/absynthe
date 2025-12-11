defmodule Absynthe.Core.Turn do
  @moduledoc """
  Transaction handling for actor turns in the Syndicated Actor Model.

  A Turn represents an atomic unit of work within an actor. When an entity
  receives an event (assertion, retraction, message, or sync), the event is
  processed within a turn. During the turn, entity handlers can enqueue
  actions (sending messages, asserting facts, etc.). At the end of the turn,
  all pending actions are either committed (executed) or rolled back (discarded).

  ## Transaction Semantics

  Turns provide transaction-like semantics for actor operations:

  - **Atomicity**: All actions in a turn either all happen or none happen
  - **Isolation**: Actions are not visible until the turn commits
  - **Consistency**: The system moves from one consistent state to another
  - **Simplicity**: Rollback is implemented by simply discarding the action list

  ## Turn Lifecycle

  1. **Creation**: A turn is created when an event is delivered to an entity
  2. **Execution**: Entity handlers process the event and enqueue actions
  3. **Accumulation**: Multiple handlers can add actions to the same turn
  4. **Commit/Rollback**: After all handlers complete:
     - On success: Turn is committed, all pending actions are executed
     - On error: Turn is rolled back, all pending actions are discarded
  5. **Completion**: The turn is marked as committed and actions are returned

  ## Actions

  Actions are protocol events that can be enqueued during a turn:

  - `Absynthe.Protocol.Event.Assert` - Publish an assertion
  - `Absynthe.Protocol.Event.Retract` - Remove an assertion
  - `Absynthe.Protocol.Event.Message` - Send a message
  - `Absynthe.Protocol.Event.Sync` - Synchronization barrier

  ## Examples

      # Create a new turn
      turn = Absynthe.Core.Turn.new(:actor_123, :facet_456)

      # Add actions during event processing
      assertion = {:string, "Hello"}
      ref = %Absynthe.Core.Ref{actor_id: 1, entity_id: 2}
      handle = %Absynthe.Assertions.Handle{id: 1}
      action = Absynthe.Protocol.Event.assert(ref, assertion, handle)
      turn = Absynthe.Core.Turn.add_action(turn, action)

      # Commit the turn to execute actions
      actions = Absynthe.Core.Turn.commit(turn)

      # Or rollback to discard actions
      [] = Absynthe.Core.Turn.rollback(turn)

  ## Design Notes

  The Turn implementation is intentionally simple. Rollback is achieved by
  simply discarding the pending actions list, which is both simple and correct.
  This design avoids the need for complex undo logic or compensating transactions.

  The turn keeps track of its facet context, which represents the conversational
  scope. This allows the actor to properly manage facet lifecycles and ensure
  that actions are executed in the correct context.
  """

  alias Absynthe.Protocol.Event

  @typedoc """
  A transaction/atomic unit of work within an actor.

  Fields:
  - `actor_id` - The ID of the actor executing this turn
  - `facet_id` - The ID of the facet (conversational scope) this turn belongs to
  - `pending_actions` - List of actions to perform when turn commits
  - `committed?` - Whether the turn has been committed
  """
  @type t :: %__MODULE__{
          actor_id: term(),
          facet_id: term(),
          pending_actions: [Event.t()],
          committed?: boolean()
        }

  @enforce_keys [:actor_id, :facet_id]
  defstruct [:actor_id, :facet_id, pending_actions: [], committed?: false]

  # Constructor

  @doc """
  Creates a new turn for the given actor and facet.

  The turn begins with an empty action list and is not committed.

  ## Parameters

  - `actor_id` - The ID of the actor executing this turn
  - `facet_id` - The ID of the facet (conversational scope)

  ## Returns

  A new `Turn` struct with no pending actions.

  ## Examples

      iex> turn = Absynthe.Core.Turn.new(:my_actor, :my_facet)
      %Absynthe.Core.Turn{
        actor_id: :my_actor,
        facet_id: :my_facet,
        pending_actions: [],
        committed?: false
      }

      iex> Absynthe.Core.Turn.committed?(turn)
      false

      iex> Absynthe.Core.Turn.actions(turn)
      []
  """
  @spec new(term(), term()) :: t()
  def new(actor_id, facet_id) do
    %__MODULE__{
      actor_id: actor_id,
      facet_id: facet_id,
      pending_actions: [],
      committed?: false
    }
  end

  # Action Management

  @doc """
  Adds an action to the turn's pending action list.

  Actions are enqueued during turn execution and will be executed when the
  turn is committed. Actions are executed in the order they are added.

  ## Parameters

  - `turn` - The turn to add the action to
  - `action` - A protocol event (Assert, Retract, Message, or Sync)

  ## Returns

  A new `Turn` struct with the action added to the pending list.

  ## Examples

      iex> turn = Absynthe.Core.Turn.new(:actor, :facet)
      iex> ref = %Absynthe.Core.Ref{actor_id: 1, entity_id: 2}
      iex> message = {:string, "Hello"}
      iex> action = Absynthe.Protocol.Event.message(ref, message)
      iex> turn = Absynthe.Core.Turn.add_action(turn, action)
      iex> length(Absynthe.Core.Turn.actions(turn))
      1

  Multiple actions can be added:

      iex> turn = Absynthe.Core.Turn.new(:actor, :facet)
      iex> ref = %Absynthe.Core.Ref{actor_id: 1, entity_id: 2}
      iex> action1 = Absynthe.Protocol.Event.message(ref, {:string, "First"})
      iex> action2 = Absynthe.Protocol.Event.message(ref, {:string, "Second"})
      iex> turn = turn |> Absynthe.Core.Turn.add_action(action1) |> Absynthe.Core.Turn.add_action(action2)
      iex> length(Absynthe.Core.Turn.actions(turn))
      2
  """
  @spec add_action(t(), Event.t()) :: t()
  def add_action(%__MODULE__{committed?: true}, _action) do
    raise ArgumentError, "Cannot add actions to an already committed turn"
  end

  def add_action(%__MODULE__{pending_actions: actions} = turn, action) do
    # Prepend for O(1) insertion - actions stored in reverse order
    %__MODULE__{turn | pending_actions: [action | actions]}
  end

  @doc """
  Returns the list of pending actions in the turn.

  This is useful for inspecting what actions will be executed when the turn
  is committed. The actions are returned in the order they were added.

  ## Parameters

  - `turn` - The turn to get actions from

  ## Returns

  A list of protocol events.

  ## Examples

      iex> turn = Absynthe.Core.Turn.new(:actor, :facet)
      iex> Absynthe.Core.Turn.actions(turn)
      []

      iex> turn = Absynthe.Core.Turn.new(:actor, :facet)
      iex> ref = %Absynthe.Core.Ref{actor_id: 1, entity_id: 2}
      iex> action = Absynthe.Protocol.Event.message(ref, {:string, "Test"})
      iex> turn = Absynthe.Core.Turn.add_action(turn, action)
      iex> [first_action] = Absynthe.Core.Turn.actions(turn)
      iex> Absynthe.Protocol.Event.message?(first_action)
      true
  """
  @spec actions(t()) :: [Event.t()]
  def actions(%__MODULE__{pending_actions: actions}), do: Enum.reverse(actions)

  # Transaction Operations

  @doc """
  Commits the turn, marking it as committed and returning the pending actions.

  Once committed, the turn's actions should be executed by the actor. The turn
  itself is marked as committed and the actions are returned for execution.

  ## Parameters

  - `turn` - The turn to commit

  ## Returns

  A tuple of `{committed_turn, actions}` where:
  - `committed_turn` is the turn marked as committed
  - `actions` is the list of pending actions to execute

  ## Examples

      iex> turn = Absynthe.Core.Turn.new(:actor, :facet)
      iex> ref = %Absynthe.Core.Ref{actor_id: 1, entity_id: 2}
      iex> action = Absynthe.Protocol.Event.message(ref, {:string, "Test"})
      iex> turn = Absynthe.Core.Turn.add_action(turn, action)
      iex> {committed_turn, actions} = Absynthe.Core.Turn.commit(turn)
      iex> Absynthe.Core.Turn.committed?(committed_turn)
      true
      iex> length(actions)
      1

  Committing an empty turn:

      iex> turn = Absynthe.Core.Turn.new(:actor, :facet)
      iex> {committed_turn, actions} = Absynthe.Core.Turn.commit(turn)
      iex> Absynthe.Core.Turn.committed?(committed_turn)
      true
      iex> actions
      []
  """
  @spec commit(t()) :: {t(), [Event.t()]}
  def commit(%__MODULE__{committed?: true}) do
    raise ArgumentError, "Cannot commit an already committed turn"
  end

  def commit(%__MODULE__{pending_actions: actions} = turn) do
    committed_turn = %__MODULE__{turn | committed?: true}
    # Reverse to restore original insertion order
    {committed_turn, Enum.reverse(actions)}
  end

  @doc """
  Rolls back the turn, discarding all pending actions.

  Rollback is simple: it just returns an empty list. No actions are executed,
  and the turn's pending actions are effectively discarded. The turn itself
  is not modified; the caller should simply stop using it.

  ## Parameters

  - `turn` - The turn to roll back

  ## Returns

  An empty list `[]`.

  ## Examples

      iex> turn = Absynthe.Core.Turn.new(:actor, :facet)
      iex> ref = %Absynthe.Core.Ref{actor_id: 1, entity_id: 2}
      iex> action = Absynthe.Protocol.Event.message(ref, {:string, "Test"})
      iex> turn = Absynthe.Core.Turn.add_action(turn, action)
      iex> Absynthe.Core.Turn.rollback(turn)
      []

  Rollback discards all actions:

      iex> turn = Absynthe.Core.Turn.new(:actor, :facet)
      iex> ref = %Absynthe.Core.Ref{actor_id: 1, entity_id: 2}
      iex> turn = turn
      ...>   |> Absynthe.Core.Turn.add_action(Absynthe.Protocol.Event.message(ref, {:string, "1"}))
      ...>   |> Absynthe.Core.Turn.add_action(Absynthe.Protocol.Event.message(ref, {:string, "2"}))
      ...>   |> Absynthe.Core.Turn.add_action(Absynthe.Protocol.Event.message(ref, {:string, "3"}))
      iex> length(Absynthe.Core.Turn.actions(turn))
      3
      iex> Absynthe.Core.Turn.rollback(turn)
      []

  ## Design Note

  The simplicity of rollback (just returning an empty list) is a key design
  decision. Since actions haven't been executed yet, there's nothing to undo.
  This makes the turn transaction model both simple and correct.
  """
  @spec rollback(t()) :: []
  def rollback(%__MODULE__{}), do: []

  # Query Functions

  @doc """
  Checks if the turn has been committed.

  ## Parameters

  - `turn` - The turn to check

  ## Returns

  `true` if the turn has been committed, `false` otherwise.

  ## Examples

      iex> turn = Absynthe.Core.Turn.new(:actor, :facet)
      iex> Absynthe.Core.Turn.committed?(turn)
      false

      iex> turn = Absynthe.Core.Turn.new(:actor, :facet)
      iex> {committed_turn, _actions} = Absynthe.Core.Turn.commit(turn)
      iex> Absynthe.Core.Turn.committed?(committed_turn)
      true
  """
  @spec committed?(t()) :: boolean()
  def committed?(%__MODULE__{committed?: committed?}), do: committed?

  @doc """
  Returns the actor ID associated with this turn.

  ## Parameters

  - `turn` - The turn to query

  ## Returns

  The actor ID.

  ## Examples

      iex> turn = Absynthe.Core.Turn.new(:my_actor, :my_facet)
      iex> Absynthe.Core.Turn.actor_id(turn)
      :my_actor
  """
  @spec actor_id(t()) :: term()
  def actor_id(%__MODULE__{actor_id: actor_id}), do: actor_id

  @doc """
  Returns the facet ID associated with this turn.

  ## Parameters

  - `turn` - The turn to query

  ## Returns

  The facet ID.

  ## Examples

      iex> turn = Absynthe.Core.Turn.new(:my_actor, :my_facet)
      iex> Absynthe.Core.Turn.facet_id(turn)
      :my_facet
  """
  @spec facet_id(t()) :: term()
  def facet_id(%__MODULE__{facet_id: facet_id}), do: facet_id

  @doc """
  Checks if the turn has any pending actions.

  ## Parameters

  - `turn` - The turn to check

  ## Returns

  `true` if the turn has pending actions, `false` if the action list is empty.

  ## Examples

      iex> turn = Absynthe.Core.Turn.new(:actor, :facet)
      iex> Absynthe.Core.Turn.has_actions?(turn)
      false

      iex> turn = Absynthe.Core.Turn.new(:actor, :facet)
      iex> ref = %Absynthe.Core.Ref{actor_id: 1, entity_id: 2}
      iex> action = Absynthe.Protocol.Event.message(ref, {:string, "Test"})
      iex> turn = Absynthe.Core.Turn.add_action(turn, action)
      iex> Absynthe.Core.Turn.has_actions?(turn)
      true
  """
  @spec has_actions?(t()) :: boolean()
  def has_actions?(%__MODULE__{pending_actions: []}), do: false
  def has_actions?(%__MODULE__{pending_actions: [_ | _]}), do: true

  @doc """
  Returns the number of pending actions in the turn.

  ## Parameters

  - `turn` - The turn to count actions for

  ## Returns

  A non-negative integer representing the number of pending actions.

  ## Examples

      iex> turn = Absynthe.Core.Turn.new(:actor, :facet)
      iex> Absynthe.Core.Turn.action_count(turn)
      0

      iex> turn = Absynthe.Core.Turn.new(:actor, :facet)
      iex> ref = %Absynthe.Core.Ref{actor_id: 1, entity_id: 2}
      iex> turn = turn
      ...>   |> Absynthe.Core.Turn.add_action(Absynthe.Protocol.Event.message(ref, {:string, "1"}))
      ...>   |> Absynthe.Core.Turn.add_action(Absynthe.Protocol.Event.message(ref, {:string, "2"}))
      iex> Absynthe.Core.Turn.action_count(turn)
      2
  """
  @spec action_count(t()) :: non_neg_integer()
  def action_count(%__MODULE__{pending_actions: actions}), do: length(actions)
end
