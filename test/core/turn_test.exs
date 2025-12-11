defmodule Absynthe.Core.TurnTest do
  use ExUnit.Case, async: true

  alias Absynthe.Core.Turn
  alias Absynthe.Core.Ref
  alias Absynthe.Protocol.Event

  describe "commit safeguards" do
    test "committing a turn twice raises error" do
      turn = Turn.new(:actor, :facet)
      {committed_turn, []} = Turn.commit(turn)

      assert_raise ArgumentError, ~r/Cannot commit an already committed turn/, fn ->
        Turn.commit(committed_turn)
      end
    end

    test "adding action to committed turn raises error" do
      turn = Turn.new(:actor, :facet)
      {committed_turn, []} = Turn.commit(turn)

      ref = Ref.new(:actor, 0)
      action = Event.message(ref, {:string, "test"})

      assert_raise ArgumentError, ~r/Cannot add actions to an already committed turn/, fn ->
        Turn.add_action(committed_turn, action)
      end
    end

    test "normal turn workflow succeeds" do
      turn = Turn.new(:actor, :facet)
      ref = Ref.new(:actor, 0)

      # Add actions
      turn = Turn.add_action(turn, Event.message(ref, {:string, "first"}))
      turn = Turn.add_action(turn, Event.message(ref, {:string, "second"}))

      assert Turn.action_count(turn) == 2
      refute Turn.committed?(turn)

      # Commit
      {committed_turn, actions} = Turn.commit(turn)

      assert Turn.committed?(committed_turn)
      assert length(actions) == 2

      # Actions are in insertion order
      [first, second] = actions
      assert first.body == {:string, "first"}
      assert second.body == {:string, "second"}
    end
  end

  describe "turn state" do
    test "new turn is not committed" do
      turn = Turn.new(:actor, :facet)
      refute Turn.committed?(turn)
    end

    test "new turn has no actions" do
      turn = Turn.new(:actor, :facet)
      assert Turn.actions(turn) == []
      refute Turn.has_actions?(turn)
    end

    test "rollback returns empty list" do
      turn = Turn.new(:actor, :facet)
      ref = Ref.new(:actor, 0)

      turn = Turn.add_action(turn, Event.message(ref, {:string, "test"}))
      assert Turn.has_actions?(turn)

      assert Turn.rollback(turn) == []
    end
  end
end
