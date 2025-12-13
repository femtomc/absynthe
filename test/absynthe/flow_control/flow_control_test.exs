defmodule Absynthe.FlowControlTest do
  use ExUnit.Case, async: true

  alias Absynthe.FlowControl
  alias Absynthe.Protocol.Event
  alias Absynthe.Core.Ref

  describe "new_account/1" do
    test "delegates to Account.new/1" do
      account = FlowControl.new_account(id: :test, limit: 100)
      assert account.id == :test
      assert account.limit == 100
    end
  end

  describe "event_cost/1" do
    test "Assert event has base cost of 1" do
      ref = Ref.new(:test_actor, 1)
      event = Event.assert(ref, {:symbol, "simple"}, nil)
      assert FlowControl.event_cost(event) == 1
    end

    test "Assert event with embedded ref adds 1 per ref" do
      ref = Ref.new(:test_actor, 1)
      embedded_ref = Ref.new(:other_actor, 2)

      event =
        Event.assert(ref, {:record, {{:symbol, "HasRef"}, [{:embedded, embedded_ref}]}}, nil)

      # 1 base + 1 for embedded ref
      assert FlowControl.event_cost(event) == 2
    end

    test "Retract event has cost of 1" do
      ref = Ref.new(:test_actor, 1)
      event = Event.retract(ref, nil)
      assert FlowControl.event_cost(event) == 1
    end

    test "Message event has base cost of 1" do
      ref = Ref.new(:test_actor, 1)
      event = Event.message(ref, {:symbol, "ping"})
      assert FlowControl.event_cost(event) == 1
    end

    test "Sync event has cost of 1" do
      ref = Ref.new(:test_actor, 1)
      peer = Ref.new(:test_actor, 2)
      event = Event.sync(ref, peer)
      assert FlowControl.event_cost(event) == 1
    end
  end

  describe "value_cost/1" do
    test "simple values have base cost of 1" do
      assert FlowControl.value_cost({:symbol, "test"}) == 1
      assert FlowControl.value_cost({:string, "hello"}) == 1
      assert FlowControl.value_cost(42) == 1
    end

    test "embedded refs add 1 per ref" do
      ref = Ref.new(:test_actor, 1)
      value = {:record, {{:symbol, "WithRef"}, [{:embedded, ref}]}}
      assert FlowControl.value_cost(value) == 2
    end

    test "nested refs accumulate" do
      ref1 = Ref.new(:test_actor, 1)
      ref2 = Ref.new(:test_actor, 2)
      value = {:record, {{:symbol, "TwoRefs"}, [{:embedded, ref1}, {:embedded, ref2}]}}
      # 1 base + 2 refs
      assert FlowControl.value_cost(value) == 3
    end
  end
end
