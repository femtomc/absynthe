defmodule Absynthe.Core.RewriteTest do
  use ExUnit.Case, async: true

  alias Absynthe.Core.{Caveat, Rewrite}

  describe "apply_attenuation/2" do
    test "empty attenuation passes value through unchanged" do
      value = {:integer, 42}
      assert Rewrite.apply_attenuation(value, []) == {:ok, {:integer, 42}}
    end

    test "nil attenuation passes value through unchanged" do
      value = {:integer, 42}
      assert Rewrite.apply_attenuation(value, nil) == {:ok, {:integer, 42}}
    end

    test "passthrough caveat passes value through" do
      value = {:string, "hello"}
      attenuation = [Caveat.passthrough()]

      assert Rewrite.apply_attenuation(value, attenuation) == {:ok, {:string, "hello"}}
    end

    test "reject caveat rejects all values" do
      value = {:integer, 42}
      attenuation = [:reject]

      assert Rewrite.apply_attenuation(value, attenuation) == :rejected
    end

    test "rewrite caveat transforms matching value" do
      # Pattern: match Person record, capture the name
      pattern = {:record, {{:symbol, "Person"}, [{:symbol, "$"}]}}
      # Template: extract just the name
      template = {:ref, 0}
      caveat = Caveat.rewrite(pattern, template)

      value = {:record, {{:symbol, "Person"}, [{:string, "Alice"}]}}

      assert Rewrite.apply_attenuation(value, [caveat]) == {:ok, {:string, "Alice"}}
    end

    test "rewrite caveat rejects non-matching value" do
      # Pattern: match Person records
      pattern = {:record, {{:symbol, "Person"}, [{:symbol, "$"}]}}
      template = {:ref, 0}
      caveat = Caveat.rewrite(pattern, template)

      # Value: an Animal record (doesn't match)
      value = {:record, {{:symbol, "Animal"}, [{:string, "Dog"}]}}

      assert Rewrite.apply_attenuation(value, [caveat]) == :rejected
    end

    test "multiple caveats apply in sequence" do
      # First caveat: match any value, wrap in record
      c1 =
        Caveat.rewrite(
          {:symbol, "$"},
          {:record, {:lit, {:symbol, "Wrapped"}}, [{:ref, 0}]}
        )

      # Second caveat: match Wrapped record, extract inner value
      c2 =
        Caveat.rewrite(
          {:record, {{:symbol, "Wrapped"}, [{:symbol, "$"}]}},
          {:ref, 0}
        )

      value = {:string, "test"}

      # After c1: {:record, {{:symbol, "Wrapped"}, [{:string, "test"}]}}
      # After c2: {:string, "test"}
      assert Rewrite.apply_attenuation(value, [c1, c2]) == {:ok, {:string, "test"}}
    end

    test "chain stops at first rejection" do
      c1 = Caveat.reject()
      c2 = Caveat.passthrough()

      value = {:integer, 42}

      assert Rewrite.apply_attenuation(value, [c1, c2]) == :rejected
    end

    test "rejection in middle of chain" do
      c1 = Caveat.passthrough()
      c2 = Caveat.reject()
      c3 = Caveat.passthrough()

      value = {:integer, 42}

      assert Rewrite.apply_attenuation(value, [c1, c2, c3]) == :rejected
    end
  end

  describe "apply_caveat/2" do
    test "reject caveat always rejects" do
      assert Rewrite.apply_caveat({:integer, 42}, :reject) == :rejected
    end

    test "rewrite with matching pattern and identity template" do
      # Match anything, pass through unchanged
      caveat = {:rewrite, {:symbol, "$"}, {:ref, 0}}

      assert Rewrite.apply_caveat({:string, "hello"}, caveat) == {:ok, {:string, "hello"}}
    end

    test "rewrite with matching record pattern" do
      pattern = {:record, {{:symbol, "Point"}, [{:symbol, "$"}, {:symbol, "$"}]}}
      # Construct new record with swapped fields
      template = {:record, {:lit, {:symbol, "Point"}}, [{:ref, 1}, {:ref, 0}]}
      caveat = {:rewrite, pattern, template}

      value = {:record, {{:symbol, "Point"}, [{:integer, 10}, {:integer, 20}]}}

      assert Rewrite.apply_caveat(value, caveat) ==
               {:ok, {:record, {{:symbol, "Point"}, [{:integer, 20}, {:integer, 10}]}}}
    end

    test "rewrite with non-matching pattern rejects" do
      pattern = {:record, {{:symbol, "Expected"}, [{:symbol, "$"}]}}
      template = {:ref, 0}
      caveat = {:rewrite, pattern, template}

      value = {:record, {{:symbol, "Actual"}, [{:integer, 42}]}}

      assert Rewrite.apply_caveat(value, caveat) == :rejected
    end

    test "alts caveat tries alternatives" do
      # First alternative: match integers
      c1 =
        Caveat.rewrite(
          {:symbol, "$"},
          {:record, {:lit, {:symbol, "Int"}}, [{:ref, 0}]}
        )

      # Second alternative: match strings
      c2 =
        Caveat.rewrite(
          {:symbol, "$"},
          {:record, {:lit, {:symbol, "Str"}}, [{:ref, 0}]}
        )

      caveat = Caveat.alts([c1, c2])

      # Both will match since both use wildcard $
      # First match wins
      assert Rewrite.apply_caveat({:integer, 42}, caveat) ==
               {:ok, {:record, {{:symbol, "Int"}, [{:integer, 42}]}}}
    end

    test "alts caveat rejects when no alternatives match" do
      # Alternative that only matches Person records
      c1 =
        Caveat.rewrite(
          {:record, {{:symbol, "Person"}, [{:symbol, "$"}]}},
          {:ref, 0}
        )

      # Alternative that only matches Animal records
      c2 =
        Caveat.rewrite(
          {:record, {{:symbol, "Animal"}, [{:symbol, "$"}]}},
          {:ref, 0}
        )

      caveat = Caveat.alts([c1, c2])

      # A Car record won't match either
      value = {:record, {{:symbol, "Car"}, [{:string, "Ford"}]}}

      assert Rewrite.apply_caveat(value, caveat) == :rejected
    end
  end

  describe "accepts?/2" do
    test "returns true for empty attenuation" do
      assert Rewrite.accepts?({:integer, 42}, []) == true
    end

    test "returns true for passthrough caveat" do
      assert Rewrite.accepts?({:integer, 42}, [Caveat.passthrough()]) == true
    end

    test "returns false for reject caveat" do
      assert Rewrite.accepts?({:integer, 42}, [:reject]) == false
    end

    test "returns true for matching pattern" do
      pattern = {:record, {{:symbol, "Person"}, [{:symbol, "$"}]}}
      caveat = Caveat.rewrite(pattern, {:ref, 0})

      value = {:record, {{:symbol, "Person"}, [{:string, "Alice"}]}}

      assert Rewrite.accepts?(value, [caveat]) == true
    end

    test "returns false for non-matching pattern" do
      pattern = {:record, {{:symbol, "Person"}, [{:symbol, "$"}]}}
      caveat = Caveat.rewrite(pattern, {:ref, 0})

      value = {:record, {{:symbol, "Animal"}, [{:string, "Dog"}]}}

      assert Rewrite.accepts?(value, [caveat]) == false
    end
  end

  describe "compose/2" do
    test "composes outer with nil inner" do
      outer = [Caveat.passthrough()]
      assert Rewrite.compose(outer, nil) == [Caveat.passthrough()]
    end

    test "composes outer with inner" do
      outer = [Caveat.passthrough()]
      inner = [Caveat.reject()]

      assert Rewrite.compose(outer, inner) == [Caveat.passthrough(), Caveat.reject()]
    end

    test "empty outer with inner" do
      outer = []
      inner = [Caveat.passthrough()]

      assert Rewrite.compose(outer, inner) == [Caveat.passthrough()]
    end
  end
end
