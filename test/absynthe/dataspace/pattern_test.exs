defmodule Absynthe.Dataspace.PatternTest do
  use ExUnit.Case, async: true

  alias Absynthe.Dataspace.Pattern

  describe "compile/1 with captures" do
    test "anonymous capture ($) compiles to capture path" do
      pattern = {:symbol, "$"}
      compiled = Pattern.compile(pattern)

      assert compiled.constraints == []
      assert compiled.captures == [[]]
    end

    test "named capture ($name) compiles to capture path" do
      pattern = {:symbol, "$name"}
      compiled = Pattern.compile(pattern)

      assert compiled.constraints == []
      assert compiled.captures == [[]]
    end

    test "record with named capture compiles correctly" do
      # Pattern: <Person $name $age>
      pattern =
        {:record, {{:symbol, "Person"}, [{:symbol, "$name"}, {:symbol, "$age"}]}}

      compiled = Pattern.compile(pattern)

      # Should have constraint on label
      assert {:symbol, "Person"} in for({[:label], v} <- compiled.constraints, do: v)
      # Should have two captures for the named fields
      assert [{:field, 0}] in compiled.captures
      assert [{:field, 1}] in compiled.captures
    end

    test "mixed anonymous and named captures compile correctly" do
      # Pattern: <Message $ $content>
      pattern =
        {:record, {{:symbol, "Message"}, [{:symbol, "$"}, {:symbol, "$content"}]}}

      compiled = Pattern.compile(pattern)

      assert [{:field, 0}] in compiled.captures
      assert [{:field, 1}] in compiled.captures
    end
  end

  describe "match/2 with named captures" do
    test "named capture extracts value" do
      pattern = {:symbol, "$name"}
      compiled = Pattern.compile(pattern)
      value = {:string, "Alice"}

      assert {:ok, [{:string, "Alice"}]} = Pattern.match(compiled, value)
    end

    test "record with named captures extracts field values" do
      pattern =
        {:record, {{:symbol, "Person"}, [{:symbol, "$name"}, {:symbol, "$age"}]}}

      compiled = Pattern.compile(pattern)

      value =
        {:record, {{:symbol, "Person"}, [{:string, "Alice"}, {:integer, 30}]}}

      assert {:ok, captures} = Pattern.match(compiled, value)
      assert {:string, "Alice"} in captures
      assert {:integer, 30} in captures
    end

    test "record with named captures fails on label mismatch" do
      pattern =
        {:record, {{:symbol, "Person"}, [{:symbol, "$name"}, {:symbol, "$age"}]}}

      compiled = Pattern.compile(pattern)

      value =
        {:record, {{:symbol, "User"}, [{:string, "Alice"}, {:integer, 30}]}}

      assert :no_match = Pattern.match(compiled, value)
    end
  end

  describe "wildcard patterns" do
    test "wildcard (_) compiles without constraints or captures" do
      pattern = {:symbol, "_"}
      compiled = Pattern.compile(pattern)

      assert compiled.constraints == []
      assert compiled.captures == []
    end
  end

  describe "set patterns" do
    test "set with literal elements matches correctly" do
      # Pattern: set containing 1, 2, 3
      pattern = {:set, MapSet.new([{:integer, 1}, {:integer, 2}, {:integer, 3}])}
      compiled = Pattern.compile(pattern)

      # Matching value - same set
      value = {:set, MapSet.new([{:integer, 1}, {:integer, 2}, {:integer, 3}])}
      assert {:ok, []} = Pattern.match(compiled, value)

      # Different order should still match (sets are unordered)
      value2 = {:set, MapSet.new([{:integer, 3}, {:integer, 1}, {:integer, 2}])}
      assert {:ok, []} = Pattern.match(compiled, value2)

      # Non-matching value - different element
      value3 = {:set, MapSet.new([{:integer, 1}, {:integer, 2}, {:integer, 4}])}
      assert :no_match = Pattern.match(compiled, value3)
    end

    test "set with capture extracts element" do
      # Pattern: set containing capture and two literals
      pattern = {:set, MapSet.new([{:symbol, "$"}, {:integer, 1}, {:integer, 2}])}
      compiled = Pattern.compile(pattern)

      # Value: set with matching literals and a third element to capture
      value = {:set, MapSet.new([{:integer, 1}, {:integer, 2}, {:string, "captured"}])}
      assert {:ok, captures} = Pattern.match(compiled, value)
      # The capture should be the "captured" value
      assert {:string, "captured"} in captures
    end

    test "set with wildcard matches any element" do
      # Pattern: set containing wildcard and two literals
      pattern = {:set, MapSet.new([{:symbol, "_"}, {:integer, 1}, {:integer, 2}])}
      compiled = Pattern.compile(pattern)

      # Should match sets with those literals plus any third element
      value = {:set, MapSet.new([{:integer, 1}, {:integer, 2}, {:string, "anything"}])}
      assert {:ok, []} = Pattern.match(compiled, value)
    end

    test "empty set matches only empty sets" do
      pattern = {:set, MapSet.new([])}
      compiled = Pattern.compile(pattern)

      value = {:set, MapSet.new([])}
      assert {:ok, []} = Pattern.match(compiled, value)

      # Empty pattern should NOT match non-empty sets
      # (structural size equality is now enforced)
      value2 = {:set, MapSet.new([{:integer, 1}])}
      assert :no_match = Pattern.match(compiled, value2)
    end
  end

  describe "structural size equality (arity/length checks)" do
    test "record pattern rejects value with extra fields" do
      # Pattern: <Person $name>
      pattern = {:record, {{:symbol, "Person"}, [{:symbol, "$name"}]}}
      compiled = Pattern.compile(pattern)

      # Value with same arity - should match
      value1 = {:record, {{:symbol, "Person"}, [{:string, "Alice"}]}}
      assert {:ok, [{:string, "Alice"}]} = Pattern.match(compiled, value1)

      # Value with extra field - should NOT match
      value2 = {:record, {{:symbol, "Person"}, [{:string, "Alice"}, {:integer, 30}]}}
      assert :no_match = Pattern.match(compiled, value2)

      # Value with fewer fields - should NOT match
      value3 = {:record, {{:symbol, "Person"}, []}}
      assert :no_match = Pattern.match(compiled, value3)
    end

    test "sequence pattern rejects value with extra elements" do
      # Pattern: [$first, $second]
      pattern = {:sequence, [{:symbol, "$first"}, {:symbol, "$second"}]}
      compiled = Pattern.compile(pattern)

      # Value with same length - should match
      value1 = {:sequence, [{:integer, 1}, {:integer, 2}]}
      assert {:ok, [{:integer, 1}, {:integer, 2}]} = Pattern.match(compiled, value1)

      # Value with extra element - should NOT match
      value2 = {:sequence, [{:integer, 1}, {:integer, 2}, {:integer, 3}]}
      assert :no_match = Pattern.match(compiled, value2)

      # Value with fewer elements - should NOT match
      value3 = {:sequence, [{:integer, 1}]}
      assert :no_match = Pattern.match(compiled, value3)
    end

    test "empty sequence pattern only matches empty sequences" do
      pattern = {:sequence, []}
      compiled = Pattern.compile(pattern)

      assert {:ok, []} = Pattern.match(compiled, {:sequence, []})
      assert :no_match = Pattern.match(compiled, {:sequence, [{:integer, 1}]})
    end

    test "dictionary pattern rejects value with extra keys" do
      # Pattern: {name: $n}
      pattern = {:dictionary, [{{:symbol, "name"}, {:symbol, "$n"}}]}
      compiled = Pattern.compile(pattern)

      # Value with same keys - should match
      value1 = {:dictionary, [{{:symbol, "name"}, {:string, "Alice"}}]}
      assert {:ok, [{:string, "Alice"}]} = Pattern.match(compiled, value1)

      # Value with extra key - should NOT match
      value2 =
        {:dictionary,
         [{{:symbol, "name"}, {:string, "Alice"}}, {{:symbol, "age"}, {:integer, 30}}]}

      assert :no_match = Pattern.match(compiled, value2)

      # Value with different key - should NOT match
      value3 = {:dictionary, [{{:symbol, "age"}, {:integer, 30}}]}
      assert :no_match = Pattern.match(compiled, value3)
    end

    test "empty dictionary pattern only matches empty dictionaries" do
      pattern = {:dictionary, []}
      compiled = Pattern.compile(pattern)

      assert {:ok, []} = Pattern.match(compiled, {:dictionary, []})
      assert :no_match = Pattern.match(compiled, {:dictionary, [{{:symbol, "a"}, {:integer, 1}}]})
    end

    test "nested structure size checks work" do
      # Pattern: <Wrapper [$a, $b]>
      pattern =
        {:record, {{:symbol, "Wrapper"}, [{:sequence, [{:symbol, "$a"}, {:symbol, "$b"}]}]}}

      compiled = Pattern.compile(pattern)

      # Matching nested structure
      value1 =
        {:record, {{:symbol, "Wrapper"}, [{:sequence, [{:integer, 1}, {:integer, 2}]}]}}

      assert {:ok, [{:integer, 1}, {:integer, 2}]} = Pattern.match(compiled, value1)

      # Extra element in nested sequence - should NOT match
      value2 =
        {:record,
         {{:symbol, "Wrapper"}, [{:sequence, [{:integer, 1}, {:integer, 2}, {:integer, 3}]}]}}

      assert :no_match = Pattern.match(compiled, value2)

      # Extra field in outer record - should NOT match
      value3 =
        {:record,
         {{:symbol, "Wrapper"}, [{:sequence, [{:integer, 1}, {:integer, 2}]}, {:string, "extra"}]}}

      assert :no_match = Pattern.match(compiled, value3)
    end
  end
end
