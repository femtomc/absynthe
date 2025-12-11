defmodule Absynthe.Preserves.PatternTest do
  use ExUnit.Case, async: true

  alias Absynthe.Preserves.Pattern
  alias Absynthe.Preserves.Value

  doctest Absynthe.Preserves.Pattern

  describe "pattern constructors" do
    test "discard/0 returns :_" do
      assert Pattern.discard() == :_
    end

    test "capture/1 returns capture tuple" do
      assert Pattern.capture(:x) == {:capture, :x}
      assert Pattern.capture(:name) == {:capture, :name}
    end

    test "bind/2 returns bind tuple" do
      pattern = Value.integer(42)
      assert Pattern.bind(:num, pattern) == {:bind, :num, pattern}
    end
  end

  describe "pattern?/1" do
    test "recognizes discard patterns" do
      assert Pattern.pattern?(:_)
    end

    test "recognizes capture patterns" do
      assert Pattern.pattern?({:capture, :x})
      refute Pattern.pattern?({:capture, "not an atom"})
    end

    test "recognizes bind patterns" do
      assert Pattern.pattern?({:bind, :x, Value.integer(42)})
      refute Pattern.pattern?({:bind, "not atom", Value.integer(42)})
    end

    test "recognizes literal value patterns" do
      assert Pattern.pattern?(Value.boolean(true))
      assert Pattern.pattern?(Value.integer(42))
      assert Pattern.pattern?(Value.double(3.14))
      assert Pattern.pattern?(Value.string("hello"))
      assert Pattern.pattern?(Value.binary(<<1, 2, 3>>))
      assert Pattern.pattern?(Value.symbol("name"))
      assert Pattern.pattern?(Value.embedded(:ref))
    end

    test "recognizes compound patterns" do
      assert Pattern.pattern?(Value.sequence([Value.integer(1), :_]))
      assert Pattern.pattern?(Value.record(Value.symbol("test"), [{:capture, :x}]))
      assert Pattern.pattern?(Value.set([Value.integer(1)]))
      assert Pattern.pattern?(Value.dictionary([{Value.symbol("key"), :_}]))
    end

    test "rejects invalid patterns" do
      refute Pattern.pattern?("not a pattern")
      refute Pattern.pattern?(42)
      refute Pattern.pattern?({:unknown, :pattern})
    end
  end

  describe "variables/1" do
    test "extracts no variables from literals" do
      assert Pattern.variables(Value.integer(42)) == []
      assert Pattern.variables(Value.string("test")) == []
    end

    test "extracts no variables from discard" do
      assert Pattern.variables(:_) == []
    end

    test "extracts single variable from capture" do
      assert Pattern.variables({:capture, :x}) == [:x]
    end

    test "extracts variable from bind" do
      assert Pattern.variables({:bind, :x, Value.integer(42)}) == [:x]
    end

    test "extracts variables from sequence" do
      pattern = Value.sequence([{:capture, :first}, :_, {:capture, :last}])
      assert Pattern.variables(pattern) == [:first, :last]
    end

    test "extracts variables from record" do
      pattern = Value.record(
        Value.symbol("point"),
        [{:capture, :x}, {:capture, :y}]
      )
      assert Pattern.variables(pattern) == [:x, :y]
    end

    test "extracts variables from nested patterns" do
      pattern = Value.record(
        {:capture, :label},
        [
          Value.sequence([{:capture, :a}, {:capture, :b}]),
          {:capture, :c}
        ]
      )
      vars = Pattern.variables(pattern)
      assert :label in vars
      assert :a in vars
      assert :b in vars
      assert :c in vars
    end

    test "deduplicates repeated variables" do
      pattern = Value.sequence([{:capture, :x}, {:capture, :x}])
      assert Pattern.variables(pattern) == [:x]
    end

    test "extracts variables from bind with nested captures" do
      pattern = Pattern.bind(:point,
        Value.record(
          Value.symbol("point"),
          [{:capture, :x}, {:capture, :y}]
        )
      )
      assert Pattern.variables(pattern) == [:point, :x, :y]
    end
  end

  describe "match/2 - discard patterns" do
    test "discard matches any value" do
      assert {:ok, %{}} = Pattern.match(:_, Value.integer(42))
      assert {:ok, %{}} = Pattern.match(:_, Value.string("anything"))
      assert {:ok, %{}} = Pattern.match(:_, Value.boolean(true))
    end
  end

  describe "match/2 - capture patterns" do
    test "capture matches and binds value" do
      assert {:ok, %{x: {:integer, 42}}} = Pattern.match({:capture, :x}, Value.integer(42))
      assert {:ok, %{name: {:string, "Alice"}}} = Pattern.match({:capture, :name}, Value.string("Alice"))
    end

    test "capture with same variable must match same value" do
      pattern = Value.sequence([{:capture, :x}, {:capture, :x}])
      value = Value.sequence([Value.integer(42), Value.integer(42)])
      assert {:ok, %{x: {:integer, 42}}} = Pattern.match(pattern, value)
    end

    test "capture with same variable fails on different values" do
      pattern = Value.sequence([{:capture, :x}, {:capture, :x}])
      value = Value.sequence([Value.integer(42), Value.integer(43)])
      assert :error = Pattern.match(pattern, value)
    end
  end

  describe "match/2 - bind patterns" do
    test "bind matches pattern and captures result" do
      pattern = Pattern.bind(:num, Value.integer(42))
      value = Value.integer(42)
      assert {:ok, %{num: {:integer, 42}}} = Pattern.match(pattern, value)
    end

    test "bind fails when pattern doesn't match" do
      pattern = Pattern.bind(:num, Value.integer(42))
      value = Value.integer(43)
      assert :error = Pattern.match(pattern, value)
    end

    test "bind works with nested captures" do
      pattern = Pattern.bind(:point,
        Value.record(
          Value.symbol("point"),
          [{:capture, :x}, {:capture, :y}]
        )
      )
      value = Value.record(
        Value.symbol("point"),
        [Value.integer(10), Value.integer(20)]
      )
      {:ok, bindings} = Pattern.match(pattern, value)
      assert bindings[:point] == value
      assert bindings[:x] == Value.integer(10)
      assert bindings[:y] == Value.integer(20)
    end
  end

  describe "match/2 - literal patterns" do
    test "exact match succeeds" do
      assert {:ok, %{}} = Pattern.match(Value.integer(42), Value.integer(42))
      assert {:ok, %{}} = Pattern.match(Value.string("test"), Value.string("test"))
      assert {:ok, %{}} = Pattern.match(Value.boolean(true), Value.boolean(true))
    end

    test "different values fail" do
      assert :error = Pattern.match(Value.integer(42), Value.integer(43))
      assert :error = Pattern.match(Value.string("test"), Value.string("other"))
      assert :error = Pattern.match(Value.boolean(true), Value.boolean(false))
    end

    test "different types fail" do
      assert :error = Pattern.match(Value.integer(42), Value.string("42"))
      assert :error = Pattern.match(Value.symbol("name"), Value.string("name"))
    end
  end

  describe "match/2 - record patterns" do
    test "matches record with exact label and fields" do
      pattern = Value.record(Value.symbol("point"), [Value.integer(10), Value.integer(20)])
      value = Value.record(Value.symbol("point"), [Value.integer(10), Value.integer(20)])
      assert {:ok, %{}} = Pattern.match(pattern, value)
    end

    test "fails on label mismatch" do
      pattern = Value.record(Value.symbol("point"), [Value.integer(10)])
      value = Value.record(Value.symbol("other"), [Value.integer(10)])
      assert :error = Pattern.match(pattern, value)
    end

    test "fails on arity mismatch" do
      pattern = Value.record(Value.symbol("point"), [Value.integer(10)])
      value = Value.record(Value.symbol("point"), [Value.integer(10), Value.integer(20)])
      assert :error = Pattern.match(pattern, value)
    end

    test "matches with captures in fields" do
      pattern = Value.record(
        Value.symbol("person"),
        [{:capture, :name}, {:capture, :age}]
      )
      value = Value.record(
        Value.symbol("person"),
        [Value.string("Alice"), Value.integer(30)]
      )
      assert {:ok, %{name: {:string, "Alice"}, age: {:integer, 30}}} = Pattern.match(pattern, value)
    end

    test "matches with mixed patterns in fields" do
      pattern = Value.record(
        Value.symbol("user"),
        [{:capture, :id}, Value.string("active"), :_]
      )
      value = Value.record(
        Value.symbol("user"),
        [Value.integer(123), Value.string("active"), Value.boolean(true)]
      )
      assert {:ok, %{id: {:integer, 123}}} = Pattern.match(pattern, value)
    end
  end

  describe "match/2 - sequence patterns" do
    test "matches sequences with exact length" do
      pattern = Value.sequence([Value.integer(1), Value.integer(2), Value.integer(3)])
      value = Value.sequence([Value.integer(1), Value.integer(2), Value.integer(3)])
      assert {:ok, %{}} = Pattern.match(pattern, value)
    end

    test "fails on length mismatch" do
      pattern = Value.sequence([Value.integer(1), Value.integer(2)])
      value = Value.sequence([Value.integer(1), Value.integer(2), Value.integer(3)])
      assert :error = Pattern.match(pattern, value)
    end

    test "matches with captures" do
      pattern = Value.sequence([{:capture, :first}, :_, {:capture, :last}])
      value = Value.sequence([Value.integer(1), Value.integer(2), Value.integer(3)])
      assert {:ok, %{first: {:integer, 1}, last: {:integer, 3}}} = Pattern.match(pattern, value)
    end

    test "matches empty sequences" do
      pattern = Value.sequence([])
      value = Value.sequence([])
      assert {:ok, %{}} = Pattern.match(pattern, value)
    end
  end

  describe "match/2 - set patterns" do
    test "matches when pattern is subset of value" do
      pattern = Value.set([Value.integer(1), Value.integer(2)])
      value = Value.set([Value.integer(1), Value.integer(2), Value.integer(3)])
      assert {:ok, %{}} = Pattern.match(pattern, value)
    end

    test "matches when pattern equals value" do
      pattern = Value.set([Value.integer(1), Value.integer(2)])
      value = Value.set([Value.integer(1), Value.integer(2)])
      assert {:ok, %{}} = Pattern.match(pattern, value)
    end

    test "fails when pattern has element not in value" do
      pattern = Value.set([Value.integer(1), Value.integer(4)])
      value = Value.set([Value.integer(1), Value.integer(2), Value.integer(3)])
      assert :error = Pattern.match(pattern, value)
    end

    test "matches with captures" do
      pattern = Value.set([{:capture, :x}])
      value = Value.set([Value.integer(42)])
      assert {:ok, %{x: {:integer, 42}}} = Pattern.match(pattern, value)
    end

    test "matches empty sets" do
      pattern = Value.set([])
      value = Value.set([])
      assert {:ok, %{}} = Pattern.match(pattern, value)
    end
  end

  describe "match/2 - dictionary patterns" do
    test "matches when pattern keys are subset of value keys" do
      pattern = Value.dictionary([
        {Value.symbol("name"), Value.string("Alice")}
      ])
      value = Value.dictionary([
        {Value.symbol("name"), Value.string("Alice")},
        {Value.symbol("age"), Value.integer(30)}
      ])
      assert {:ok, %{}} = Pattern.match(pattern, value)
    end

    test "fails when pattern key not in value" do
      pattern = Value.dictionary([
        {Value.symbol("missing"), Value.string("value")}
      ])
      value = Value.dictionary([
        {Value.symbol("name"), Value.string("Alice")}
      ])
      assert :error = Pattern.match(pattern, value)
    end

    test "fails when value doesn't match for existing key" do
      pattern = Value.dictionary([
        {Value.symbol("name"), Value.string("Bob")}
      ])
      value = Value.dictionary([
        {Value.symbol("name"), Value.string("Alice")}
      ])
      assert :error = Pattern.match(pattern, value)
    end

    test "matches with captures in values" do
      pattern = Value.dictionary([
        {Value.symbol("name"), {:capture, :name}},
        {Value.symbol("age"), {:capture, :age}}
      ])
      value = Value.dictionary([
        {Value.symbol("name"), Value.string("Alice")},
        {Value.symbol("age"), Value.integer(30)}
      ])
      assert {:ok, %{name: {:string, "Alice"}, age: {:integer, 30}}} = Pattern.match(pattern, value)
    end

    test "matches empty dictionaries" do
      pattern = Value.dictionary([])
      value = Value.dictionary([])
      assert {:ok, %{}} = Pattern.match(pattern, value)
    end
  end

  describe "match?/2" do
    test "returns true for successful match" do
      assert Pattern.match?(Value.integer(42), Value.integer(42))
      assert Pattern.match?(:_, Value.string("anything"))
      assert Pattern.match?({:capture, :x}, Value.boolean(true))
    end

    test "returns false for failed match" do
      refute Pattern.match?(Value.integer(42), Value.integer(43))
      refute Pattern.match?(Value.string("test"), Value.integer(1))
    end
  end

  describe "matches/2" do
    test "filters values to those matching pattern" do
      pattern = Value.record(
        Value.symbol("user"),
        [:_, Value.string("active")]
      )
      values = [
        Value.record(
          Value.symbol("user"),
          [Value.integer(1), Value.string("active")]
        ),
        Value.record(
          Value.symbol("user"),
          [Value.integer(2), Value.string("inactive")]
        ),
        Value.record(
          Value.symbol("user"),
          [Value.integer(3), Value.string("active")]
        ),
        Value.integer(999)
      ]

      results = Pattern.matches(pattern, values)
      assert length(results) == 2
      assert Enum.at(results, 0) == Value.record(Value.symbol("user"), [Value.integer(1), Value.string("active")])
      assert Enum.at(results, 1) == Value.record(Value.symbol("user"), [Value.integer(3), Value.string("active")])
    end

    test "returns empty list when no matches" do
      pattern = Value.integer(999)
      values = [Value.integer(1), Value.integer(2), Value.integer(3)]
      assert Pattern.matches(pattern, values) == []
    end

    test "returns all when all match" do
      pattern = :_
      values = [Value.integer(1), Value.string("test"), Value.boolean(true)]
      assert Pattern.matches(pattern, values) == values
    end
  end

  describe "find_match/2" do
    test "returns first match with bindings" do
      pattern = {:capture, :x}
      values = [Value.integer(1), Value.integer(2), Value.integer(3)]
      assert {:ok, {:integer, 1}, %{x: {:integer, 1}}} = Pattern.find_match(pattern, values)
    end

    test "returns error when no match found" do
      pattern = Value.integer(999)
      values = [Value.integer(1), Value.integer(2), Value.integer(3)]
      assert :error = Pattern.find_match(pattern, values)
    end

    test "finds first matching complex pattern" do
      pattern = Value.record(Value.symbol("ok"), [{:capture, :result}])
      values = [
        Value.record(Value.symbol("error"), [Value.string("failed")]),
        Value.record(Value.symbol("ok"), [Value.integer(42)]),
        Value.record(Value.symbol("ok"), [Value.integer(43)])
      ]
      assert {:ok, result, bindings} = Pattern.find_match(pattern, values)
      assert result == Value.record(Value.symbol("ok"), [Value.integer(42)])
      assert bindings == %{result: {:integer, 42}}
    end
  end

  describe "complex nested patterns" do
    test "matches deeply nested structures" do
      pattern = Value.record(
        Value.symbol("response"),
        [
          {:capture, :status},
          Value.dictionary([
            {Value.symbol("data"), Value.sequence([{:capture, :first}, :_])},
            {Value.symbol("count"), {:capture, :count}}
          ])
        ]
      )

      value = Value.record(
        Value.symbol("response"),
        [
          Value.integer(200),
          Value.dictionary([
            {Value.symbol("data"), Value.sequence([Value.string("item1"), Value.string("item2")])},
            {Value.symbol("count"), Value.integer(2)},
            {Value.symbol("extra"), Value.boolean(true)}
          ])
        ]
      )

      {:ok, bindings} = Pattern.match(pattern, value)
      assert bindings[:status] == Value.integer(200)
      assert bindings[:first] == Value.string("item1")
      assert bindings[:count] == Value.integer(2)
    end

    test "matches records with captured labels" do
      pattern = Value.record({:capture, :label}, [{:capture, :value}])
      value = Value.record(Value.symbol("custom"), [Value.integer(42)])

      {:ok, bindings} = Pattern.match(pattern, value)
      assert bindings[:label] == Value.symbol("custom")
      assert bindings[:value] == Value.integer(42)
    end

    test "matches with multiple levels of bind" do
      inner_pattern = Value.record(
        Value.symbol("inner"),
        [{:capture, :x}]
      )
      middle_pattern = Pattern.bind(:middle, inner_pattern)
      outer_pattern = Pattern.bind(:outer, middle_pattern)

      value = Value.record(Value.symbol("inner"), [Value.integer(42)])

      {:ok, bindings} = Pattern.match(outer_pattern, value)
      assert bindings[:outer] == value
      assert bindings[:middle] == value
      assert bindings[:x] == Value.integer(42)
    end
  end
end
