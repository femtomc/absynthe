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
end
