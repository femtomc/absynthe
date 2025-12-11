defmodule AbsyntheTest do
  use ExUnit.Case
  doctest Absynthe

  describe "symbol/1" do
    test "creates symbol from string" do
      assert Absynthe.symbol("Person") == {:symbol, "Person"}
    end

    test "creates symbol from atom" do
      assert Absynthe.symbol(:greeting) == {:symbol, "greeting"}
    end
  end

  describe "record/2" do
    test "creates record with atom label" do
      result = Absynthe.record(:Person, ["Alice", 30])

      assert {:record, {{:symbol, "Person"}, fields}} = result
      assert [{:string, "Alice"}, {:integer, 30}] = fields
    end

    test "creates record with symbol label" do
      label = Absynthe.symbol("Point")
      result = Absynthe.record(label, [1, 2])

      assert {:record, {{:symbol, "Point"}, fields}} = result
      assert [{:integer, 1}, {:integer, 2}] = fields
    end
  end

  describe "encode!/1" do
    test "encodes boolean" do
      assert Absynthe.encode!({:boolean, true}) == <<0x81>>
      assert Absynthe.encode!({:boolean, false}) == <<0x80>>
    end

    test "encodes integer" do
      assert Absynthe.encode!({:integer, 42}) == <<0xB0, 0x01, 0x2A>>
    end
  end

  describe "wildcard/0" do
    test "returns wildcard symbol" do
      assert Absynthe.wildcard() == {:symbol, "_"}
    end
  end

  describe "capture/1" do
    test "returns anonymous capture" do
      assert Absynthe.capture() == {:symbol, "$"}
    end

    test "returns named capture" do
      assert Absynthe.capture(:name) == {:symbol, "$name"}
    end
  end
end
