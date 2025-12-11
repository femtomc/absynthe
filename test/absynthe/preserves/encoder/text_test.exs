defmodule Absynthe.Preserves.Encoder.TextTest do
  use ExUnit.Case, async: true

  alias Absynthe.Preserves.{Encoder, Value}

  describe "canonical ordering" do
    test "dictionary keys sorted by binary repr" do
      # Construct dictionary directly with Map - integer 2 sorts before string "10" in binary repr
      dict = {:dictionary, %{
        Value.string("10") => Value.string("str"),
        Value.integer(2) => Value.string("int")
      }}

      assert "{2: \"int\" \"10\": \"str\"}" == Encoder.Text.encode!(dict)
    end

    test "set elements sorted by binary repr" do
      # Construct set directly with MapSet - integer, string, symbol order per binary repr
      set = {:set, MapSet.new([Value.symbol("z"), Value.string("a"), Value.integer(1)])}

      # Ordering should be integer, string, symbol per Preserves ordering
      assert "\#{1 \"a\" z}" == Encoder.Text.encode!(set)
    end
  end
end
