defmodule Absynthe.Preserves.Decoder.TextTest do
  use ExUnit.Case, async: true

  alias Absynthe.Preserves.Decoder.Text
  alias Absynthe.Preserves.Value

  describe "decode/1 - booleans" do
    test "decodes #t as true" do
      assert {:ok, {:boolean, true}, ""} = Text.decode("#t")
    end

    test "decodes #f as false" do
      assert {:ok, {:boolean, false}, ""} = Text.decode("#f")
    end

    test "decodes boolean with remaining text" do
      assert {:ok, {:boolean, true}, " more"} = Text.decode("#t more")
    end
  end

  describe "decode/1 - integers" do
    test "decodes positive integer" do
      assert {:ok, {:integer, 42}, ""} = Text.decode("42")
    end

    test "decodes negative integer" do
      assert {:ok, {:integer, -17}, ""} = Text.decode("-17")
    end

    test "decodes zero" do
      assert {:ok, {:integer, 0}, ""} = Text.decode("0")
    end

    test "decodes hex integer" do
      assert {:ok, {:integer, 42}, ""} = Text.decode("0x2a")
    end

    test "decodes negative hex integer" do
      assert {:ok, {:integer, -42}, ""} = Text.decode("-0x2a")
    end

    test "decodes octal integer" do
      assert {:ok, {:integer, 42}, ""} = Text.decode("0o52")
    end

    test "decodes binary integer" do
      assert {:ok, {:integer, 42}, ""} = Text.decode("0b101010")
    end
  end

  describe "decode/1 - doubles" do
    test "decodes simple float" do
      assert {:ok, {:double, 3.14}, ""} = Text.decode("3.14")
    end

    test "decodes negative float" do
      assert {:ok, {:double, -0.5}, ""} = Text.decode("-0.5")
    end

    test "decodes scientific notation" do
      assert {:ok, {:double, 1.0e10}, ""} = Text.decode("1e10")
    end

    test "decodes negative exponent" do
      assert {:ok, {:double, value}, ""} = Text.decode("1.5e-3")
      assert_in_delta value, 0.0015, 0.0001
    end

    test "decodes infinity" do
      assert {:ok, {:double, value}, ""} = Text.decode("#inf")
      assert value == :math.pow(10, 1000)
    end

    test "decodes negative infinity" do
      assert {:ok, {:double, value}, ""} = Text.decode("#-inf")
      assert value == :math.pow(10, 1000) * -1
    end

    test "decodes NaN" do
      assert {:ok, {:double, value}, ""} = Text.decode("#nan")
      assert Float.nan?(value)
    end
  end

  describe "decode/1 - strings" do
    test "decodes simple string" do
      assert {:ok, {:string, "hello"}, ""} = Text.decode(~s("hello"))
    end

    test "decodes empty string" do
      assert {:ok, {:string, ""}, ""} = Text.decode(~s(""))
    end

    test "decodes string with escape sequences" do
      assert {:ok, {:string, "line1\nline2"}, ""} = Text.decode(~s("line1\\nline2"))
    end

    test "decodes string with tab" do
      assert {:ok, {:string, "a\tb"}, ""} = Text.decode(~s("a\\tb"))
    end

    test "decodes string with escaped quote" do
      assert {:ok, {:string, ~s(say "hi")}, ""} = Text.decode(~s("say \\"hi\\""))
    end

    test "decodes string with unicode escape" do
      assert {:ok, {:string, "A"}, ""} = Text.decode(~s("\\u0041"))
    end
  end

  describe "decode/1 - bytestrings" do
    test "decodes simple bytestring" do
      assert {:ok, {:binary, "hello"}, ""} = Text.decode(~s(#"hello"))
    end

    test "decodes hex bytestring" do
      assert {:ok, {:binary, <<0xDE, 0xAD, 0xBE, 0xEF>>}, ""} = Text.decode(~s(#x"deadbeef"))
    end

    test "decodes hex bytestring with whitespace" do
      assert {:ok, {:binary, <<0xDE, 0xAD, 0xBE, 0xEF>>}, ""} = Text.decode(~s(#x"de ad be ef"))
    end

    test "decodes base64 bytestring" do
      # "hello" in base64 is "aGVsbG8="
      assert {:ok, {:binary, "hello"}, ""} = Text.decode(~s(#[aGVsbG8=]))
    end
  end

  describe "decode/1 - symbols" do
    test "decodes simple symbol" do
      assert {:ok, {:symbol, "foo"}, ""} = Text.decode("foo")
    end

    test "decodes hyphenated symbol" do
      assert {:ok, {:symbol, "my-symbol"}, ""} = Text.decode("my-symbol")
    end

    test "decodes symbol with numbers" do
      assert {:ok, {:symbol, "var123"}, ""} = Text.decode("var123")
    end

    test "decodes special symbols" do
      assert {:ok, {:symbol, "+"}, ""} = Text.decode("+")
      assert {:ok, {:symbol, "*"}, ""} = Text.decode("*")
    end

    test "decodes quoted symbol" do
      assert {:ok, {:symbol, "with spaces"}, ""} = Text.decode(~s(|with spaces|))
    end

    test "decodes quoted symbol with escapes" do
      assert {:ok, {:symbol, "new\nline"}, ""} = Text.decode(~s(|new\\nline|))
    end
  end

  describe "decode/1 - sequences" do
    test "decodes empty sequence" do
      assert {:ok, {:sequence, []}, ""} = Text.decode("[]")
    end

    test "decodes sequence with integers" do
      assert {:ok, {:sequence, [{:integer, 1}, {:integer, 2}, {:integer, 3}]}, ""} =
               Text.decode("[1 2 3]")
    end

    test "decodes sequence with commas" do
      assert {:ok, {:sequence, [{:integer, 1}, {:integer, 2}, {:integer, 3}]}, ""} =
               Text.decode("[1, 2, 3]")
    end

    test "decodes nested sequences" do
      assert {:ok, {:sequence, [{:sequence, [{:integer, 1}]}, {:sequence, [{:integer, 2}]}]}, ""} =
               Text.decode("[[1] [2]]")
    end

    test "decodes sequence with mixed types" do
      assert {:ok, {:sequence, [{:integer, 1}, {:string, "two"}, {:boolean, true}]}, ""} =
               Text.decode(~s([1 "two" #t]))
    end
  end

  describe "decode/1 - sets" do
    test "decodes empty set" do
      assert {:ok, {:set, set}, ""} = Text.decode("#{}")
      assert MapSet.size(set) == 0
    end

    test "decodes set with integers" do
      assert {:ok, {:set, set}, ""} = Text.decode("#{1 2 3}")
      assert MapSet.member?(set, {:integer, 1})
      assert MapSet.member?(set, {:integer, 2})
      assert MapSet.member?(set, {:integer, 3})
    end

    test "decodes set with duplicates (removed)" do
      assert {:ok, {:set, set}, ""} = Text.decode("#{1 2 1}")
      assert MapSet.size(set) == 2
    end
  end

  describe "decode/1 - dictionaries" do
    test "decodes empty dictionary" do
      assert {:ok, {:dictionary, dict}, ""} = Text.decode("{}")
      assert map_size(dict) == 0
    end

    test "decodes dictionary with symbol keys" do
      assert {:ok, {:dictionary, dict}, ""} = Text.decode(~s({name: "Alice" age: 30}))
      assert dict[{:symbol, "name"}] == {:string, "Alice"}
      assert dict[{:symbol, "age"}] == {:integer, 30}
    end

    test "decodes dictionary with string keys (JSON style)" do
      assert {:ok, {:dictionary, dict}, ""} = Text.decode(~s({"key": "value"}))
      assert dict[{:string, "key"}] == {:string, "value"}
    end

    test "decodes nested dictionary" do
      assert {:ok, {:dictionary, dict}, ""} = Text.decode(~s({inner: {x: 1}}))
      assert {:dictionary, inner} = dict[{:symbol, "inner"}]
      assert inner[{:symbol, "x"}] == {:integer, 1}
    end
  end

  describe "decode/1 - records" do
    test "decodes record with symbol label" do
      assert {:ok, {:record, {{:symbol, "point"}, [{:integer, 10}, {:integer, 20}]}}, ""} =
               Text.decode("<point 10 20>")
    end

    test "decodes record with no fields" do
      assert {:ok, {:record, {{:symbol, "empty"}, []}}, ""} = Text.decode("<empty>")
    end

    test "decodes record with nested values" do
      assert {:ok, {:record, {{:symbol, "person"}, [{:string, "Alice"}, {:integer, 30}]}}, ""} =
               Text.decode(~s(<person "Alice" 30>))
    end

    test "decodes nested records" do
      assert {:ok,
              {:record,
               {{:symbol, "outer"}, [{:record, {{:symbol, "inner"}, [{:integer, 42}]}}]}}, ""} =
               Text.decode("<outer <inner 42>>")
    end
  end

  describe "decode/1 - embedded" do
    test "decodes embedded value" do
      assert {:ok, {:embedded, {:integer, 42}}, ""} = Text.decode("#!42")
    end

    test "decodes embedded record" do
      assert {:ok, {:embedded, {:record, {{:symbol, "ref"}, [{:integer, 123}]}}}, ""} =
               Text.decode("#!<ref 123>")
    end
  end

  describe "decode/1 - whitespace and comments" do
    test "handles leading whitespace" do
      assert {:ok, {:integer, 42}, ""} = Text.decode("  42")
    end

    test "handles trailing whitespace in remaining" do
      assert {:ok, {:integer, 42}, "  "} = Text.decode("42  ")
    end

    test "handles line comments" do
      assert {:ok, {:integer, 42}, ""} = Text.decode("42 ; this is a comment")
    end

    test "handles line comments before value" do
      assert {:ok, {:integer, 42}, ""} = Text.decode("; comment\n42")
    end

    test "handles value comments" do
      assert {:ok, {:integer, 2}, ""} = Text.decode("#;1 2")
    end

    test "handles multiple value comments" do
      assert {:ok, {:integer, 3}, ""} = Text.decode("#;1 #;2 3")
    end
  end

  describe "decode_all/1" do
    test "succeeds with only whitespace after value" do
      assert {:ok, {:integer, 42}} = Text.decode_all("42  ")
    end

    test "succeeds with comment after value" do
      assert {:ok, {:integer, 42}} = Text.decode_all("42 ; comment")
    end

    test "fails with extra content after value" do
      assert {:error, _reason, _pos} = Text.decode_all("42 extra")
    end
  end

  describe "decode_all!/1" do
    test "returns value on success" do
      assert {:integer, 42} = Text.decode_all!("42")
    end

    test "raises on error" do
      assert_raise RuntimeError, fn ->
        Text.decode_all!("42 extra")
      end
    end
  end

  describe "decode!/1" do
    test "returns tuple on success" do
      assert {{:integer, 42}, ""} = Text.decode!("42")
    end

    test "raises on error" do
      assert_raise RuntimeError, fn ->
        Text.decode!("invalid")
      end
    end
  end

  describe "complex examples" do
    test "decodes complex nested structure" do
      input = ~s(<person "Alice" 30 {city: "NYC" coords: <point 40.7 -74.0>}>)

      assert {:ok,
              {:record,
               {
                 {:symbol, "person"},
                 [
                   {:string, "Alice"},
                   {:integer, 30},
                   {:dictionary, dict}
                 ]
               }}, ""} = Text.decode(input)

      assert dict[{:symbol, "city"}] == {:string, "NYC"}

      assert {:record, {{:symbol, "point"}, [{:double, 40.7}, {:double, -74.0}]}} =
               dict[{:symbol, "coords"}]
    end

    test "decodes array of records" do
      input = ~s([<point 1 2> <point 3 4>])

      assert {:ok,
              {:sequence,
               [
                 {:record, {{:symbol, "point"}, [{:integer, 1}, {:integer, 2}]}},
                 {:record, {{:symbol, "point"}, [{:integer, 3}, {:integer, 4}]}}
               ]}, ""} = Text.decode(input)
    end
  end
end
