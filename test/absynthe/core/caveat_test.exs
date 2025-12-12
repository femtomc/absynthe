defmodule Absynthe.Core.CaveatTest do
  use ExUnit.Case, async: true

  alias Absynthe.Core.Caveat

  describe "rewrite/2" do
    test "creates a rewrite caveat" do
      pattern = {:symbol, "$"}
      template = {:ref, 0}

      caveat = Caveat.rewrite(pattern, template)

      assert caveat == {:rewrite, {:symbol, "$"}, {:ref, 0}}
    end
  end

  describe "alts/1" do
    test "creates an alternatives caveat" do
      c1 = Caveat.rewrite({:symbol, "$"}, {:ref, 0})
      c2 = Caveat.reject()

      caveat = Caveat.alts([c1, c2])

      assert caveat == {:alts, [{:rewrite, {:symbol, "$"}, {:ref, 0}}, :reject]}
    end
  end

  describe "reject/0" do
    test "creates a reject caveat" do
      assert Caveat.reject() == :reject
    end
  end

  describe "passthrough/0" do
    test "creates a passthrough caveat" do
      caveat = Caveat.passthrough()
      assert caveat == {:rewrite, {:symbol, "$"}, {:ref, 0}}
    end
  end

  describe "apply_template/2" do
    test "applies literal template" do
      template = {:lit, {:integer, 42}}

      assert Caveat.apply_template(template, []) == {:ok, {:integer, 42}}
    end

    test "applies ref template with valid index" do
      template = {:ref, 0}
      captures = [{:string, "hello"}]

      assert Caveat.apply_template(template, captures) == {:ok, {:string, "hello"}}
    end

    test "applies ref template with second capture" do
      template = {:ref, 1}
      captures = [{:string, "first"}, {:string, "second"}]

      assert Caveat.apply_template(template, captures) == {:ok, {:string, "second"}}
    end

    test "returns error for out-of-range ref" do
      template = {:ref, 5}
      captures = [{:string, "only one"}]

      assert Caveat.apply_template(template, captures) ==
               {:error, {:capture_out_of_range, 5, 1}}
    end

    test "applies record template" do
      template = {:record, {:lit, {:symbol, "Point"}}, [{:ref, 0}, {:ref, 1}]}
      captures = [{:integer, 10}, {:integer, 20}]

      assert Caveat.apply_template(template, captures) ==
               {:ok, {:record, {{:symbol, "Point"}, [{:integer, 10}, {:integer, 20}]}}}
    end

    test "applies sequence template" do
      template = {:sequence, [{:ref, 0}, {:lit, {:integer, 99}}]}
      captures = [{:string, "first"}]

      assert Caveat.apply_template(template, captures) ==
               {:ok, {:sequence, [{:string, "first"}, {:integer, 99}]}}
    end

    test "applies dictionary template" do
      template =
        {:dictionary, [{{:lit, {:symbol, "key"}}, {:ref, 0}}]}

      captures = [{:string, "value"}]

      assert Caveat.apply_template(template, captures) ==
               {:ok, {:dictionary, %{{:symbol, "key"} => {:string, "value"}}}}
    end

    test "applies set template" do
      template = {:set, [{:ref, 0}, {:ref, 1}]}
      captures = [{:integer, 1}, {:integer, 2}]

      assert {:ok, {:set, set}} = Caveat.apply_template(template, captures)
      assert MapSet.member?(set, {:integer, 1})
      assert MapSet.member?(set, {:integer, 2})
    end

    test "returns error for invalid template" do
      assert {:error, {:invalid_template, :invalid}} =
               Caveat.apply_template(:invalid, [])
    end
  end

  describe "validate/1" do
    test "validates reject caveat" do
      assert Caveat.validate(:reject) == :ok
    end

    test "validates valid rewrite caveat" do
      caveat = Caveat.rewrite({:symbol, "$"}, {:ref, 0})
      assert Caveat.validate(caveat) == :ok
    end

    test "validates rewrite caveat with multiple captures" do
      pattern = {:record, {{:symbol, "Pair"}, [{:symbol, "$"}, {:symbol, "$"}]}}
      template = {:ref, 1}

      caveat = Caveat.rewrite(pattern, template)
      assert Caveat.validate(caveat) == :ok
    end

    test "fails validation for out-of-range ref" do
      caveat = Caveat.rewrite({:symbol, "$"}, {:ref, 5})

      assert Caveat.validate(caveat) == {:error, {:capture_out_of_range, 5, 1}}
    end

    test "validates alts caveat" do
      c1 = Caveat.passthrough()
      c2 = Caveat.reject()

      caveat = Caveat.alts([c1, c2])
      assert Caveat.validate(caveat) == :ok
    end

    test "fails validation for invalid caveat in alts" do
      c1 = Caveat.passthrough()
      c2 = {:rewrite, {:symbol, "$"}, {:ref, 99}}

      caveat = Caveat.alts([c1, c2])
      assert Caveat.validate(caveat) == {:error, {:capture_out_of_range, 99, 1}}
    end

    test "fails validation for invalid structure" do
      assert Caveat.validate(:invalid) == {:error, :invalid_caveat_structure}
    end
  end

  describe "validate_attenuation/1" do
    test "validates empty attenuation" do
      assert Caveat.validate_attenuation([]) == :ok
    end

    test "validates single caveat attenuation" do
      attenuation = [Caveat.passthrough()]
      assert Caveat.validate_attenuation(attenuation) == :ok
    end

    test "validates multiple caveat attenuation" do
      attenuation = [Caveat.passthrough(), Caveat.passthrough()]
      assert Caveat.validate_attenuation(attenuation) == :ok
    end

    test "fails for invalid caveat in chain" do
      attenuation = [Caveat.passthrough(), {:rewrite, {:symbol, "$"}, {:ref, 99}}]

      assert Caveat.validate_attenuation(attenuation) ==
               {:error, {:capture_out_of_range, 99, 1}}
    end

    test "fails for non-list attenuation" do
      assert Caveat.validate_attenuation(:invalid) == {:error, :attenuation_must_be_list}
    end
  end
end
