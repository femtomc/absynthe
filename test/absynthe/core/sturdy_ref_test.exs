defmodule Absynthe.Core.SturdyRefTest do
  use ExUnit.Case, async: true

  alias Absynthe.Core.{SturdyRef, Caveat, Ref}

  # Generate a fresh key for each test
  defp test_key, do: :crypto.strong_rand_bytes(32)

  describe "mint/2" do
    test "creates a SturdyRef with a 16-byte signature" do
      key = test_key()
      {:ok, sref} = SturdyRef.mint({:symbol, "my-service"}, key)

      assert sref.oid == {:symbol, "my-service"}
      assert byte_size(sref.sig) == 16
      assert sref.caveats == []
    end

    test "produces deterministic signatures for the same oid and key" do
      key = test_key()
      {:ok, sref1} = SturdyRef.mint({:symbol, "svc"}, key)
      {:ok, sref2} = SturdyRef.mint({:symbol, "svc"}, key)

      assert sref1.sig == sref2.sig
    end

    test "produces different signatures for different oids" do
      key = test_key()
      {:ok, sref1} = SturdyRef.mint({:symbol, "svc1"}, key)
      {:ok, sref2} = SturdyRef.mint({:symbol, "svc2"}, key)

      assert sref1.sig != sref2.sig
    end

    test "produces different signatures for different keys" do
      {:ok, sref1} = SturdyRef.mint({:symbol, "svc"}, test_key())
      {:ok, sref2} = SturdyRef.mint({:symbol, "svc"}, test_key())

      assert sref1.sig != sref2.sig
    end

    test "accepts various Preserves value types as oid" do
      key = test_key()

      # Symbol
      {:ok, sref1} = SturdyRef.mint({:symbol, "svc"}, key)
      assert sref1.oid == {:symbol, "svc"}

      # String
      {:ok, sref2} = SturdyRef.mint({:string, "my-service"}, key)
      assert sref2.oid == {:string, "my-service"}

      # Integer
      {:ok, sref3} = SturdyRef.mint({:integer, 42}, key)
      assert sref3.oid == {:integer, 42}

      # Record
      oid = {:record, {{:symbol, "ServiceId"}, [{:string, "main"}]}}
      {:ok, sref4} = SturdyRef.mint(oid, key)
      assert sref4.oid == oid
    end

    test "mint!/2 raises on encoding failure" do
      key = test_key()
      # This should work - valid Preserves value
      sref = SturdyRef.mint!({:symbol, "svc"}, key)
      assert sref.oid == {:symbol, "svc"}
    end
  end

  describe "validate/2" do
    test "validates a freshly minted SturdyRef" do
      key = test_key()
      {:ok, sref} = SturdyRef.mint({:symbol, "svc"}, key)

      assert SturdyRef.validate(sref, key) == :ok
    end

    test "rejects SturdyRef with wrong key" do
      key1 = test_key()
      key2 = test_key()
      {:ok, sref} = SturdyRef.mint({:symbol, "svc"}, key1)

      assert {:error, :invalid_signature} = SturdyRef.validate(sref, key2)
    end

    test "rejects SturdyRef with tampered oid" do
      key = test_key()
      {:ok, sref} = SturdyRef.mint({:symbol, "svc"}, key)

      tampered = %{sref | oid: {:symbol, "different"}}
      assert {:error, :invalid_signature} = SturdyRef.validate(tampered, key)
    end

    test "rejects SturdyRef with tampered signature" do
      key = test_key()
      {:ok, sref} = SturdyRef.mint({:symbol, "svc"}, key)

      tampered = %{sref | sig: :crypto.strong_rand_bytes(16)}
      assert {:error, :invalid_signature} = SturdyRef.validate(tampered, key)
    end

    test "rejects SturdyRef with tampered caveats" do
      key = test_key()
      {:ok, sref} = SturdyRef.mint({:symbol, "svc"}, key)
      {:ok, attenuated} = SturdyRef.attenuate(sref, Caveat.passthrough(), key)

      # Tamper by adding an extra caveat without updating signature
      tampered = %{attenuated | caveats: attenuated.caveats ++ [Caveat.reject()]}
      assert {:error, :invalid_signature} = SturdyRef.validate(tampered, key)
    end
  end

  describe "valid?/2" do
    test "returns true for valid SturdyRef" do
      key = test_key()
      {:ok, sref} = SturdyRef.mint({:symbol, "svc"}, key)

      assert SturdyRef.valid?(sref, key)
    end

    test "returns false for invalid SturdyRef" do
      key = test_key()
      {:ok, sref} = SturdyRef.mint({:symbol, "svc"}, key)

      tampered = %{sref | oid: {:symbol, "tampered"}}
      refute SturdyRef.valid?(tampered, key)
    end
  end

  describe "attenuate/3" do
    test "adds a caveat to the chain" do
      key = test_key()
      {:ok, sref} = SturdyRef.mint({:symbol, "svc"}, key)

      {:ok, attenuated} = SturdyRef.attenuate(sref, Caveat.passthrough(), key)

      assert length(attenuated.caveats) == 1
      assert hd(attenuated.caveats) == Caveat.passthrough()
    end

    test "produces a valid signature after attenuation" do
      key = test_key()
      {:ok, sref} = SturdyRef.mint({:symbol, "svc"}, key)

      {:ok, attenuated} = SturdyRef.attenuate(sref, Caveat.passthrough(), key)

      assert SturdyRef.validate(attenuated, key) == :ok
    end

    test "supports multiple attenuations" do
      key = test_key()
      {:ok, sref} = SturdyRef.mint({:symbol, "svc"}, key)

      {:ok, a1} = SturdyRef.attenuate(sref, Caveat.passthrough(), key)
      {:ok, a2} = SturdyRef.attenuate(a1, Caveat.reject(), key)
      {:ok, a3} = SturdyRef.attenuate(a2, Caveat.passthrough(), key)

      assert length(a3.caveats) == 3
      assert SturdyRef.validate(a3, key) == :ok
    end

    test "each attenuation changes the signature" do
      key = test_key()
      {:ok, sref} = SturdyRef.mint({:symbol, "svc"}, key)
      {:ok, a1} = SturdyRef.attenuate(sref, Caveat.passthrough(), key)
      {:ok, a2} = SturdyRef.attenuate(a1, Caveat.passthrough(), key)

      assert sref.sig != a1.sig
      assert a1.sig != a2.sig
    end

    test "validates caveat before attenuating" do
      key = test_key()
      {:ok, sref} = SturdyRef.mint({:symbol, "svc"}, key)

      # Invalid caveat: ref index out of range
      invalid_caveat = {:rewrite, {:symbol, "$"}, {:ref, 99}}

      assert {:error, {:capture_out_of_range, 99, 1}} =
               SturdyRef.attenuate(sref, invalid_caveat, key)
    end

    test "attenuate!/3 raises on invalid caveat" do
      key = test_key()
      {:ok, sref} = SturdyRef.mint({:symbol, "svc"}, key)

      invalid_caveat = {:rewrite, {:symbol, "$"}, {:ref, 99}}

      assert_raise ArgumentError, fn ->
        SturdyRef.attenuate!(sref, invalid_caveat, key)
      end
    end
  end

  describe "materialize/3" do
    test "resolves valid SturdyRef to a Ref" do
      key = test_key()
      {:ok, sref} = SturdyRef.mint({:symbol, "my-entity"}, key)

      resolver = fn {:symbol, "my-entity"} -> {:ok, {:my_actor, :my_entity}} end

      {:ok, ref} = SturdyRef.materialize(sref, key, resolver)

      assert ref.actor_id == :my_actor
      assert ref.entity_id == :my_entity
      assert ref.attenuation == nil
    end

    test "includes attenuation in materialized Ref" do
      key = test_key()
      {:ok, sref} = SturdyRef.mint({:symbol, "my-entity"}, key)
      {:ok, attenuated} = SturdyRef.attenuate(sref, Caveat.passthrough(), key)

      resolver = fn {:symbol, "my-entity"} -> {:ok, {:my_actor, :my_entity}} end

      {:ok, ref} = SturdyRef.materialize(attenuated, key, resolver)

      assert ref.attenuation == [Caveat.passthrough()]
    end

    test "rejects invalid SturdyRef" do
      key = test_key()
      {:ok, sref} = SturdyRef.mint({:symbol, "my-entity"}, key)
      tampered = %{sref | sig: :crypto.strong_rand_bytes(16)}

      resolver = fn _ -> {:ok, {:actor, :entity}} end

      assert {:error, :invalid_signature} = SturdyRef.materialize(tampered, key, resolver)
    end

    test "propagates resolver errors" do
      key = test_key()
      {:ok, sref} = SturdyRef.mint({:symbol, "unknown"}, key)

      resolver = fn {:symbol, "unknown"} -> {:error, :not_found} end

      assert {:error, :not_found} = SturdyRef.materialize(sref, key, resolver)
    end
  end

  describe "to_preserves/1 and from_preserves/1" do
    test "roundtrips a SturdyRef without caveats" do
      key = test_key()
      {:ok, sref} = SturdyRef.mint({:symbol, "svc"}, key)

      preserves = SturdyRef.to_preserves(sref)
      {:ok, restored} = SturdyRef.from_preserves(preserves)

      assert restored.oid == sref.oid
      assert restored.sig == sref.sig
      assert restored.caveats == sref.caveats
    end

    test "roundtrips a SturdyRef with passthrough caveat" do
      key = test_key()
      {:ok, sref} = SturdyRef.mint({:symbol, "svc"}, key)
      {:ok, attenuated} = SturdyRef.attenuate(sref, Caveat.passthrough(), key)

      preserves = SturdyRef.to_preserves(attenuated)
      {:ok, restored} = SturdyRef.from_preserves(preserves)

      assert restored.oid == attenuated.oid
      assert restored.sig == attenuated.sig
      assert restored.caveats == attenuated.caveats
    end

    test "roundtrips a SturdyRef with reject caveat" do
      key = test_key()
      {:ok, sref} = SturdyRef.mint({:symbol, "svc"}, key)
      {:ok, attenuated} = SturdyRef.attenuate(sref, Caveat.reject(), key)

      preserves = SturdyRef.to_preserves(attenuated)
      {:ok, restored} = SturdyRef.from_preserves(preserves)

      assert restored.caveats == [:reject]
    end

    test "roundtrips a SturdyRef with rewrite caveat" do
      key = test_key()
      {:ok, sref} = SturdyRef.mint({:symbol, "svc"}, key)

      pattern = {:record, {{:symbol, "Person"}, [{:symbol, "$"}]}}
      caveat = Caveat.rewrite(pattern, {:ref, 0})
      {:ok, attenuated} = SturdyRef.attenuate(sref, caveat, key)

      preserves = SturdyRef.to_preserves(attenuated)
      {:ok, restored} = SturdyRef.from_preserves(preserves)

      assert restored.caveats == attenuated.caveats
    end

    test "roundtrips a SturdyRef with alts caveat" do
      key = test_key()
      {:ok, sref} = SturdyRef.mint({:symbol, "svc"}, key)

      caveat = Caveat.alts([Caveat.passthrough(), Caveat.passthrough()])
      {:ok, attenuated} = SturdyRef.attenuate(sref, caveat, key)

      preserves = SturdyRef.to_preserves(attenuated)
      {:ok, restored} = SturdyRef.from_preserves(preserves)

      assert restored.caveats == attenuated.caveats
    end

    test "roundtrips a SturdyRef with multiple caveats" do
      key = test_key()
      {:ok, sref} = SturdyRef.mint({:symbol, "svc"}, key)
      {:ok, a1} = SturdyRef.attenuate(sref, Caveat.passthrough(), key)
      {:ok, a2} = SturdyRef.attenuate(a1, Caveat.reject(), key)

      preserves = SturdyRef.to_preserves(a2)
      {:ok, restored} = SturdyRef.from_preserves(preserves)

      assert restored.caveats == a2.caveats
      assert SturdyRef.validate(restored, key) == :ok
    end

    test "roundtrips a SturdyRef with complex template" do
      key = test_key()
      {:ok, sref} = SturdyRef.mint({:symbol, "svc"}, key)

      # Complex template: record with literal label and captured field
      caveat =
        Caveat.rewrite(
          {:symbol, "$"},
          {:record, {:lit, {:symbol, "Wrapped"}}, [{:ref, 0}]}
        )

      {:ok, attenuated} = SturdyRef.attenuate(sref, caveat, key)

      preserves = SturdyRef.to_preserves(attenuated)
      {:ok, restored} = SturdyRef.from_preserves(preserves)

      assert restored.caveats == attenuated.caveats
    end

    test "preserves format is correct" do
      key = test_key()
      {:ok, sref} = SturdyRef.mint({:symbol, "svc"}, key)

      preserves = SturdyRef.to_preserves(sref)

      # Should be a record with label "ref"
      assert {:record, {{:symbol, "ref"}, [params]}} = preserves
      assert {:dictionary, dict} = params
      assert Map.has_key?(dict, {:symbol, "oid"})
      assert Map.has_key?(dict, {:symbol, "sig"})
      assert Map.has_key?(dict, {:symbol, "caveats"})
    end

    test "returns error for invalid format" do
      assert {:error, :invalid_sturdy_ref_format} = SturdyRef.from_preserves({:integer, 42})

      assert {:error, :invalid_sturdy_ref_format} =
               SturdyRef.from_preserves({:record, {{:symbol, "wrong"}, []}})
    end

    test "returns error for malformed dict template pairs (does not crash)" do
      # Construct a SturdyRef with a malformed dict template that has invalid pair structure
      malformed_template =
        {:record,
         {{:symbol, "dict"},
          [
            {:sequence,
             [
               # This pair is malformed - should be {:sequence, [k, v]} but has wrong length
               {:sequence, [{:record, {{:symbol, "lit"}, [{:integer, 1}]}}]},
               # This is also malformed - not a sequence at all
               {:integer, 42}
             ]}
          ]}}

      malformed_caveat =
        {:record, {{:symbol, "rewrite"}, [{:symbol, "$"}, malformed_template]}}

      malformed_params =
        {:dictionary,
         %{
           {:symbol, "oid"} => {:symbol, "test"},
           {:symbol, "sig"} => {:binary, :crypto.strong_rand_bytes(16)},
           {:symbol, "caveats"} => {:sequence, [malformed_caveat]}
         }}

      malformed_preserves = {:record, {{:symbol, "ref"}, [malformed_params]}}

      # Should return error, not crash
      assert {:error, :invalid_template_format} = SturdyRef.from_preserves(malformed_preserves)
    end
  end

  describe "sig_to_hex/1" do
    test "converts signature to lowercase hex string" do
      key = test_key()
      {:ok, sref} = SturdyRef.mint({:symbol, "svc"}, key)

      hex = SturdyRef.sig_to_hex(sref)

      # 16 bytes = 32 hex characters
      assert String.length(hex) == 32
      assert hex == String.downcase(hex)
      assert Regex.match?(~r/^[0-9a-f]+$/, hex)
    end
  end

  describe "signature chain integrity" do
    test "macaroon-style chaining produces unique signatures per caveat sequence" do
      key = test_key()
      {:ok, sref} = SturdyRef.mint({:symbol, "svc"}, key)

      # Add caveats in order: passthrough, reject
      {:ok, a1} = SturdyRef.attenuate(sref, Caveat.passthrough(), key)
      {:ok, a2} = SturdyRef.attenuate(a1, Caveat.reject(), key)

      # Add caveats in reverse order: reject, passthrough
      {:ok, b1} = SturdyRef.attenuate(sref, Caveat.reject(), key)
      {:ok, b2} = SturdyRef.attenuate(b1, Caveat.passthrough(), key)

      # Different caveat orders should produce different signatures
      assert a2.sig != b2.sig
    end

    test "cannot forge attenuation by skipping intermediate caveats" do
      key = test_key()
      {:ok, sref} = SturdyRef.mint({:symbol, "svc"}, key)
      {:ok, a1} = SturdyRef.attenuate(sref, Caveat.passthrough(), key)
      {:ok, a2} = SturdyRef.attenuate(a1, Caveat.reject(), key)

      # Try to create a forged ref that skips the passthrough caveat
      forged = %{a2 | caveats: [Caveat.reject()]}

      assert {:error, :invalid_signature} = SturdyRef.validate(forged, key)
    end

    test "signature depends on canonical encoding of caveats" do
      key = test_key()
      {:ok, sref} = SturdyRef.mint({:symbol, "svc"}, key)

      # Two semantically different patterns that should produce different sigs
      pattern1 = {:record, {{:symbol, "A"}, [{:symbol, "$"}]}}
      pattern2 = {:record, {{:symbol, "B"}, [{:symbol, "$"}]}}

      {:ok, a1} = SturdyRef.attenuate(sref, Caveat.rewrite(pattern1, {:ref, 0}), key)
      {:ok, a2} = SturdyRef.attenuate(sref, Caveat.rewrite(pattern2, {:ref, 0}), key)

      assert a1.sig != a2.sig
    end
  end
end
