defmodule Absynthe.Relay.MembraneTest do
  use ExUnit.Case, async: true

  alias Absynthe.Relay.Membrane
  alias Absynthe.Core.Ref

  describe "new/1" do
    test "creates an empty membrane" do
      membrane = Membrane.new()

      assert membrane.next_oid == 0
      assert Membrane.size(membrane) == 0
    end

    test "accepts initial_oid option" do
      membrane = Membrane.new(initial_oid: 100)

      assert membrane.next_oid == 100
    end
  end

  describe "export/2" do
    test "assigns unique OIDs to different refs" do
      membrane = Membrane.new()
      ref1 = Ref.new(:actor, 1)
      ref2 = Ref.new(:actor, 2)

      {oid1, membrane} = Membrane.export(membrane, ref1)
      {oid2, membrane} = Membrane.export(membrane, ref2)

      assert oid1 == 0
      assert oid2 == 1
      assert Membrane.size(membrane) == 2
    end

    test "returns same OID for same ref" do
      membrane = Membrane.new()
      ref = Ref.new(:actor, 1)

      {oid1, membrane} = Membrane.export(membrane, ref)
      {oid2, _membrane} = Membrane.export(membrane, ref)

      assert oid1 == oid2
    end

    test "strips attenuation for key lookup" do
      membrane = Membrane.new()
      ref = Ref.new(:actor, 1)
      attenuated_ref = Ref.with_attenuation(ref, [:read_only])

      {oid1, membrane} = Membrane.export(membrane, ref)
      {oid2, _membrane} = Membrane.export(membrane, attenuated_ref)

      assert oid1 == oid2
    end
  end

  describe "import/4" do
    test "creates proxy via factory for new OID" do
      membrane = Membrane.new()

      factory = fn oid -> Ref.new(:proxy, oid) end
      {ref, membrane} = Membrane.import(membrane, 42, [], factory)

      assert ref.entity_id == 42
      assert Membrane.size(membrane) == 1
    end

    test "returns existing ref for known OID" do
      membrane = Membrane.new()

      factory = fn oid -> Ref.new(:proxy, oid) end
      {ref1, membrane} = Membrane.import(membrane, 42, [], factory)
      {ref2, _membrane} = Membrane.import(membrane, 42, [], factory)

      assert ref1 == ref2
    end

    test "applies attenuation to imported ref" do
      membrane = Membrane.new()
      factory = fn oid -> Ref.new(:proxy, oid) end

      {ref, _membrane} = Membrane.import(membrane, 42, [:read_only], factory)

      assert Ref.attenuated?(ref)
      assert Ref.attenuation(ref) == [:read_only]
    end
  end

  describe "lookup_by_oid/2" do
    test "returns ref for known OID" do
      membrane = Membrane.new()
      ref = Ref.new(:actor, 1)

      {oid, membrane} = Membrane.export(membrane, ref)

      assert {:ok, ^ref} = Membrane.lookup_by_oid(membrane, oid)
    end

    test "returns error for unknown OID" do
      membrane = Membrane.new()

      assert :error = Membrane.lookup_by_oid(membrane, 999)
    end
  end

  describe "lookup_by_ref/2" do
    test "returns OID for known ref" do
      membrane = Membrane.new()
      ref = Ref.new(:actor, 1)

      {oid, membrane} = Membrane.export(membrane, ref)

      assert {:ok, ^oid} = Membrane.lookup_by_ref(membrane, ref)
    end

    test "returns error for unknown ref" do
      membrane = Membrane.new()
      ref = Ref.new(:actor, 1)

      assert :error = Membrane.lookup_by_ref(membrane, ref)
    end
  end

  describe "inc_ref/2 and dec_ref/2" do
    test "tracks refcount correctly" do
      membrane = Membrane.new()
      ref = Ref.new(:actor, 1)

      {oid, membrane} = Membrane.export(membrane, ref)
      assert Membrane.refcount(membrane, oid) == 0

      membrane = Membrane.inc_ref(membrane, oid)
      assert Membrane.refcount(membrane, oid) == 1

      membrane = Membrane.inc_ref(membrane, oid)
      assert Membrane.refcount(membrane, oid) == 2

      {membrane, status} = Membrane.dec_ref(membrane, oid)
      assert status == :alive
      assert Membrane.refcount(membrane, oid) == 1

      {membrane, status} = Membrane.dec_ref(membrane, oid)
      assert status == :removed
      assert Membrane.size(membrane) == 0
    end

    test "dec_ref returns not_found for unknown OID" do
      membrane = Membrane.new()

      {_membrane, status} = Membrane.dec_ref(membrane, 999)
      assert status == :not_found
    end

    test "garbage collects when refcount hits zero" do
      membrane = Membrane.new()
      ref = Ref.new(:actor, 1)

      {oid, membrane} = Membrane.export(membrane, ref)
      membrane = Membrane.inc_ref(membrane, oid)
      {membrane, :removed} = Membrane.dec_ref(membrane, oid)

      assert :error = Membrane.lookup_by_oid(membrane, oid)
      assert :error = Membrane.lookup_by_ref(membrane, ref)
    end
  end

  describe "oids/1" do
    test "returns all OIDs in membrane" do
      membrane = Membrane.new()
      ref1 = Ref.new(:actor, 1)
      ref2 = Ref.new(:actor, 2)

      {_oid, membrane} = Membrane.export(membrane, ref1)
      {_oid, membrane} = Membrane.export(membrane, ref2)

      oids = Membrane.oids(membrane)
      assert length(oids) == 2
      assert Enum.sort(oids) == [0, 1]
    end
  end

  describe "refcount lifecycle" do
    test "multiple assertions referencing the same OID share refcount" do
      membrane = Membrane.new()
      ref = Ref.new(:actor, 1)

      # Export ref (refcount starts at 0)
      {oid, membrane} = Membrane.export(membrane, ref)
      assert Membrane.refcount(membrane, oid) == 0
      assert Membrane.size(membrane) == 1

      # First assertion references this OID
      membrane = Membrane.inc_ref(membrane, oid)
      assert Membrane.refcount(membrane, oid) == 1

      # Second assertion references the same OID
      membrane = Membrane.inc_ref(membrane, oid)
      assert Membrane.refcount(membrane, oid) == 2

      # Third assertion references the same OID
      membrane = Membrane.inc_ref(membrane, oid)
      assert Membrane.refcount(membrane, oid) == 3

      # Retract first assertion
      {membrane, status} = Membrane.dec_ref(membrane, oid)
      assert status == :alive
      assert Membrane.refcount(membrane, oid) == 2
      assert Membrane.size(membrane) == 1

      # Retract second assertion
      {membrane, status} = Membrane.dec_ref(membrane, oid)
      assert status == :alive
      assert Membrane.refcount(membrane, oid) == 1
      assert Membrane.size(membrane) == 1

      # Retract third assertion - should GC
      {membrane, status} = Membrane.dec_ref(membrane, oid)
      assert status == :removed
      assert Membrane.size(membrane) == 0

      # OID should no longer be lookupable
      assert :error = Membrane.lookup_by_oid(membrane, oid)
      assert :error = Membrane.lookup_by_ref(membrane, ref)
    end

    test "independent OIDs have independent refcounts" do
      membrane = Membrane.new()
      ref1 = Ref.new(:actor, 1)
      ref2 = Ref.new(:actor, 2)

      # Export both refs
      {oid1, membrane} = Membrane.export(membrane, ref1)
      {oid2, membrane} = Membrane.export(membrane, ref2)
      assert Membrane.size(membrane) == 2

      # Assertion 1 references oid1
      membrane = Membrane.inc_ref(membrane, oid1)

      # Assertion 2 references oid2
      membrane = Membrane.inc_ref(membrane, oid2)

      # Assertion 3 references both oid1 and oid2
      membrane = Membrane.inc_ref(membrane, oid1)
      membrane = Membrane.inc_ref(membrane, oid2)

      assert Membrane.refcount(membrane, oid1) == 2
      assert Membrane.refcount(membrane, oid2) == 2

      # Retract assertion 1 (only oid1)
      {membrane, _} = Membrane.dec_ref(membrane, oid1)
      assert Membrane.refcount(membrane, oid1) == 1
      assert Membrane.refcount(membrane, oid2) == 2
      assert Membrane.size(membrane) == 2

      # Retract assertion 2 (only oid2)
      {membrane, _} = Membrane.dec_ref(membrane, oid2)
      assert Membrane.refcount(membrane, oid1) == 1
      assert Membrane.refcount(membrane, oid2) == 1
      assert Membrane.size(membrane) == 2

      # Retract assertion 3 (both oid1 and oid2)
      {membrane, :removed} = Membrane.dec_ref(membrane, oid1)
      {membrane, :removed} = Membrane.dec_ref(membrane, oid2)
      assert Membrane.size(membrane) == 0
    end

    test "imported OIDs follow the same lifecycle" do
      membrane = Membrane.new()
      factory = fn oid -> Ref.new(:proxy, oid) end

      # Import OID 42
      {ref, membrane} = Membrane.import(membrane, 42, [], factory)
      assert ref.entity_id == 42
      assert Membrane.refcount(membrane, 42) == 0
      assert Membrane.size(membrane) == 1

      # First assertion uses this imported ref
      membrane = Membrane.inc_ref(membrane, 42)
      assert Membrane.refcount(membrane, 42) == 1

      # Second assertion also uses it
      membrane = Membrane.inc_ref(membrane, 42)
      assert Membrane.refcount(membrane, 42) == 2

      # Retract both assertions
      {membrane, :alive} = Membrane.dec_ref(membrane, 42)
      {membrane, :removed} = Membrane.dec_ref(membrane, 42)

      # OID should be GC'd
      assert Membrane.size(membrane) == 0
      assert :error = Membrane.lookup_by_oid(membrane, 42)
    end
  end
end
