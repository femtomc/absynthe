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
end
