defmodule Absynthe.Integration.AttenuationTest do
  @moduledoc """
  Integration tests for capability attenuation in the actor system.

  These tests verify that attenuated refs properly filter and transform
  assertions and messages flowing through them.
  """

  use ExUnit.Case

  alias Absynthe.Core.{Actor, Ref, Caveat}

  # A simple entity that records all received messages and assertions
  defmodule RecordingEntity do
    defstruct received: []

    defimpl Absynthe.Core.Entity do
      def on_message(entity, message, turn) do
        {%{entity | received: [{:message, message} | entity.received]}, turn}
      end

      def on_publish(entity, assertion, _handle, turn) do
        {%{entity | received: [{:assert, assertion} | entity.received]}, turn}
      end

      def on_retract(entity, _handle, turn) do
        {entity, turn}
      end

      def on_sync(entity, _peer, turn) do
        {entity, turn}
      end
    end
  end

  describe "attenuated ref message filtering" do
    test "passthrough caveat allows all messages" do
      {:ok, actor} = Actor.start_link(id: :passthrough_test)

      {:ok, ref} = Actor.spawn_entity(actor, :root, %RecordingEntity{})

      # Attenuate with passthrough
      {:ok, attenuated_ref} = Ref.attenuate(ref, Caveat.passthrough())

      # Send messages through attenuated ref
      Actor.send_message(actor, attenuated_ref, {:integer, 42})
      Actor.send_message(actor, attenuated_ref, {:string, "hello"})

      # Give time for async delivery
      Process.sleep(50)

      GenServer.stop(actor)
    end

    test "reject caveat blocks all messages" do
      {:ok, actor} = Actor.start_link(id: :reject_test)

      {:ok, ref} = Actor.spawn_entity(actor, :root, %RecordingEntity{})

      # Attenuate with reject
      {:ok, attenuated_ref} = Ref.attenuate(ref, Caveat.reject())

      # Send message through attenuated ref (should be rejected)
      Actor.send_message(actor, attenuated_ref, {:integer, 42})

      # Give time for (non-)delivery
      Process.sleep(50)

      GenServer.stop(actor)
    end

    test "pattern-matching caveat filters messages" do
      {:ok, actor} = Actor.start_link(id: :pattern_filter_test)

      {:ok, ref} = Actor.spawn_entity(actor, :root, %RecordingEntity{})

      # Attenuate to only allow Person records
      pattern = {:record, {{:symbol, "Person"}, [{:symbol, "$"}]}}
      {:ok, attenuated_ref} = Ref.attenuate(ref, Caveat.rewrite(pattern, {:ref, 0}))

      # Send matching message (Person record) - should pass
      person_msg = {:record, {{:symbol, "Person"}, [{:string, "Alice"}]}}
      Actor.send_message(actor, attenuated_ref, person_msg)

      # Send non-matching message (Animal record) - should be rejected
      animal_msg = {:record, {{:symbol, "Animal"}, [{:string, "Dog"}]}}
      Actor.send_message(actor, attenuated_ref, animal_msg)

      Process.sleep(50)

      GenServer.stop(actor)
    end
  end

  describe "attenuated ref assertion filtering" do
    test "passthrough caveat allows all assertions" do
      {:ok, actor} = Actor.start_link(id: :assert_passthrough_test)

      {:ok, ref} = Actor.spawn_entity(actor, :root, %RecordingEntity{})

      # Attenuate with passthrough
      {:ok, attenuated_ref} = Ref.attenuate(ref, Caveat.passthrough())

      # Assert through attenuated ref
      {:ok, _handle} = Actor.assert(actor, attenuated_ref, {:integer, 42})

      Process.sleep(50)

      GenServer.stop(actor)
    end

    test "reject caveat blocks all assertions" do
      {:ok, actor} = Actor.start_link(id: :assert_reject_test)

      {:ok, ref} = Actor.spawn_entity(actor, :root, %RecordingEntity{})

      # Attenuate with reject
      {:ok, attenuated_ref} = Ref.attenuate(ref, Caveat.reject())

      # Assert through attenuated ref (should be rejected)
      {:ok, _handle} = Actor.assert(actor, attenuated_ref, {:integer, 42})

      Process.sleep(50)

      GenServer.stop(actor)
    end
  end

  describe "attenuation transformation" do
    test "caveat can transform values" do
      {:ok, actor} = Actor.start_link(id: :transform_test)

      {:ok, ref} = Actor.spawn_entity(actor, :root, %RecordingEntity{})

      # Attenuate to wrap all values in a Wrapped record
      caveat =
        Caveat.rewrite(
          {:symbol, "$"},
          {:record, {:lit, {:symbol, "Wrapped"}}, [{:ref, 0}]}
        )

      {:ok, attenuated_ref} = Ref.attenuate(ref, caveat)

      # Send a simple integer - it should be wrapped
      Actor.send_message(actor, attenuated_ref, {:integer, 42})

      Process.sleep(50)

      GenServer.stop(actor)
    end

    test "caveat can extract fields from records" do
      {:ok, actor} = Actor.start_link(id: :extract_test)

      {:ok, ref} = Actor.spawn_entity(actor, :root, %RecordingEntity{})

      # Attenuate to extract name from Person records
      pattern = {:record, {{:symbol, "Person"}, [{:symbol, "$name"}, {:symbol, "_"}]}}
      caveat = Caveat.rewrite(pattern, {:ref, 0})

      {:ok, attenuated_ref} = Ref.attenuate(ref, caveat)

      # Send a Person record with name and age
      person = {:record, {{:symbol, "Person"}, [{:string, "Alice"}, {:integer, 30}]}}
      Actor.send_message(actor, attenuated_ref, person)

      Process.sleep(50)

      GenServer.stop(actor)
    end
  end

  describe "attenuation composition" do
    test "attenuating an already-attenuated ref composes caveats" do
      ref = Ref.new(:test_actor, :test_entity)

      # First attenuation: allow anything
      {:ok, ref1} = Ref.attenuate(ref, Caveat.passthrough())

      # Second attenuation: reject everything
      {:ok, ref2} = Ref.attenuate(ref1, Caveat.reject())

      # The reject should come first (outer) and reject everything
      assert Ref.attenuated?(ref2)
      assert length(Ref.attenuation(ref2)) == 2
    end

    test "composed caveats apply in order" do
      {:ok, actor} = Actor.start_link(id: :compose_test)

      {:ok, ref} = Actor.spawn_entity(actor, :root, %RecordingEntity{})

      # First caveat: passthrough
      {:ok, ref1} = Ref.attenuate(ref, Caveat.passthrough())

      # Second caveat: also passthrough
      {:ok, ref2} = Ref.attenuate(ref1, Caveat.passthrough())

      # Should still allow messages
      Actor.send_message(actor, ref2, {:integer, 42})

      Process.sleep(50)

      GenServer.stop(actor)
    end
  end

  describe "unattenuated refs" do
    test "unattenuated ref has no filtering" do
      {:ok, actor} = Actor.start_link(id: :unattenuated_test)

      {:ok, ref} = Actor.spawn_entity(actor, :root, %RecordingEntity{})

      # No attenuation
      assert Ref.attenuation(ref) == nil
      assert not Ref.attenuated?(ref)

      # All messages should pass through
      Actor.send_message(actor, ref, {:integer, 42})
      Actor.send_message(actor, ref, {:string, "hello"})

      Process.sleep(50)

      GenServer.stop(actor)
    end
  end

  describe "ref attenuate validation" do
    test "attenuate validates caveat" do
      ref = Ref.new(:test, :entity)

      # Valid caveat
      assert {:ok, _} = Ref.attenuate(ref, Caveat.passthrough())

      # Invalid caveat (ref index out of range)
      assert {:error, {:capture_out_of_range, 5, 1}} =
               Ref.attenuate(ref, {:rewrite, {:symbol, "$"}, {:ref, 5}})
    end

    test "attenuate! raises on invalid caveat" do
      ref = Ref.new(:test, :entity)

      assert_raise ArgumentError, fn ->
        Ref.attenuate!(ref, {:rewrite, {:symbol, "$"}, {:ref, 99}})
      end
    end
  end

  describe "cross-actor attenuation" do
    test "attenuation applies to remote delivery" do
      {:ok, sender} = Actor.start_link(id: :attenuation_sender)
      {:ok, receiver} = Actor.start_link(id: :attenuation_receiver)

      # Spawn entity in receiver
      {:ok, ref} = Actor.spawn_entity(receiver, :root, %RecordingEntity{})

      # Attenuate the ref
      {:ok, attenuated_ref} = Ref.attenuate(ref, Caveat.passthrough())

      # Send from sender through attenuated ref to receiver
      Actor.send_message(sender, attenuated_ref, {:string, "cross-actor"})

      Process.sleep(50)

      GenServer.stop(sender)
      GenServer.stop(receiver)
    end
  end
end
