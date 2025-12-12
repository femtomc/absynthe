defmodule Absynthe.Integration.AttenuationTest do
  @moduledoc """
  Integration tests for capability attenuation in the actor system.

  These tests verify that attenuated refs properly filter and transform
  assertions and messages flowing through them.
  """

  use ExUnit.Case

  alias Absynthe.Core.{Actor, Ref, Caveat}

  # A simple entity that reports received messages to a test process
  defmodule RecordingEntity do
    defstruct test_pid: nil

    defimpl Absynthe.Core.Entity do
      def on_message(entity, message, turn) do
        send(entity.test_pid, {:received_message, message})
        {entity, turn}
      end

      def on_publish(entity, assertion, _handle, turn) do
        send(entity.test_pid, {:received_assert, assertion})
        {entity, turn}
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

      {:ok, ref} = Actor.spawn_entity(actor, :root, %RecordingEntity{test_pid: self()})

      # Attenuate with passthrough
      {:ok, attenuated_ref} = Ref.attenuate(ref, Caveat.passthrough())

      # Send messages through attenuated ref
      Actor.send_message(actor, attenuated_ref, {:integer, 42})
      Actor.send_message(actor, attenuated_ref, {:string, "hello"})

      # Verify both messages were received
      assert_receive {:received_message, {:integer, 42}}, 100
      assert_receive {:received_message, {:string, "hello"}}, 100

      GenServer.stop(actor)
    end

    test "reject caveat blocks all messages" do
      {:ok, actor} = Actor.start_link(id: :reject_test)

      {:ok, ref} = Actor.spawn_entity(actor, :root, %RecordingEntity{test_pid: self()})

      # Attenuate with reject
      {:ok, attenuated_ref} = Ref.attenuate(ref, Caveat.reject())

      # Send message through attenuated ref (should be rejected)
      Actor.send_message(actor, attenuated_ref, {:integer, 42})

      # Verify message was NOT received
      refute_receive {:received_message, _}, 100

      GenServer.stop(actor)
    end

    test "pattern-matching caveat filters messages" do
      {:ok, actor} = Actor.start_link(id: :pattern_filter_test)

      {:ok, ref} = Actor.spawn_entity(actor, :root, %RecordingEntity{test_pid: self()})

      # Attenuate to only allow Person records, extract the name
      pattern = {:record, {{:symbol, "Person"}, [{:symbol, "$"}]}}
      {:ok, attenuated_ref} = Ref.attenuate(ref, Caveat.rewrite(pattern, {:ref, 0}))

      # Send matching message (Person record) - should pass, delivers the name
      person_msg = {:record, {{:symbol, "Person"}, [{:string, "Alice"}]}}
      Actor.send_message(actor, attenuated_ref, person_msg)

      # Send non-matching message (Animal record) - should be rejected
      animal_msg = {:record, {{:symbol, "Animal"}, [{:string, "Dog"}]}}
      Actor.send_message(actor, attenuated_ref, animal_msg)

      # Verify only the Person name was received (extracted by template)
      assert_receive {:received_message, {:string, "Alice"}}, 100
      refute_receive {:received_message, _}, 100

      GenServer.stop(actor)
    end
  end

  describe "attenuated ref assertion filtering" do
    test "passthrough caveat allows all assertions" do
      {:ok, actor} = Actor.start_link(id: :assert_passthrough_test)

      {:ok, ref} = Actor.spawn_entity(actor, :root, %RecordingEntity{test_pid: self()})

      # Attenuate with passthrough
      {:ok, attenuated_ref} = Ref.attenuate(ref, Caveat.passthrough())

      # Assert through attenuated ref
      {:ok, _handle} = Actor.assert(actor, attenuated_ref, {:integer, 42})

      # Verify assertion was received
      assert_receive {:received_assert, {:integer, 42}}, 100

      GenServer.stop(actor)
    end

    test "reject caveat blocks all assertions and returns error" do
      {:ok, actor} = Actor.start_link(id: :assert_reject_test)

      {:ok, ref} = Actor.spawn_entity(actor, :root, %RecordingEntity{test_pid: self()})

      # Attenuate with reject
      {:ok, attenuated_ref} = Ref.attenuate(ref, Caveat.reject())

      # Assert through attenuated ref (should be rejected with error)
      assert {:error, :attenuation_rejected} =
               Actor.assert(actor, attenuated_ref, {:integer, 42})

      # Verify assertion was NOT received
      refute_receive {:received_assert, _}, 100

      GenServer.stop(actor)
    end

    test "pattern caveat blocks non-matching assertions and returns error" do
      {:ok, actor} = Actor.start_link(id: :assert_pattern_test)

      {:ok, ref} = Actor.spawn_entity(actor, :root, %RecordingEntity{test_pid: self()})

      # Attenuate to only allow Person records
      pattern = {:record, {{:symbol, "Person"}, [{:symbol, "$"}]}}
      {:ok, attenuated_ref} = Ref.attenuate(ref, Caveat.rewrite(pattern, {:ref, 0}))

      # Assert matching value - should succeed
      person = {:record, {{:symbol, "Person"}, [{:string, "Bob"}]}}
      {:ok, _handle} = Actor.assert(actor, attenuated_ref, person)

      # Assert non-matching value - should fail
      animal = {:record, {{:symbol, "Animal"}, [{:string, "Cat"}]}}
      assert {:error, :attenuation_rejected} = Actor.assert(actor, attenuated_ref, animal)

      # Verify only the Person name was received (extracted by template)
      assert_receive {:received_assert, {:string, "Bob"}}, 100
      refute_receive {:received_assert, _}, 100

      GenServer.stop(actor)
    end
  end

  describe "attenuation transformation" do
    test "caveat can transform values" do
      {:ok, actor} = Actor.start_link(id: :transform_test)

      {:ok, ref} = Actor.spawn_entity(actor, :root, %RecordingEntity{test_pid: self()})

      # Attenuate to wrap all values in a Wrapped record
      caveat =
        Caveat.rewrite(
          {:symbol, "$"},
          {:record, {:lit, {:symbol, "Wrapped"}}, [{:ref, 0}]}
        )

      {:ok, attenuated_ref} = Ref.attenuate(ref, caveat)

      # Send a simple integer - it should be wrapped
      Actor.send_message(actor, attenuated_ref, {:integer, 42})

      # Verify the wrapped value was received
      expected = {:record, {{:symbol, "Wrapped"}, [{:integer, 42}]}}
      assert_receive {:received_message, ^expected}, 100

      GenServer.stop(actor)
    end

    test "caveat can extract fields from records" do
      {:ok, actor} = Actor.start_link(id: :extract_test)

      {:ok, ref} = Actor.spawn_entity(actor, :root, %RecordingEntity{test_pid: self()})

      # Attenuate to extract name from Person records
      pattern = {:record, {{:symbol, "Person"}, [{:symbol, "$name"}, {:symbol, "_"}]}}
      caveat = Caveat.rewrite(pattern, {:ref, 0})

      {:ok, attenuated_ref} = Ref.attenuate(ref, caveat)

      # Send a Person record with name and age
      person = {:record, {{:symbol, "Person"}, [{:string, "Alice"}, {:integer, 30}]}}
      Actor.send_message(actor, attenuated_ref, person)

      # Verify only the name was received (extracted by template)
      assert_receive {:received_message, {:string, "Alice"}}, 100

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

      {:ok, ref} = Actor.spawn_entity(actor, :root, %RecordingEntity{test_pid: self()})

      # First caveat: passthrough
      {:ok, ref1} = Ref.attenuate(ref, Caveat.passthrough())

      # Second caveat: also passthrough
      {:ok, ref2} = Ref.attenuate(ref1, Caveat.passthrough())

      # Should still allow messages
      Actor.send_message(actor, ref2, {:integer, 42})

      assert_receive {:received_message, {:integer, 42}}, 100

      GenServer.stop(actor)
    end

    test "composed reject blocks even with passthrough inner" do
      {:ok, actor} = Actor.start_link(id: :compose_reject_test)

      {:ok, ref} = Actor.spawn_entity(actor, :root, %RecordingEntity{test_pid: self()})

      # First caveat: passthrough
      {:ok, ref1} = Ref.attenuate(ref, Caveat.passthrough())

      # Second caveat (outer): reject
      {:ok, ref2} = Ref.attenuate(ref1, Caveat.reject())

      # Should block messages due to outer reject
      Actor.send_message(actor, ref2, {:integer, 42})

      refute_receive {:received_message, _}, 100

      GenServer.stop(actor)
    end
  end

  describe "unattenuated refs" do
    test "unattenuated ref has no filtering" do
      {:ok, actor} = Actor.start_link(id: :unattenuated_test)

      {:ok, ref} = Actor.spawn_entity(actor, :root, %RecordingEntity{test_pid: self()})

      # No attenuation
      assert Ref.attenuation(ref) == nil
      assert not Ref.attenuated?(ref)

      # All messages should pass through
      Actor.send_message(actor, ref, {:integer, 42})
      Actor.send_message(actor, ref, {:string, "hello"})

      assert_receive {:received_message, {:integer, 42}}, 100
      assert_receive {:received_message, {:string, "hello"}}, 100

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
      # Must register names for cross-actor routing to work
      {:ok, sender} = Actor.start_link(id: :attenuation_sender, name: :attenuation_sender)
      {:ok, receiver} = Actor.start_link(id: :attenuation_receiver, name: :attenuation_receiver)

      # Spawn entity in receiver
      {:ok, ref} = Actor.spawn_entity(receiver, :root, %RecordingEntity{test_pid: self()})

      # Attenuate the ref
      {:ok, attenuated_ref} = Ref.attenuate(ref, Caveat.passthrough())

      # Send from sender through attenuated ref to receiver
      Actor.send_message(sender, attenuated_ref, {:string, "cross-actor"})

      assert_receive {:received_message, {:string, "cross-actor"}}, 100

      GenServer.stop(sender)
      GenServer.stop(receiver)
    end

    test "cross-actor reject caveat blocks messages" do
      # Must register names for cross-actor routing to work
      {:ok, sender} = Actor.start_link(id: :cross_reject_sender, name: :cross_reject_sender)
      {:ok, receiver} = Actor.start_link(id: :cross_reject_receiver, name: :cross_reject_receiver)

      # Spawn entity in receiver
      {:ok, ref} = Actor.spawn_entity(receiver, :root, %RecordingEntity{test_pid: self()})

      # Attenuate with reject
      {:ok, attenuated_ref} = Ref.attenuate(ref, Caveat.reject())

      # Send from sender - should be rejected
      Actor.send_message(sender, attenuated_ref, {:string, "should-not-arrive"})

      refute_receive {:received_message, _}, 100

      GenServer.stop(sender)
      GenServer.stop(receiver)
    end
  end
end
