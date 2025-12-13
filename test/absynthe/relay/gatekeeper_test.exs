# Test observer entity that forwards messages to the test process
# Must be defined before the test module
defmodule Absynthe.Relay.GatekeeperTest.TestObserver do
  @moduledoc false
  defstruct [:test_pid]
end

defimpl Absynthe.Core.Entity, for: Absynthe.Relay.GatekeeperTest.TestObserver do
  def on_publish(entity, _assertion, _handle, turn), do: {entity, turn}
  def on_retract(entity, _handle, turn), do: {entity, turn}

  def on_message(entity, message, turn) do
    send(entity.test_pid, {:observer_got_message, message})
    {entity, turn}
  end

  def on_sync(entity, _peer, turn), do: {entity, turn}
end

defmodule Absynthe.Relay.GatekeeperTest do
  use ExUnit.Case, async: true

  alias Absynthe.Core.{Actor, Caveat, Entity, Ref, SturdyRef}
  alias Absynthe.Relay.Gatekeeper
  alias Absynthe.Assertions.Handle
  alias __MODULE__.TestObserver

  # Generate a fresh key for each test
  defp test_key, do: :crypto.strong_rand_bytes(32)

  # Helper to create a test resolver
  defp test_resolver do
    fn
      {:symbol, "my-service"} -> {:ok, {:test_actor, :my_entity}}
      {:symbol, "attenuated-service"} -> {:ok, {:test_actor, :attenuated_entity}}
      _ -> {:error, :not_found}
    end
  end

  describe "new/2" do
    test "creates a Gatekeeper with key and resolver" do
      key = test_key()
      resolver = fn _ -> {:error, :not_implemented} end

      gatekeeper = Gatekeeper.new(key, resolver)

      assert gatekeeper.key == key
      assert is_function(gatekeeper.resolver, 1)
    end
  end

  describe "with_registry/2" do
    test "creates a Gatekeeper with a registry-based resolver" do
      key = test_key()

      registry = %{
        {:symbol, "service1"} => {:actor1, :entity1},
        {:symbol, "service2"} => {:actor2, :entity2}
      }

      gatekeeper = Gatekeeper.with_registry(key, registry)

      # Test that the resolver works
      assert {:ok, {:actor1, :entity1}} = gatekeeper.resolver.({:symbol, "service1"})
      assert {:ok, {:actor2, :entity2}} = gatekeeper.resolver.({:symbol, "service2"})

      assert {:error, {:unknown_oid, {:symbol, "unknown"}}} =
               gatekeeper.resolver.({:symbol, "unknown"})
    end
  end

  describe "mint/2 and mint!/2" do
    test "mints a sturdy ref using the gatekeeper's key" do
      key = test_key()
      gatekeeper = Gatekeeper.new(key, fn _ -> {:error, :not_found} end)

      {:ok, sref} = Gatekeeper.mint(gatekeeper, {:symbol, "my-service"})

      assert sref.oid == {:symbol, "my-service"}
      assert byte_size(sref.sig) == 16
      assert SturdyRef.validate(sref, key) == :ok
    end

    test "mint! returns the sturdy ref directly" do
      key = test_key()
      gatekeeper = Gatekeeper.new(key, fn _ -> {:error, :not_found} end)

      sref = Gatekeeper.mint!(gatekeeper, {:symbol, "test"})

      assert sref.oid == {:symbol, "test"}
    end
  end

  describe "attenuate/3" do
    test "attenuates a sturdy ref with a caveat" do
      key = test_key()
      gatekeeper = Gatekeeper.new(key, fn _ -> {:error, :not_found} end)

      {:ok, sref} = Gatekeeper.mint(gatekeeper, {:symbol, "my-service"})
      {:ok, attenuated} = Gatekeeper.attenuate(gatekeeper, sref, Caveat.passthrough())

      assert length(attenuated.caveats) == 1
      assert SturdyRef.validate(attenuated, key) == :ok
    end
  end

  describe "Entity protocol - on_publish/4" do
    setup do
      # Start a test actor with a registered name so it can be routed to
      {:ok, actor_pid} =
        Actor.start_link(id: :test_gatekeeper_actor, name: :test_gatekeeper_actor)

      # Create an observer entity that forwards messages to the test process
      test_pid = self()
      observer_entity = %TestObserver{test_pid: test_pid}
      {:ok, observer_ref} = Actor.spawn_entity(actor_pid, :root, observer_entity)

      on_exit(fn ->
        if Process.alive?(actor_pid), do: GenServer.stop(actor_pid)
      end)

      %{actor_pid: actor_pid, observer_ref: observer_ref}
    end

    test "resolves valid sturdy ref and sends accepted response", %{
      actor_pid: _actor_pid,
      observer_ref: observer_ref
    } do
      key = test_key()
      resolver = test_resolver()
      gatekeeper = Gatekeeper.new(key, resolver)

      # Mint a valid sturdy ref
      {:ok, sref} = SturdyRef.mint({:symbol, "my-service"}, key)
      sref_preserves = SturdyRef.to_preserves(sref)

      # Create the resolve assertion
      resolve_assertion =
        {:record,
         {{:symbol, "resolve"},
          [
            {:dictionary,
             %{
               {:symbol, "step"} => sref_preserves,
               {:symbol, "observer"} => {:embedded, observer_ref}
             }}
          ]}}

      # Process the assertion
      handle = Handle.new(:test, 1)
      turn = nil

      {_gatekeeper, _turn} = Entity.on_publish(gatekeeper, resolve_assertion, handle, turn)

      # Give time for the message to be delivered
      Process.sleep(50)

      # Check that the observer received an accepted response
      assert_receive {:observer_got_message, response}, 1000

      assert {:record, {{:symbol, "accepted"}, [params]}} = response
      assert {:dictionary, dict} = params
      assert {:embedded, resolved_ref} = Map.get(dict, {:symbol, "responderSession"})
      assert resolved_ref.actor_id == :test_actor
      assert resolved_ref.entity_id == :my_entity
    end

    test "rejects invalid sturdy ref signature", %{
      actor_pid: _actor_pid,
      observer_ref: observer_ref
    } do
      key = test_key()
      wrong_key = test_key()
      resolver = test_resolver()
      gatekeeper = Gatekeeper.new(key, resolver)

      # Mint with wrong key
      {:ok, sref} = SturdyRef.mint({:symbol, "my-service"}, wrong_key)
      sref_preserves = SturdyRef.to_preserves(sref)

      # Create the resolve assertion
      resolve_assertion =
        {:record,
         {{:symbol, "resolve"},
          [
            {:dictionary,
             %{
               {:symbol, "step"} => sref_preserves,
               {:symbol, "observer"} => {:embedded, observer_ref}
             }}
          ]}}

      handle = Handle.new(:test, 1)
      {_gatekeeper, _turn} = Entity.on_publish(gatekeeper, resolve_assertion, handle, nil)

      Process.sleep(50)

      # Check that the observer received a rejected response
      assert_receive {:observer_got_message, response}, 1000

      assert {:record, {{:symbol, "rejected"}, [params]}} = response
      assert {:dictionary, dict} = params
      assert {:symbol, "invalid_signature"} = Map.get(dict, {:symbol, "detail"})
    end

    test "rejects unknown oid", %{actor_pid: _actor_pid, observer_ref: observer_ref} do
      key = test_key()
      resolver = test_resolver()
      gatekeeper = Gatekeeper.new(key, resolver)

      # Mint with valid key but unknown oid
      {:ok, sref} = SturdyRef.mint({:symbol, "unknown-service"}, key)
      sref_preserves = SturdyRef.to_preserves(sref)

      # Create the resolve assertion
      resolve_assertion =
        {:record,
         {{:symbol, "resolve"},
          [
            {:dictionary,
             %{
               {:symbol, "step"} => sref_preserves,
               {:symbol, "observer"} => {:embedded, observer_ref}
             }}
          ]}}

      handle = Handle.new(:test, 1)
      {_gatekeeper, _turn} = Entity.on_publish(gatekeeper, resolve_assertion, handle, nil)

      Process.sleep(50)

      # Check that the observer received a rejected response
      assert_receive {:observer_got_message, response}, 1000

      assert {:record, {{:symbol, "rejected"}, [params]}} = response
      assert {:dictionary, dict} = params
      # The resolver returns :not_found, which is formatted as a string
      assert {:string, _} = Map.get(dict, {:symbol, "detail"})
    end

    test "resolves attenuated sturdy ref and includes attenuation in result", %{
      actor_pid: _actor_pid,
      observer_ref: observer_ref
    } do
      key = test_key()
      resolver = test_resolver()
      gatekeeper = Gatekeeper.new(key, resolver)

      # Mint and attenuate
      {:ok, sref} = SturdyRef.mint({:symbol, "my-service"}, key)
      {:ok, attenuated} = SturdyRef.attenuate(sref, Caveat.passthrough(), key)
      sref_preserves = SturdyRef.to_preserves(attenuated)

      # Create the resolve assertion
      resolve_assertion =
        {:record,
         {{:symbol, "resolve"},
          [
            {:dictionary,
             %{
               {:symbol, "step"} => sref_preserves,
               {:symbol, "observer"} => {:embedded, observer_ref}
             }}
          ]}}

      handle = Handle.new(:test, 1)
      {_gatekeeper, _turn} = Entity.on_publish(gatekeeper, resolve_assertion, handle, nil)

      Process.sleep(50)

      # Check that the observer received an accepted response with attenuation
      assert_receive {:observer_got_message, response}, 1000

      assert {:record, {{:symbol, "accepted"}, [params]}} = response
      assert {:dictionary, dict} = params
      assert {:embedded, resolved_ref} = Map.get(dict, {:symbol, "responderSession"})
      assert resolved_ref.attenuation == [Caveat.passthrough()]
    end

    test "ignores non-resolve assertions", %{actor_pid: _actor_pid} do
      key = test_key()
      gatekeeper = Gatekeeper.new(key, test_resolver())

      # Some random assertion
      assertion = {:record, {{:symbol, "SomeOtherRecord"}, [{:string, "data"}]}}

      handle = Handle.new(:test, 1)
      {returned_gatekeeper, _turn} = Entity.on_publish(gatekeeper, assertion, handle, nil)

      # Should return the gatekeeper unchanged
      assert returned_gatekeeper == gatekeeper
    end
  end

  describe "Entity protocol - on_retract/3" do
    test "handles retraction gracefully" do
      key = test_key()
      gatekeeper = Gatekeeper.new(key, test_resolver())

      handle = Handle.new(:test, 1)
      {returned_gatekeeper, _turn} = Entity.on_retract(gatekeeper, handle, nil)

      # Should return unchanged
      assert returned_gatekeeper == gatekeeper
    end
  end

  describe "Entity protocol - on_message/3" do
    test "ignores messages" do
      key = test_key()
      gatekeeper = Gatekeeper.new(key, test_resolver())

      {returned_gatekeeper, _turn} = Entity.on_message(gatekeeper, {:symbol, "ping"}, nil)

      # Should return unchanged
      assert returned_gatekeeper == gatekeeper
    end
  end
end
