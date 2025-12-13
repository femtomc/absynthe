# Test helper entities - must be defined before the test module
defmodule Absynthe.Relay.GatekeeperIntegrationTest.TestService do
  @moduledoc false
  defstruct [:name, :replies]
end

defimpl Absynthe.Core.Entity, for: Absynthe.Relay.GatekeeperIntegrationTest.TestService do
  def on_publish(entity, _assertion, _handle, turn), do: {entity, turn}
  def on_retract(entity, _handle, turn), do: {entity, turn}

  def on_message(entity, message, turn) do
    {%{entity | replies: [message | entity.replies]}, turn}
  end

  def on_sync(entity, _peer, turn), do: {entity, turn}
end

defmodule Absynthe.Relay.GatekeeperIntegrationTest.TestCounter do
  @moduledoc false
  defstruct [:value]
end

defimpl Absynthe.Core.Entity, for: Absynthe.Relay.GatekeeperIntegrationTest.TestCounter do
  def on_publish(entity, _assertion, _handle, turn), do: {entity, turn}
  def on_retract(entity, _handle, turn), do: {entity, turn}

  def on_message(
        %Absynthe.Relay.GatekeeperIntegrationTest.TestCounter{value: v} = entity,
        {:symbol, "increment"},
        turn
      ) do
    {%{entity | value: v + 1}, turn}
  end

  def on_message(entity, _message, turn), do: {entity, turn}

  def on_sync(entity, _peer, turn), do: {entity, turn}
end

defmodule Absynthe.Relay.GatekeeperIntegrationTest do
  @moduledoc """
  Integration tests demonstrating cross-node authentication using the Gatekeeper.

  These tests show how a remote client can present a sturdy ref to authenticate
  and gain access to a service via the relay.
  """
  use ExUnit.Case

  alias Absynthe.Core.{Actor, Caveat, Ref, SturdyRef}
  alias Absynthe.Relay.{Broker, Gatekeeper}
  alias __MODULE__.{TestService, TestCounter}

  @moduletag :integration
  @moduletag timeout: 10_000

  describe "Gatekeeper authentication flow" do
    test "client authenticates with valid sturdy ref and receives resolved capability" do
      # Setup: Create broker with gatekeeper
      key = :crypto.strong_rand_bytes(32)
      socket_path = "/tmp/absynthe_gatekeeper_test_#{:erlang.unique_integer([:positive])}.sock"

      {:ok, broker} = Broker.start_link(socket_path: socket_path)
      actor_pid = Broker.actor_pid(broker)
      _dataspace_ref = Broker.dataspace_ref(broker)

      # Create a service that the client wants to access
      {:ok, service_ref} =
        Actor.spawn_entity(actor_pid, :root, %TestService{
          name: "echo",
          replies: []
        })

      # Register the service with the gatekeeper
      registry = %{
        {:symbol, "echo-service"} => {Ref.actor_id(service_ref), Ref.entity_id(service_ref)}
      }

      gatekeeper = Gatekeeper.with_registry(key, registry)
      {:ok, _gatekeeper_ref} = Actor.spawn_entity(actor_pid, :root, gatekeeper)

      # Mint a sturdy ref for the service
      {:ok, sturdy_ref} = SturdyRef.mint({:symbol, "echo-service"}, key)
      _sturdy_ref_preserves = SturdyRef.to_preserves(sturdy_ref)

      Process.sleep(100)

      # Client connects
      {:ok, client} = :gen_tcp.connect({:local, socket_path}, 0, [:binary, active: false])

      # For this test, we verify the gatekeeper can resolve the sturdy ref
      # by materializing it directly (simulating what the relay would do)
      {:ok, resolved_ref} = SturdyRef.materialize(sturdy_ref, key, gatekeeper.resolver)

      assert resolved_ref.actor_id == Ref.actor_id(service_ref)
      assert resolved_ref.entity_id == Ref.entity_id(service_ref)

      # Close client
      :gen_tcp.close(client)
      Broker.stop(broker)
    end

    test "gatekeeper rejects invalid sturdy ref signature" do
      key = :crypto.strong_rand_bytes(32)
      wrong_key = :crypto.strong_rand_bytes(32)

      socket_path = "/tmp/absynthe_gatekeeper_test_#{:erlang.unique_integer([:positive])}.sock"

      {:ok, broker} = Broker.start_link(socket_path: socket_path)
      actor_pid = Broker.actor_pid(broker)

      # Create and register gatekeeper
      registry = %{{:symbol, "test-service"} => {:actor, :entity}}
      gatekeeper = Gatekeeper.with_registry(key, registry)
      {:ok, _gatekeeper_ref} = Actor.spawn_entity(actor_pid, :root, gatekeeper)

      # Mint sturdy ref with WRONG key
      {:ok, bad_sturdy_ref} = SturdyRef.mint({:symbol, "test-service"}, wrong_key)

      # This should fail validation
      assert {:error, :invalid_signature} = SturdyRef.validate(bad_sturdy_ref, key)

      Broker.stop(broker)
    end

    test "gatekeeper preserves attenuation from sturdy ref" do
      key = :crypto.strong_rand_bytes(32)

      # Create a sturdy ref with attenuation
      {:ok, sref} = SturdyRef.mint({:symbol, "restricted-service"}, key)
      {:ok, attenuated_sref} = SturdyRef.attenuate(sref, Caveat.passthrough(), key)

      # Create gatekeeper
      registry = %{{:symbol, "restricted-service"} => {:actor, :entity}}
      gatekeeper = Gatekeeper.with_registry(key, registry)

      # Materialize the attenuated sturdy ref
      {:ok, ref} = SturdyRef.materialize(attenuated_sref, key, gatekeeper.resolver)

      # The resulting ref should have attenuation
      assert Ref.attenuated?(ref)
    end

    test "end-to-end authentication with service access" do
      key = :crypto.strong_rand_bytes(32)
      socket_path = "/tmp/absynthe_gatekeeper_e2e_#{:erlang.unique_integer([:positive])}.sock"

      {:ok, broker} = Broker.start_link(socket_path: socket_path)
      actor_pid = Broker.actor_pid(broker)
      _dataspace_ref = Broker.dataspace_ref(broker)

      # Create a counter service
      {:ok, counter_ref} =
        Actor.spawn_entity(actor_pid, :root, %TestCounter{value: 0})

      # Create gatekeeper with the counter service registered
      registry = %{
        {:symbol, "counter"} => {Ref.actor_id(counter_ref), Ref.entity_id(counter_ref)}
      }

      gatekeeper = Gatekeeper.with_registry(key, registry)
      {:ok, _gatekeeper_ref} = Actor.spawn_entity(actor_pid, :root, gatekeeper)

      # Mint a sturdy ref that the "client" will use
      {:ok, counter_sref} = SturdyRef.mint({:symbol, "counter"}, key)

      # Validate it works
      assert SturdyRef.valid?(counter_sref, key)

      # Materialize to get a live ref
      {:ok, live_counter_ref} = SturdyRef.materialize(counter_sref, key, gatekeeper.resolver)

      assert live_counter_ref.actor_id == Ref.actor_id(counter_ref)
      assert live_counter_ref.entity_id == Ref.entity_id(counter_ref)

      Broker.stop(broker)
    end

    test "attenuated sturdy ref limits capabilities" do
      key = :crypto.strong_rand_bytes(32)

      # Create a sturdy ref and attenuate it
      {:ok, sref} = SturdyRef.mint({:symbol, "full-service"}, key)

      # Add a restrictive caveat
      pattern = {:record, {{:symbol, "Query"}, [{:symbol, "$"}]}}
      query_only_caveat = Caveat.rewrite(pattern, {:ref, 0})

      {:ok, read_only_sref} = SturdyRef.attenuate(sref, query_only_caveat, key)

      # Create gatekeeper
      registry = %{{:symbol, "full-service"} => {:actor, :service}}
      gatekeeper = Gatekeeper.with_registry(key, registry)

      # Materialize the attenuated ref
      {:ok, ref} = SturdyRef.materialize(read_only_sref, key, gatekeeper.resolver)

      # The ref should have the caveat in its attenuation
      assert Ref.attenuated?(ref)
      attenuation = Ref.attenuation(ref)
      assert length(attenuation) == 1
      assert {:rewrite, ^pattern, {:ref, 0}} = hd(attenuation)
    end
  end
end
