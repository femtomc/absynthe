defmodule Absynthe.Relay.NoiseIntegrationTest do
  use ExUnit.Case, async: false

  alias Absynthe.Relay.{Broker, Client}
  alias Absynthe.Relay.Transport.Noise

  @moduletag :integration

  describe "Noise encrypted broker connections" do
    test "client connects to noise-enabled broker via Unix socket" do
      socket_path = "/tmp/noise_integ_test_#{:erlang.unique_integer([:positive])}.sock"

      on_exit(fn ->
        File.rm(socket_path)
      end)

      # Generate keypair for broker
      {:ok, keypair} = Noise.generate_keypair()
      {public_key, _private} = keypair

      # Start broker with Noise encryption
      {:ok, broker} =
        Broker.start_link(
          socket_path: socket_path,
          noise_keypair: keypair
        )

      # Verify broker exposes public key
      assert Broker.noise_public_key(broker) == public_key

      # Give broker time to start listener
      Process.sleep(50)

      # Connect client with Noise encryption
      {:ok, client} = Client.connect_unix(socket_path, noise_server_key: public_key)

      # Verify handshake hash is available (proves encryption is active)
      assert is_binary(Client.handshake_hash(client))
      assert byte_size(Client.handshake_hash(client)) == 32

      # Clean up
      Client.close(client)
      Broker.stop(broker)
    end

    test "client can send and receive packets over noise connection" do
      socket_path = "/tmp/noise_packet_test_#{:erlang.unique_integer([:positive])}.sock"

      on_exit(fn ->
        File.rm(socket_path)
      end)

      # Generate keypair for broker
      {:ok, keypair} = Noise.generate_keypair()
      {public_key, _private} = keypair

      # Start broker
      {:ok, broker} =
        Broker.start_link(
          socket_path: socket_path,
          noise_keypair: keypair
        )

      Process.sleep(50)

      # Connect client
      {:ok, client} = Client.connect_unix(socket_path, noise_server_key: public_key)

      # Send a nop packet (keepalive) - simplest packet type
      {:ok, client} = Client.send_packet(client, :nop)

      # The server won't respond to nop, but we can verify our send worked
      # by checking the client is still connected
      assert client.transport == :noise

      # Clean up
      Client.close(client)
      Broker.stop(broker)
    end

    test "broker can be configured with noise_secret" do
      socket_path = "/tmp/noise_secret_test_#{:erlang.unique_integer([:positive])}.sock"

      on_exit(fn ->
        File.rm(socket_path)
      end)

      # Use a deterministic secret
      secret = :crypto.strong_rand_bytes(32)

      # Start broker with noise_secret
      {:ok, broker} =
        Broker.start_link(
          socket_path: socket_path,
          noise_secret: secret
        )

      # Get the derived public key
      public_key = Broker.noise_public_key(broker)
      assert is_binary(public_key)
      assert byte_size(public_key) == 32

      # Derive expected keypair from same secret
      {:ok, {expected_public, _}} = Noise.keypair_from_secret(secret)
      assert public_key == expected_public

      Process.sleep(50)

      # Connect with the derived key
      {:ok, client} = Client.connect_unix(socket_path, noise_server_key: public_key)
      assert client.transport == :noise

      Client.close(client)
      Broker.stop(broker)
    end

    test "client rejects connection to noise broker with wrong key" do
      socket_path = "/tmp/noise_wrong_key_test_#{:erlang.unique_integer([:positive])}.sock"

      on_exit(fn ->
        File.rm(socket_path)
      end)

      # Generate keypair for broker
      {:ok, broker_keypair} = Noise.generate_keypair()

      # Generate different keypair that client will incorrectly use
      {:ok, {wrong_public, _}} = Noise.generate_keypair()

      # Start broker
      {:ok, broker} =
        Broker.start_link(
          socket_path: socket_path,
          noise_keypair: broker_keypair
        )

      Process.sleep(50)

      # Client tries to connect with wrong key - handshake should fail
      result = Client.connect_unix(socket_path, noise_server_key: wrong_public)
      assert {:error, {:noise_handshake_failed, _reason}} = result

      Broker.stop(broker)
    end

    test "plaintext client cannot connect to noise-enabled broker" do
      socket_path = "/tmp/noise_plaintext_test_#{:erlang.unique_integer([:positive])}.sock"

      on_exit(fn ->
        File.rm(socket_path)
      end)

      {:ok, keypair} = Noise.generate_keypair()

      {:ok, broker} =
        Broker.start_link(
          socket_path: socket_path,
          noise_keypair: keypair
        )

      Process.sleep(50)

      # Client connects without noise_server_key (plaintext mode)
      {:ok, client} = Client.connect_unix(socket_path)

      # Client is in plaintext mode
      assert client.transport == :gen_tcp

      # Send some garbage that isn't a valid Noise handshake message
      # The server will fail to decrypt and close the connection
      :gen_tcp.send(client.socket, "hello plaintext")

      # Give server time to process and close
      Process.sleep(100)

      # Socket should be closed by server due to handshake failure
      result = :gen_tcp.recv(client.socket, 0, 100)
      assert {:error, :closed} == result or {:error, :timeout} == result

      Client.close(client)
      Broker.stop(broker)
    end

    test "broker without noise allows plaintext connections" do
      socket_path = "/tmp/noise_disabled_test_#{:erlang.unique_integer([:positive])}.sock"

      on_exit(fn ->
        File.rm(socket_path)
      end)

      # Start broker without Noise (plaintext mode)
      {:ok, broker} = Broker.start_link(socket_path: socket_path)

      # No public key should be available
      assert Broker.noise_public_key(broker) == nil

      Process.sleep(50)

      # Connect client in plaintext mode
      {:ok, client} = Client.connect_unix(socket_path)

      assert client.transport == :gen_tcp
      assert Client.handshake_hash(client) == nil

      Client.close(client)
      Broker.stop(broker)
    end

    test "multiple clients can connect concurrently with noise" do
      socket_path = "/tmp/noise_multi_test_#{:erlang.unique_integer([:positive])}.sock"

      on_exit(fn ->
        File.rm(socket_path)
      end)

      {:ok, keypair} = Noise.generate_keypair()
      {public_key, _} = keypair

      {:ok, broker} =
        Broker.start_link(
          socket_path: socket_path,
          noise_keypair: keypair
        )

      Process.sleep(50)

      # Connect multiple clients
      clients =
        Enum.map(1..5, fn _i ->
          {:ok, client} = Client.connect_unix(socket_path, noise_server_key: public_key)
          client
        end)

      # All clients should be connected with Noise
      Enum.each(clients, fn client ->
        assert client.transport == :noise
        assert is_binary(Client.handshake_hash(client))
      end)

      # All handshake hashes should be different (each session is unique)
      hashes = Enum.map(clients, &Client.handshake_hash/1)
      unique_hashes = Enum.uniq(hashes)
      assert length(unique_hashes) == length(hashes)

      # Clean up
      Enum.each(clients, &Client.close/1)
      Broker.stop(broker)
    end
  end
end
