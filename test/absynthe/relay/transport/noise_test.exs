defmodule Absynthe.Relay.Transport.NoiseTest do
  use ExUnit.Case, async: false

  alias Absynthe.Relay.Transport.Noise

  # Note: Decibel uses process dictionary to store session state,
  # so handshakes must be done in the same process that will use the session.
  # This makes testing with separate server/client processes tricky.

  describe "generate_keypair/0" do
    test "generates valid keypair with correct sizes" do
      {:ok, {public, private}} = Noise.generate_keypair()
      assert byte_size(public) == 32
      assert byte_size(private) == 32
    end

    test "generates different keypairs each time" do
      {:ok, keypair1} = Noise.generate_keypair()
      {:ok, keypair2} = Noise.generate_keypair()
      assert keypair1 != keypair2
    end
  end

  describe "keypair_from_secret/1" do
    test "derives valid keypair from 32-byte secret" do
      secret = :crypto.strong_rand_bytes(32)
      {:ok, {public, private}} = Noise.keypair_from_secret(secret)
      assert byte_size(public) == 32
      assert byte_size(private) == 32
    end

    test "derivation is deterministic" do
      secret = :crypto.strong_rand_bytes(32)
      {:ok, keypair1} = Noise.keypair_from_secret(secret)
      {:ok, keypair2} = Noise.keypair_from_secret(secret)
      assert keypair1 == keypair2
    end

    test "different secrets produce different keypairs" do
      secret1 = :crypto.strong_rand_bytes(32)
      secret2 = :crypto.strong_rand_bytes(32)
      {:ok, keypair1} = Noise.keypair_from_secret(secret1)
      {:ok, keypair2} = Noise.keypair_from_secret(secret2)
      assert keypair1 != keypair2
    end

    test "rejects secrets with wrong length" do
      assert {:error, {:invalid_secret_length, 16, :expected, 32}} =
               Noise.keypair_from_secret(:crypto.strong_rand_bytes(16))

      assert {:error, {:invalid_secret_length, 64, :expected, 32}} =
               Noise.keypair_from_secret(:crypto.strong_rand_bytes(64))
    end

    test "rejects non-binary input" do
      assert {:error, :invalid_secret_type} = Noise.keypair_from_secret(12345)
      assert {:error, :invalid_secret_type} = Noise.keypair_from_secret(:atom)
    end
  end

  describe "public_key/1" do
    test "extracts public key from keypair" do
      {:ok, {public, _private} = keypair} = Noise.generate_keypair()
      assert Noise.public_key(keypair) == public
    end
  end

  describe "handshake" do
    test "successful NK handshake and encrypted communication" do
      socket_path = "/tmp/noise_test_#{:erlang.unique_integer([:positive])}.sock"

      on_exit(fn ->
        File.rm(socket_path)
      end)

      {:ok, server_keypair} = Noise.generate_keypair()
      {server_public, _} = server_keypair

      # Start listener
      {:ok, listen} =
        :gen_tcp.listen(0, [:binary, {:active, false}, {:ifaddr, {:local, socket_path}}])

      parent = self()

      # Server process - does handshake and encryption in same process
      server_pid =
        spawn_link(fn ->
          {:ok, server_socket} = :gen_tcp.accept(listen, 5_000)
          {:ok, server_session} = Noise.server_handshake(server_socket, server_keypair)

          # Wait for client's encrypted message
          {:ok, <<len::16-big>>} = :gen_tcp.recv(server_socket, 2, 5_000)
          {:ok, ciphertext} = :gen_tcp.recv(server_socket, len, 5_000)
          {:ok, plaintext, server_session} = Noise.decrypt(server_session, ciphertext)

          send(parent, {:server_decrypted, plaintext})

          # Send encrypted response
          {:ok, response_ct, _} = Noise.encrypt(server_session, "pong")
          :gen_tcp.send(server_socket, <<byte_size(response_ct)::16-big, response_ct::binary>>)

          :gen_tcp.close(server_socket)
        end)

      # Give server time to start accepting
      Process.sleep(10)

      # Client process (in test process)
      {:ok, client_socket} =
        :gen_tcp.connect({:local, socket_path}, 0, [:binary, {:active, false}])

      {:ok, client_session} = Noise.client_handshake(client_socket, server_public)

      # Send encrypted message
      {:ok, ciphertext, client_session} = Noise.encrypt(client_session, "ping")
      :gen_tcp.send(client_socket, <<byte_size(ciphertext)::16-big, ciphertext::binary>>)

      # Receive encrypted response
      {:ok, <<resp_len::16-big>>} = :gen_tcp.recv(client_socket, 2, 5_000)
      {:ok, response_ct} = :gen_tcp.recv(client_socket, resp_len, 5_000)
      {:ok, response_pt, _} = Noise.decrypt(client_session, response_ct)

      # Verify communication worked
      assert_receive {:server_decrypted, "ping"}, 5_000
      assert response_pt == "pong"

      # Clean up
      Noise.close(client_session)
      :gen_tcp.close(client_socket)
      :gen_tcp.close(listen)
      Process.unlink(server_pid)
    end

    test "handshake fails with wrong server key" do
      socket_path = "/tmp/noise_test_#{:erlang.unique_integer([:positive])}.sock"

      on_exit(fn ->
        File.rm(socket_path)
      end)

      {:ok, server_keypair} = Noise.generate_keypair()
      {:ok, {wrong_public, _}} = Noise.generate_keypair()

      {:ok, listen} =
        :gen_tcp.listen(0, [:binary, {:active, false}, {:ifaddr, {:local, socket_path}}])

      parent = self()

      # Server process
      server_pid =
        spawn_link(fn ->
          {:ok, server_socket} = :gen_tcp.accept(listen, 5_000)
          # Server handshake will fail because client used wrong key
          result = Noise.server_handshake(server_socket, server_keypair)
          send(parent, {:server_result, result})
          :gen_tcp.close(server_socket)
        end)

      Process.sleep(10)

      {:ok, client_socket} =
        :gen_tcp.connect({:local, socket_path}, 0, [:binary, {:active, false}])

      # Client uses wrong key - server will fail to decrypt
      _client_result = Noise.client_handshake(client_socket, wrong_public)

      # Server should fail (wrong key means MAC verification fails)
      assert_receive {:server_result, {:error, _reason}}, 5_000

      :gen_tcp.close(client_socket)
      :gen_tcp.close(listen)
      Process.unlink(server_pid)
    end

    test "client rejects invalid public key length" do
      {:ok, socket} = :gen_tcp.listen(0, [:binary, {:active, false}])

      assert {:error, {:invalid_public_key_length, 16, :expected, 32}} =
               Noise.client_handshake(socket, :crypto.strong_rand_bytes(16))

      :gen_tcp.close(socket)
    end
  end

  describe "encrypt/decrypt" do
    test "bidirectional encrypted communication" do
      socket_path = "/tmp/noise_enc_test_#{:erlang.unique_integer([:positive])}.sock"

      on_exit(fn ->
        File.rm(socket_path)
      end)

      {:ok, server_keypair} = Noise.generate_keypair()
      {server_public, _} = server_keypair

      {:ok, listen} =
        :gen_tcp.listen(0, [:binary, {:active, false}, {:ifaddr, {:local, socket_path}}])

      parent = self()

      # Server process - handles encryption in its own process
      server_pid =
        spawn_link(fn ->
          {:ok, server_socket} = :gen_tcp.accept(listen, 5_000)
          {:ok, session} = Noise.server_handshake(server_socket, server_keypair)

          # Wait for messages from client
          messages =
            Enum.map(1..3, fn _ ->
              {:ok, <<len::16-big>>} = :gen_tcp.recv(server_socket, 2, 5_000)
              {:ok, ct} = :gen_tcp.recv(server_socket, len, 5_000)
              {:ok, pt, session} = Noise.decrypt(session, ct)
              # Update session in loop (needed for nonce increment)
              Process.put(:session, session)
              pt
            end)

          session = Process.get(:session, session)

          # Send response
          {:ok, ct, _} = Noise.encrypt(session, "all received")
          :gen_tcp.send(server_socket, <<byte_size(ct)::16-big, ct::binary>>)

          send(parent, {:server_messages, messages})
          :gen_tcp.close(server_socket)
        end)

      Process.sleep(10)

      {:ok, client_socket} =
        :gen_tcp.connect({:local, socket_path}, 0, [:binary, {:active, false}])

      {:ok, client_session} = Noise.client_handshake(client_socket, server_public)

      # Send multiple messages
      messages = ["msg1", "message two", "third message"]

      client_session =
        Enum.reduce(messages, client_session, fn msg, session ->
          {:ok, ct, session} = Noise.encrypt(session, msg)
          :gen_tcp.send(client_socket, <<byte_size(ct)::16-big, ct::binary>>)
          session
        end)

      # Receive response
      {:ok, <<len::16-big>>} = :gen_tcp.recv(client_socket, 2, 5_000)
      {:ok, ct} = :gen_tcp.recv(client_socket, len, 5_000)
      {:ok, response, _} = Noise.decrypt(client_session, ct)

      assert_receive {:server_messages, ^messages}, 5_000
      assert response == "all received"

      :gen_tcp.close(client_socket)
      :gen_tcp.close(listen)
      Process.unlink(server_pid)
    end

    test "ciphertext cannot be modified (integrity)" do
      socket_path = "/tmp/noise_int_test_#{:erlang.unique_integer([:positive])}.sock"

      on_exit(fn ->
        File.rm(socket_path)
      end)

      {:ok, server_keypair} = Noise.generate_keypair()
      {server_public, _} = server_keypair

      {:ok, listen} =
        :gen_tcp.listen(0, [:binary, {:active, false}, {:ifaddr, {:local, socket_path}}])

      parent = self()

      # Server that will try to decrypt tampered message
      server_pid =
        spawn_link(fn ->
          {:ok, server_socket} = :gen_tcp.accept(listen, 5_000)
          {:ok, session} = Noise.server_handshake(server_socket, server_keypair)

          # Receive tampered ciphertext
          {:ok, <<len::16-big>>} = :gen_tcp.recv(server_socket, 2, 5_000)
          {:ok, tampered_ct} = :gen_tcp.recv(server_socket, len, 5_000)

          result = Noise.decrypt(session, tampered_ct)
          send(parent, {:decrypt_result, result})
          :gen_tcp.close(server_socket)
        end)

      Process.sleep(10)

      {:ok, client_socket} =
        :gen_tcp.connect({:local, socket_path}, 0, [:binary, {:active, false}])

      {:ok, client_session} = Noise.client_handshake(client_socket, server_public)

      # Encrypt a message
      {:ok, ciphertext, _} = Noise.encrypt(client_session, "authenticated message")

      # Tamper with ciphertext
      tampered =
        ciphertext
        |> :binary.bin_to_list()
        |> Enum.with_index()
        |> Enum.map(fn {byte, i} -> if i == 5, do: rem(byte + 1, 256), else: byte end)
        |> :binary.list_to_bin()

      # Send tampered ciphertext
      :gen_tcp.send(client_socket, <<byte_size(tampered)::16-big, tampered::binary>>)

      # Server should fail to decrypt
      assert_receive {:decrypt_result, {:error, _}}, 5_000

      :gen_tcp.close(client_socket)
      :gen_tcp.close(listen)
      Process.unlink(server_pid)
    end
  end

  describe "handshake_hash/1" do
    test "returns 32-byte hash and is same on both sides" do
      socket_path = "/tmp/noise_hash_test_#{:erlang.unique_integer([:positive])}.sock"

      on_exit(fn ->
        File.rm(socket_path)
      end)

      {:ok, server_keypair} = Noise.generate_keypair()
      {server_public, _} = server_keypair

      {:ok, listen} =
        :gen_tcp.listen(0, [:binary, {:active, false}, {:ifaddr, {:local, socket_path}}])

      parent = self()

      # Server sends its handshake hash
      server_pid =
        spawn_link(fn ->
          {:ok, server_socket} = :gen_tcp.accept(listen, 5_000)
          {:ok, session} = Noise.server_handshake(server_socket, server_keypair)
          hash = Noise.handshake_hash(session)
          send(parent, {:server_hash, hash})
          :gen_tcp.close(server_socket)
        end)

      Process.sleep(10)

      {:ok, client_socket} =
        :gen_tcp.connect({:local, socket_path}, 0, [:binary, {:active, false}])

      {:ok, client_session} = Noise.client_handshake(client_socket, server_public)

      client_hash = Noise.handshake_hash(client_session)

      assert byte_size(client_hash) == 32
      assert_receive {:server_hash, server_hash}, 5_000
      assert byte_size(server_hash) == 32
      assert client_hash == server_hash

      :gen_tcp.close(client_socket)
      :gen_tcp.close(listen)
      Process.unlink(server_pid)
    end
  end
end
