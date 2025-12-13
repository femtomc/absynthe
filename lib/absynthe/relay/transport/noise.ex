defmodule Absynthe.Relay.Transport.Noise do
  @moduledoc """
  Noise protocol transport wrapper for encrypted relay connections.

  Implements the NK handshake pattern where:
  - Server has a known static keypair
  - Client is anonymous (no static key)
  - Client must know server's public key beforehand

  This provides server authentication, forward secrecy, and encrypted
  transport without requiring client identity at the transport layer.
  Application-level authorization is handled separately via Gatekeeper.

  ## Protocol Details

  Uses `Noise_NK_25519_ChaChaPoly_SHA256`:
  - X25519 for key exchange
  - ChaCha20-Poly1305 for AEAD encryption
  - SHA-256 for hashing

  ## Usage (Server Side)

      # Generate or load keypair
      {:ok, keypair} = Noise.generate_keypair()

      # Perform server-side handshake
      {:ok, session} = Noise.server_handshake(socket, keypair)

      # Use session for encrypt/decrypt
      {:ok, ciphertext, session} = Noise.encrypt(session, plaintext)

  ## Usage (Client Side)

      # Perform client-side handshake (must know server's public key)
      {:ok, session} = Noise.client_handshake(socket, server_public_key)

      # Use session for encrypt/decrypt
      {:ok, plaintext, session} = Noise.decrypt(session, ciphertext)

  ## Wire Format

  Handshake and transport messages are length-prefixed with a 2-byte
  big-endian length field, followed by the payload.
  """

  require Logger

  @protocol_name "Noise_NK_25519_ChaChaPoly_SHA256"
  @handshake_timeout 5_000

  @typedoc """
  A Noise keypair: {public_key, private_key}.
  Both keys are 32-byte binaries for X25519.
  """
  @type keypair :: {public_key :: binary(), private_key :: binary()}

  @typedoc """
  An active Noise session after handshake completion.
  """
  @opaque session :: %{
            decibel_ref: reference(),
            socket: port()
          }

  # Keypair Management

  @doc """
  Generates a new random X25519 keypair.

  ## Returns

  `{:ok, {public_key, private_key}}` where both are 32-byte binaries.

  ## Examples

      {:ok, {public, private}} = Noise.generate_keypair()
      byte_size(public)   # => 32
      byte_size(private)  # => 32

  """
  @spec generate_keypair() :: {:ok, keypair()}
  def generate_keypair do
    {public_key, private_key} = :crypto.generate_key(:ecdh, :x25519)
    {:ok, {public_key, private_key}}
  end

  @doc """
  Derives a keypair deterministically from a 32-byte secret.

  This is useful for persistent server keys that can be regenerated
  from a stored secret.

  ## Parameters

  - `secret` - 32-byte binary used to derive the keypair

  ## Returns

  `{:ok, {public_key, private_key}}` or `{:error, reason}`

  ## Examples

      secret = :crypto.strong_rand_bytes(32)
      {:ok, keypair1} = Noise.keypair_from_secret(secret)
      {:ok, keypair2} = Noise.keypair_from_secret(secret)
      keypair1 == keypair2  # => true

  """
  @spec keypair_from_secret(binary()) :: {:ok, keypair()} | {:error, term()}
  def keypair_from_secret(secret) when byte_size(secret) == 32 do
    # Use HKDF to derive a proper private key from the secret
    private_key = :crypto.mac(:hmac, :sha256, "noise-keypair-derivation", secret)
    # Derive public key from private key using X25519 scalar multiplication
    {public_key, ^private_key} = :crypto.generate_key(:ecdh, :x25519, private_key)
    {:ok, {public_key, private_key}}
  end

  def keypair_from_secret(secret) when is_binary(secret) do
    {:error, {:invalid_secret_length, byte_size(secret), :expected, 32}}
  end

  def keypair_from_secret(_), do: {:error, :invalid_secret_type}

  @doc """
  Extracts the public key from a keypair.
  """
  @spec public_key(keypair()) :: binary()
  def public_key({public, _private}), do: public

  # Server-side Handshake

  @doc """
  Performs the server-side NK handshake.

  The server is the responder in the NK pattern. It has a known static
  keypair that the client uses to authenticate.

  ## Parameters

  - `socket` - A connected socket (must be in passive mode)
  - `keypair` - The server's static keypair

  ## Returns

  `{:ok, session}` on successful handshake, `{:error, reason}` on failure.

  ## Handshake Flow (NK Pattern)

  1. Client sends: e, es (ephemeral key + DH with server static)
  2. Server sends: e, ee (ephemeral key + DH with client ephemeral)

  """
  @spec server_handshake(port(), keypair()) :: {:ok, session()} | {:error, term()}
  def server_handshake(socket, {_public_key, _private_key} = keypair) do
    # Initialize Decibel as responder with our static keypair
    # Keys must be a map: %{s: {public, private}}
    decibel_ref = Decibel.new(@protocol_name, :rsp, %{s: keypair})

    # Receive first handshake message from client
    case recv_handshake_message(socket) do
      {:ok, client_msg} ->
        # Process client's message (mutates decibel_ref in process dict)
        try do
          _payload = Decibel.handshake_decrypt(decibel_ref, client_msg)

          # Generate and send our response
          server_msg = Decibel.handshake_encrypt(decibel_ref)

          case send_handshake_message(socket, IO.iodata_to_binary(server_msg)) do
            :ok ->
              if Decibel.is_handshake_complete?(decibel_ref) do
                Logger.debug("Noise server handshake complete")
                {:ok, %{decibel_ref: decibel_ref, socket: socket}}
              else
                Decibel.close(decibel_ref)
                {:error, :handshake_incomplete}
              end

            {:error, reason} ->
              Decibel.close(decibel_ref)
              {:error, {:send_failed, reason}}
          end
        rescue
          e in Decibel.DecryptionError ->
            Decibel.close(decibel_ref)
            {:error, {:decryption_error, e.message}}
        end

      {:error, reason} ->
        Decibel.close(decibel_ref)
        {:error, {:recv_failed, reason}}
    end
  end

  # Client-side Handshake

  @doc """
  Performs the client-side NK handshake.

  The client is the initiator in the NK pattern. It must know the
  server's public key beforehand.

  ## Parameters

  - `socket` - A connected socket (must be in passive mode)
  - `server_public_key` - The server's 32-byte public key

  ## Returns

  `{:ok, session}` on successful handshake, `{:error, reason}` on failure.

  """
  @spec client_handshake(port(), binary()) :: {:ok, session()} | {:error, term()}
  def client_handshake(socket, server_public_key) when byte_size(server_public_key) == 32 do
    # Initialize Decibel as initiator with server's static public key
    # Keys must be a map: %{rs: public_key}
    decibel_ref = Decibel.new(@protocol_name, :ini, %{rs: server_public_key})

    # Generate and send first handshake message
    client_msg = Decibel.handshake_encrypt(decibel_ref)

    case send_handshake_message(socket, IO.iodata_to_binary(client_msg)) do
      :ok ->
        # Receive server's response
        case recv_handshake_message(socket) do
          {:ok, server_msg} ->
            try do
              _payload = Decibel.handshake_decrypt(decibel_ref, server_msg)

              if Decibel.is_handshake_complete?(decibel_ref) do
                Logger.debug("Noise client handshake complete")
                {:ok, %{decibel_ref: decibel_ref, socket: socket}}
              else
                Decibel.close(decibel_ref)
                {:error, :handshake_incomplete}
              end
            rescue
              e in Decibel.DecryptionError ->
                Decibel.close(decibel_ref)
                {:error, {:decryption_error, e.message}}
            end

          {:error, reason} ->
            Decibel.close(decibel_ref)
            {:error, {:recv_failed, reason}}
        end

      {:error, reason} ->
        Decibel.close(decibel_ref)
        {:error, {:send_failed, reason}}
    end
  end

  def client_handshake(_socket, server_public_key) when is_binary(server_public_key) do
    {:error, {:invalid_public_key_length, byte_size(server_public_key), :expected, 32}}
  end

  def client_handshake(_socket, _), do: {:error, :invalid_public_key_type}

  # Session Encryption/Decryption

  @doc """
  Encrypts plaintext using the established Noise session.

  Returns the ciphertext and the same session (for API consistency).
  Note: Decibel mutates the session via process dictionary internally.

  ## Parameters

  - `session` - An active Noise session from handshake
  - `plaintext` - Binary data to encrypt

  ## Returns

  `{:ok, ciphertext, session}` or `{:error, reason}`

  """
  @spec encrypt(session(), binary()) :: {:ok, binary(), session()} | {:error, term()}
  def encrypt(%{decibel_ref: ref} = session, plaintext) when is_binary(plaintext) do
    ciphertext = Decibel.encrypt(ref, plaintext)
    {:ok, IO.iodata_to_binary(ciphertext), session}
  end

  @doc """
  Decrypts ciphertext using the established Noise session.

  Returns the plaintext and the same session (for API consistency).
  Note: Decibel mutates the session via process dictionary internally.

  ## Parameters

  - `session` - An active Noise session from handshake
  - `ciphertext` - Encrypted binary data

  ## Returns

  `{:ok, plaintext, session}` or `{:error, reason}`

  """
  @spec decrypt(session(), binary()) :: {:ok, binary(), session()} | {:error, term()}
  def decrypt(%{decibel_ref: ref} = session, ciphertext) when is_binary(ciphertext) do
    plaintext = Decibel.decrypt(ref, ciphertext)
    {:ok, IO.iodata_to_binary(plaintext), session}
  rescue
    e in Decibel.DecryptionError ->
      {:error, {:decryption_error, e.message}}
  end

  @doc """
  Closes a Noise session, releasing resources.
  """
  @spec close(session()) :: :ok
  def close(%{decibel_ref: ref}) do
    Decibel.close(ref)
  end

  @doc """
  Returns the handshake hash for channel binding.

  This 32-byte value uniquely identifies the session and can be used
  for higher-level authentication protocols.
  """
  @spec handshake_hash(session()) :: binary()
  def handshake_hash(%{decibel_ref: ref}) do
    Decibel.get_handshake_hash(ref)
  end

  # Wire Format Helpers

  # Handshake messages are length-prefixed with 2-byte big-endian length
  defp send_handshake_message(socket, message) do
    length = byte_size(message)
    packet = <<length::16-big, message::binary>>
    :gen_tcp.send(socket, packet)
  end

  defp recv_handshake_message(socket) do
    # First receive the 2-byte length prefix
    case :gen_tcp.recv(socket, 2, @handshake_timeout) do
      {:ok, <<length::16-big>>} ->
        # Then receive the message body
        case :gen_tcp.recv(socket, length, @handshake_timeout) do
          {:ok, message} -> {:ok, message}
          {:error, reason} -> {:error, {:recv_body, reason}}
        end

      {:ok, _} ->
        {:error, :invalid_length_prefix}

      {:error, reason} ->
        {:error, {:recv_length, reason}}
    end
  end

  # Transport-level send/recv with length framing

  @doc """
  Encrypts and sends data over the socket with length framing.

  ## Parameters

  - `session` - Active Noise session
  - `plaintext` - Data to encrypt and send

  ## Returns

  `{:ok, updated_session}` or `{:error, reason}`

  """
  @spec send_encrypted(session(), binary()) :: {:ok, session()} | {:error, term()}
  def send_encrypted(%{socket: socket} = session, plaintext) do
    case encrypt(session, plaintext) do
      {:ok, ciphertext, session} ->
        length = byte_size(ciphertext)
        packet = <<length::16-big, ciphertext::binary>>

        case :gen_tcp.send(socket, packet) do
          :ok -> {:ok, session}
          {:error, reason} -> {:error, {:send, reason}}
        end

      {:error, reason} ->
        {:error, {:encrypt, reason}}
    end
  end

  @doc """
  Receives and decrypts a length-framed message from the socket.

  ## Parameters

  - `session` - Active Noise session
  - `timeout` - Receive timeout in milliseconds

  ## Returns

  `{:ok, plaintext, updated_session}` or `{:error, reason}`

  """
  @spec recv_encrypted(session(), timeout()) :: {:ok, binary(), session()} | {:error, term()}
  def recv_encrypted(%{socket: socket} = session, timeout \\ 5_000) do
    case :gen_tcp.recv(socket, 2, timeout) do
      {:ok, <<length::16-big>>} ->
        case :gen_tcp.recv(socket, length, timeout) do
          {:ok, ciphertext} ->
            decrypt(session, ciphertext)

          {:error, reason} ->
            {:error, {:recv_body, reason}}
        end

      {:error, reason} ->
        {:error, {:recv_length, reason}}
    end
  end
end
