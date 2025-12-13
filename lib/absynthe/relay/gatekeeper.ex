defmodule Absynthe.Relay.Gatekeeper do
  @moduledoc """
  Gatekeeper entity for validating and resolving sturdy references.

  The Gatekeeper is an entity that receives sturdy reference resolution requests
  from remote peers and validates them using a secret key. When valid, it returns
  a live Ref that the peer can use to interact with the target entity.

  ## Protocol

  Clients send resolve requests as assertions:

      <resolve <sturdy-ref {...}> observer-ref>

  The Gatekeeper validates the sturdy ref and responds to the observer:

  - On success: Sends a message `<accepted resolved-ref>` to the observer
  - On failure: Sends a message `<rejected reason>` to the observer

  ## Security

  The Gatekeeper:
  - Validates cryptographic signatures on sturdy refs
  - Enforces attenuation from caveats in the sturdy ref
  - Prevents capability amplification on re-import
  - Uses constant-time comparison for signatures

  ## Usage

      # Create a gatekeeper with a secret key
      key = :crypto.strong_rand_bytes(32)
      gatekeeper = Gatekeeper.new(key, resolver_fn)

      # Spawn as an entity
      {:ok, gatekeeper_ref} = Actor.spawn_entity(actor_pid, :root, gatekeeper)

  ## Wire Format

  The resolve assertion format:

      <resolve {step: <ref {oid: any, sig: bytes, caveats: [...]}>} {observer: embedded-ref}>

  Response formats:

      <accepted {responderSession: embedded-ref}>
      <rejected {detail: any}>

  """

  alias Absynthe.Core.{Ref, SturdyRef}

  @typedoc """
  Resolver function that maps object IDs to actor/entity ID pairs.

  Takes an oid (any Preserves value) and returns either:
  - `{:ok, {actor_id, entity_id}}` if the oid is known
  - `{:error, reason}` if the oid cannot be resolved
  """
  @type resolver :: (term() -> {:ok, {term(), term()}} | {:error, term()})

  @typedoc """
  Gatekeeper state.
  """
  @type t :: %__MODULE__{
          key: binary(),
          resolver: resolver()
        }

  @enforce_keys [:key, :resolver]
  defstruct [:key, :resolver]

  @doc """
  Creates a new Gatekeeper entity.

  ## Parameters

  - `key` - Secret key for validating sturdy ref signatures (32 bytes recommended)
  - `resolver` - Function to resolve oids to actor/entity pairs

  ## Examples

      key = :crypto.strong_rand_bytes(32)
      resolver = fn {:symbol, "my-service"} -> {:ok, {:actor, :entity}} end
      gatekeeper = Gatekeeper.new(key, resolver)

  """
  @spec new(binary(), resolver()) :: t()
  def new(key, resolver) when is_binary(key) and is_function(resolver, 1) do
    %__MODULE__{key: key, resolver: resolver}
  end

  @doc """
  Creates a Gatekeeper with a registry-based resolver.

  The resolver looks up oids in a map of registered services.

  ## Parameters

  - `key` - Secret key for validating sturdy ref signatures
  - `registry` - Map of oid -> {actor_id, entity_id} pairs

  ## Examples

      key = :crypto.strong_rand_bytes(32)
      registry = %{
        {:symbol, "dataspace"} => {:broker, :dataspace}
      }
      gatekeeper = Gatekeeper.with_registry(key, registry)

  """
  @spec with_registry(binary(), %{term() => {term(), term()}}) :: t()
  def with_registry(key, registry) when is_binary(key) and is_map(registry) do
    resolver = fn oid ->
      case Map.fetch(registry, oid) do
        {:ok, {actor_id, entity_id}} -> {:ok, {actor_id, entity_id}}
        :error -> {:error, {:unknown_oid, oid}}
      end
    end

    new(key, resolver)
  end

  @doc """
  Mints a new sturdy ref for a service.

  This is a convenience function for creating sturdy refs that the
  Gatekeeper will be able to validate.

  ## Parameters

  - `gatekeeper` - The Gatekeeper instance
  - `oid` - Object identifier for the service

  ## Returns

  `{:ok, sturdy_ref}` or `{:error, reason}`

  """
  @spec mint(t(), term()) :: {:ok, SturdyRef.t()} | {:error, term()}
  def mint(%__MODULE__{key: key}, oid) do
    SturdyRef.mint(oid, key)
  end

  @doc """
  Mints a new sturdy ref, raising on failure.
  """
  @spec mint!(t(), term()) :: SturdyRef.t()
  def mint!(%__MODULE__{key: key}, oid) do
    SturdyRef.mint!(oid, key)
  end

  @doc """
  Attenuates a sturdy ref with a caveat.

  ## Parameters

  - `gatekeeper` - The Gatekeeper instance
  - `sturdy_ref` - The sturdy ref to attenuate
  - `caveat` - The caveat to add

  ## Returns

  `{:ok, attenuated_ref}` or `{:error, reason}`

  """
  @spec attenuate(t(), SturdyRef.t(), term()) :: {:ok, SturdyRef.t()} | {:error, term()}
  def attenuate(%__MODULE__{key: key}, sturdy_ref, caveat) do
    SturdyRef.attenuate(sturdy_ref, caveat, key)
  end
end

defimpl Absynthe.Core.Entity, for: Absynthe.Relay.Gatekeeper do
  require Logger

  alias Absynthe.Core.{Actor, Ref, SturdyRef}
  alias Absynthe.Relay.Gatekeeper

  @doc """
  Handles resolve assertions.

  Expects assertions of the form:
  - `<resolve {step: sturdy-ref-value} {observer: embedded-ref}>`

  """
  def on_publish(%Gatekeeper{} = gatekeeper, assertion, _handle, turn) do
    case parse_resolve_assertion(assertion) do
      {:ok, sturdy_ref_value, observer_ref} ->
        resolve_sturdy_ref(gatekeeper, sturdy_ref_value, observer_ref, turn)

      :not_resolve ->
        # Not a resolve assertion, ignore
        {gatekeeper, turn}

      {:error, reason} ->
        Logger.warning("Gatekeeper: invalid assertion format: #{inspect(reason)}")
        {gatekeeper, turn}
    end
  end

  def on_retract(%Gatekeeper{} = gatekeeper, _handle, turn) do
    # Resolve assertions don't need cleanup when retracted
    {gatekeeper, turn}
  end

  def on_message(%Gatekeeper{} = gatekeeper, _message, turn) do
    # Gatekeeper doesn't handle messages
    {gatekeeper, turn}
  end

  def on_sync(%Gatekeeper{} = gatekeeper, peer, turn) do
    # Respond to sync requests
    send(Ref.actor_id(peer), {:synced, peer})
    {gatekeeper, turn}
  end

  # Parse a resolve assertion
  # Format: <resolve {step: sturdy-ref} {observer: #ref}>
  defp parse_resolve_assertion({:record, {{:symbol, "resolve"}, [params]}}) do
    case params do
      {:dictionary, dict} ->
        with {:ok, step} <- extract_field(dict, "step"),
             {:ok, {:embedded, observer_ref}} <- extract_field(dict, "observer") do
          {:ok, step, observer_ref}
        else
          error -> {:error, {:invalid_resolve_params, error}}
        end

      _ ->
        {:error, :invalid_resolve_params_type}
    end
  end

  defp parse_resolve_assertion(_), do: :not_resolve

  defp extract_field(dict, name) do
    case Map.fetch(dict, {:symbol, name}) do
      {:ok, value} -> {:ok, value}
      :error -> {:error, {:missing_field, name}}
    end
  end

  # Resolve a sturdy ref and send response to observer
  defp resolve_sturdy_ref(gatekeeper, sturdy_ref_value, observer_ref, turn) do
    case SturdyRef.from_preserves(sturdy_ref_value) do
      {:ok, sturdy_ref} ->
        case SturdyRef.materialize(sturdy_ref, gatekeeper.key, gatekeeper.resolver) do
          {:ok, resolved_ref} ->
            # Send accepted response
            send_accepted(observer_ref, resolved_ref, turn)
            {gatekeeper, turn}

          {:error, reason} ->
            # Send rejected response
            Logger.debug("Gatekeeper: resolution failed: #{inspect(reason)}")
            send_rejected(observer_ref, reason, turn)
            {gatekeeper, turn}
        end

      {:error, reason} ->
        Logger.debug("Gatekeeper: invalid sturdy ref format: #{inspect(reason)}")
        send_rejected(observer_ref, {:invalid_format, reason}, turn)
        {gatekeeper, turn}
    end
  end

  # Send accepted response: <accepted {responderSession: #ref}>
  defp send_accepted(observer_ref, resolved_ref, _turn) do
    response =
      {:record,
       {{:symbol, "accepted"},
        [
          {:dictionary,
           %{
             {:symbol, "responderSession"} => {:embedded, resolved_ref}
           }}
        ]}}

    # Send the message via the actor system
    case observer_ref do
      %Ref{} = ref ->
        Actor.send_message(Ref.actor_id(ref), ref, response)

      _ ->
        Logger.warning("Gatekeeper: observer_ref is not a Ref: #{inspect(observer_ref)}")
    end
  end

  # Send rejected response: <rejected {detail: reason}>
  defp send_rejected(observer_ref, reason, _turn) do
    detail =
      case reason do
        :invalid_signature -> {:symbol, "invalid_signature"}
        {:unknown_oid, oid} -> {:record, {{:symbol, "unknown_oid"}, [format_value(oid)]}}
        other -> {:string, inspect(other)}
      end

    response =
      {:record,
       {{:symbol, "rejected"},
        [
          {:dictionary,
           %{
             {:symbol, "detail"} => detail
           }}
        ]}}

    case observer_ref do
      %Ref{} = ref ->
        Actor.send_message(Ref.actor_id(ref), ref, response)

      _ ->
        Logger.warning("Gatekeeper: observer_ref is not a Ref: #{inspect(observer_ref)}")
    end
  end

  # Format a value for error messages
  defp format_value({:symbol, s}), do: {:symbol, s}
  defp format_value({:string, s}), do: {:string, s}
  defp format_value({:integer, n}), do: {:integer, n}
  defp format_value(other), do: {:string, inspect(other)}
end
