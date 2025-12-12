defmodule Absynthe.Core.SturdyRef do
  @moduledoc """
  Durable capability references for the Syndicated Actor Model.

  A SturdyRef is a cryptographically-signed, durable representation of a capability
  that can be serialized, transmitted across trust boundaries, and validated upon
  receipt. Unlike in-memory Refs, SturdyRefs maintain their validity across process
  restarts and network boundaries.

  ## Structure

  A SturdyRef contains:
  - `oid` - Object identifier chosen by the minting service
  - `sig` - 16-byte HMAC-BLAKE2s signature (truncated from 32 bytes)
  - `caveats` - List of capability restrictions (attenuation chain)

  ## Signature Computation

  The signature uses an iterative HMAC-BLAKE2s-256 construction (macaroon-style):

      f(k, d) = HMAC-BLAKE2s-256(k, d)[0..16)

  Starting with the secret key k:

      sig = f(...f(f(k, e(oid)), e(caveat1)), e(caveat2)...)

  where e(v) is the canonical Preserves binary encoding of v.

  ## Security Properties

  - **Unforgeability**: Without the secret key, valid signatures cannot be created
  - **Monotonic attenuation**: Caveats can only be added, never removed
  - **Non-amplification**: Attenuated refs cannot regain lost authority

  ## Wire Format

  SturdyRefs are encoded as Preserves records:

      <ref {oid: any, sig: bytes, caveats: [Caveat ...]}>

  ## Examples

      # Mint a new SturdyRef
      key = :crypto.strong_rand_bytes(32)
      {:ok, sref} = SturdyRef.mint("my-service", key)

      # Attenuate to restrict capability
      pattern = {:record, {{:symbol, "Person"}, [{:symbol, "$"}]}}
      caveat = Absynthe.Core.Caveat.rewrite(pattern, {:ref, 0})
      {:ok, attenuated} = SturdyRef.attenuate(sref, caveat, key)

      # Validate on receipt
      :ok = SturdyRef.validate(attenuated, key)

      # Convert to Preserves for wire transmission
      preserves_value = SturdyRef.to_preserves(attenuated)

  ## References

  - [Synit Gatekeeper Protocol](https://synit.org/book/protocols/syndicate/gatekeeper.html)
  - [Macaroons](https://ai.google/research/pubs/pub41892)
  """

  alias Absynthe.Core.Caveat
  alias Absynthe.Preserves.Encoder.Binary, as: BinaryEncoder
  alias Absynthe.Preserves.Value

  @typedoc """
  A durable, cryptographically-signed capability reference.
  """
  @type t :: %__MODULE__{
          oid: term(),
          sig: binary(),
          caveats: [Caveat.t()]
        }

  @enforce_keys [:oid, :sig]
  defstruct [:oid, :sig, caveats: []]

  @doc """
  Creates a new SturdyRef by minting a signature for the given object identifier.

  The oid can be any Preserves-encodable value (typically a string, symbol, or
  record that identifies the target service/entity).

  ## Parameters

  - `oid` - Object identifier (any Preserves value)
  - `key` - Secret key (32 bytes recommended)

  ## Returns

  - `{:ok, sturdy_ref}` - Successfully minted SturdyRef
  - `{:error, reason}` - Failed to encode oid

  ## Examples

      iex> key = :crypto.strong_rand_bytes(32)
      iex> {:ok, sref} = SturdyRef.mint({:symbol, "my-service"}, key)
      iex> sref.oid
      {:symbol, "my-service"}
      iex> byte_size(sref.sig)
      16
  """
  @spec mint(term(), binary()) :: {:ok, t()} | {:error, term()}
  def mint(oid, key) when is_binary(key) do
    case encode_for_signing(oid) do
      {:ok, encoded_oid} ->
        sig = compute_sig(key, encoded_oid)
        {:ok, %__MODULE__{oid: oid, sig: sig, caveats: []}}

      {:error, _} = error ->
        error
    end
  end

  @doc """
  Creates a new SturdyRef, raising on failure.

  See `mint/2` for details.

  ## Raises

  - `ArgumentError` if the oid cannot be encoded
  """
  @spec mint!(term(), binary()) :: t()
  def mint!(oid, key) do
    case mint(oid, key) do
      {:ok, sref} -> sref
      {:error, reason} -> raise ArgumentError, "Failed to mint SturdyRef: #{inspect(reason)}"
    end
  end

  @doc """
  Attenuates a SturdyRef by adding a caveat.

  The caveat is added to the chain and the signature is recomputed. This
  requires the secret key because the new signature must chain from the
  previous one.

  ## Parameters

  - `sturdy_ref` - The SturdyRef to attenuate
  - `caveat` - The caveat to add
  - `key` - The secret key (must be the same key used to mint)

  ## Returns

  - `{:ok, attenuated_ref}` - Successfully attenuated
  - `{:error, reason}` - Invalid caveat or encoding failure

  ## Examples

      iex> {:ok, sref} = SturdyRef.mint({:symbol, "svc"}, key)
      iex> caveat = Caveat.passthrough()
      iex> {:ok, attenuated} = SturdyRef.attenuate(sref, caveat, key)
      iex> length(attenuated.caveats)
      1
  """
  @spec attenuate(t(), Caveat.t(), binary()) :: {:ok, t()} | {:error, term()}
  def attenuate(%__MODULE__{} = sref, caveat, key) do
    with :ok <- Caveat.validate(caveat),
         {:ok, _encoded_caveat} <- encode_caveat_for_signing(caveat) do
      # Recompute the entire signature chain from scratch
      new_caveats = sref.caveats ++ [caveat]
      new_sig = compute_full_sig(sref.oid, new_caveats, key)
      {:ok, %__MODULE__{sref | caveats: new_caveats, sig: new_sig}}
    end
  end

  @doc """
  Attenuates a SturdyRef, raising on failure.

  See `attenuate/3` for details.
  """
  @spec attenuate!(t(), Caveat.t(), binary()) :: t()
  def attenuate!(%__MODULE__{} = sref, caveat, key) do
    case attenuate(sref, caveat, key) do
      {:ok, attenuated} -> attenuated
      {:error, reason} -> raise ArgumentError, "Failed to attenuate: #{inspect(reason)}"
    end
  end

  @doc """
  Validates a SturdyRef's signature.

  Recomputes the expected signature from the oid and caveats using the
  provided key, then compares it to the stored signature.

  ## Parameters

  - `sturdy_ref` - The SturdyRef to validate
  - `key` - The secret key

  ## Returns

  - `:ok` - Signature is valid
  - `{:error, :invalid_signature}` - Signature mismatch
  - `{:error, reason}` - Encoding failure

  ## Examples

      iex> {:ok, sref} = SturdyRef.mint({:symbol, "svc"}, key)
      iex> SturdyRef.validate(sref, key)
      :ok

      iex> tampered = %{sref | caveats: [Caveat.reject()]}
      iex> SturdyRef.validate(tampered, key)
      {:error, :invalid_signature}
  """
  @spec validate(t(), binary()) :: :ok | {:error, term()}
  def validate(%__MODULE__{} = sref, key) do
    case compute_full_sig_result(sref.oid, sref.caveats, key) do
      {:ok, expected_sig} ->
        if secure_compare(expected_sig, sref.sig) do
          :ok
        else
          {:error, :invalid_signature}
        end

      {:error, _} = error ->
        error
    end
  end

  @doc """
  Checks if a SturdyRef is valid without raising.

  ## Examples

      iex> SturdyRef.valid?(sref, key)
      true
  """
  @spec valid?(t(), binary()) :: boolean()
  def valid?(%__MODULE__{} = sref, key) do
    validate(sref, key) == :ok
  end

  @doc """
  Validates a SturdyRef and materializes it to a live Ref.

  This is the primary entry point for resolving SturdyRefs received over
  the wire. The `resolver` function maps the oid to an actor/entity pair.

  ## Parameters

  - `sturdy_ref` - The SturdyRef to materialize
  - `key` - The secret key for validation
  - `resolver` - Function `(oid) -> {:ok, {actor_id, entity_id}} | {:error, reason}`

  ## Returns

  - `{:ok, ref}` - Valid SturdyRef resolved to a live Ref with attenuation
  - `{:error, reason}` - Validation or resolution failed

  ## Examples

      resolver = fn {:symbol, "my-entity"} -> {:ok, {:my_actor, :my_entity}} end
      {:ok, ref} = SturdyRef.materialize(sref, key, resolver)
  """
  @spec materialize(t(), binary(), (term() -> {:ok, {term(), term()}} | {:error, term()})) ::
          {:ok, Absynthe.Core.Ref.t()} | {:error, term()}
  def materialize(%__MODULE__{} = sref, key, resolver) when is_function(resolver, 1) do
    with :ok <- validate(sref, key),
         {:ok, {actor_id, entity_id}} <- resolver.(sref.oid) do
      # Build attenuation from caveats.
      # SturdyRef stores caveats in signature-chain order (oldest first: [c1, c2, c3])
      # but Ref.attenuate uses outer-first order (newest first: [c3, c2, c1]).
      # Reverse to match Ref.attenuate semantics so outer caveats apply first.
      attenuation = if sref.caveats == [], do: nil, else: Enum.reverse(sref.caveats)
      ref = Absynthe.Core.Ref.new(actor_id, entity_id, attenuation)
      {:ok, ref}
    end
  end

  # ============================================================================
  # Preserves Encoding/Decoding
  # ============================================================================

  @doc """
  Converts a SturdyRef to its Preserves value representation.

  The wire format is: `<ref {oid: any, sig: bytes, caveats: [Caveat ...]}>`

  ## Examples

      iex> preserves = SturdyRef.to_preserves(sref)
      {:record, {{:symbol, "ref"}, [{:dictionary, %{...}}]}}
  """
  @spec to_preserves(t()) :: Value.t()
  def to_preserves(%__MODULE__{} = sref) do
    caveats_value =
      if sref.caveats == [] do
        {:sequence, []}
      else
        {:sequence, Enum.map(sref.caveats, &caveat_to_preserves/1)}
      end

    params =
      {:dictionary,
       %{
         {:symbol, "oid"} => to_preserves_value(sref.oid),
         {:symbol, "sig"} => {:binary, sref.sig},
         {:symbol, "caveats"} => caveats_value
       }}

    {:record, {{:symbol, "ref"}, [params]}}
  end

  @doc """
  Parses a SturdyRef from its Preserves value representation.

  ## Returns

  - `{:ok, sturdy_ref}` - Successfully parsed
  - `{:error, reason}` - Invalid format

  ## Examples

      iex> {:ok, sref} = SturdyRef.from_preserves(preserves_value)
  """
  @spec from_preserves(Value.t()) :: {:ok, t()} | {:error, term()}
  def from_preserves({:record, {{:symbol, "ref"}, [params]}}) do
    with {:ok, oid} <- extract_field(params, "oid"),
         {:ok, {:binary, sig}} <- extract_field(params, "sig"),
         {:ok, caveats_value} <- extract_field(params, "caveats"),
         {:ok, caveats} <- parse_caveats(caveats_value) do
      {:ok, %__MODULE__{oid: from_preserves_value(oid), sig: sig, caveats: caveats}}
    end
  end

  def from_preserves(_), do: {:error, :invalid_sturdy_ref_format}

  # ============================================================================
  # Hex Encoding (for text format / debugging)
  # ============================================================================

  @doc """
  Encodes a SturdyRef signature as a hex string.

  Useful for debugging and text-based wire formats.

  ## Examples

      iex> SturdyRef.sig_to_hex(sref)
      "a1b2c3d4..."
  """
  @spec sig_to_hex(t()) :: String.t()
  def sig_to_hex(%__MODULE__{sig: sig}) do
    Base.encode16(sig, case: :lower)
  end

  # ============================================================================
  # Private: HMAC-BLAKE2s-256 Signature Computation
  # ============================================================================

  # Compute f(k, d) = HMAC-BLAKE2s-256(k, d)[0..16)
  defp hmac_blake2s_truncated(key, data) do
    # Use HMAC with BLAKE2s-256
    full_mac = :crypto.mac(:hmac, :blake2s, key, data)
    # Truncate to first 16 bytes per spec
    binary_part(full_mac, 0, 16)
  end

  # Compute initial signature from key and oid
  defp compute_sig(key, encoded_oid) do
    hmac_blake2s_truncated(key, encoded_oid)
  end

  # Compute full signature chain: f(...f(f(k, e(oid)), e(c1)), e(c2)...)
  defp compute_full_sig(oid, caveats, key) do
    {:ok, encoded_oid} = encode_for_signing(oid)
    initial_sig = compute_sig(key, encoded_oid)

    Enum.reduce(caveats, initial_sig, fn caveat, current_sig ->
      {:ok, encoded_caveat} = encode_caveat_for_signing(caveat)
      hmac_blake2s_truncated(current_sig, encoded_caveat)
    end)
  end

  # Same as compute_full_sig but returns {:ok, sig} or {:error, reason}
  defp compute_full_sig_result(oid, caveats, key) do
    with {:ok, encoded_oid} <- encode_for_signing(oid) do
      initial_sig = compute_sig(key, encoded_oid)

      result =
        Enum.reduce_while(caveats, {:ok, initial_sig}, fn caveat, {:ok, current_sig} ->
          case encode_caveat_for_signing(caveat) do
            {:ok, encoded_caveat} ->
              new_sig = hmac_blake2s_truncated(current_sig, encoded_caveat)
              {:cont, {:ok, new_sig}}

            {:error, _} = error ->
              {:halt, error}
          end
        end)

      result
    end
  end

  # Encode a value for signing (canonical Preserves binary)
  defp encode_for_signing(value) do
    preserves_value = to_preserves_value(value)
    BinaryEncoder.encode(preserves_value)
  end

  # Encode a caveat for signing
  defp encode_caveat_for_signing(caveat) do
    preserves_value = caveat_to_preserves(caveat)
    BinaryEncoder.encode(preserves_value)
  end

  # ============================================================================
  # Private: Preserves Conversion Helpers
  # ============================================================================

  # Convert Elixir values to Preserves values for encoding
  # If already a Preserves value (tuple with atom tag), pass through
  defp to_preserves_value({:boolean, _} = v), do: v
  defp to_preserves_value({:integer, _} = v), do: v
  defp to_preserves_value({:double, _} = v), do: v
  defp to_preserves_value({:string, _} = v), do: v
  defp to_preserves_value({:binary, _} = v), do: v
  defp to_preserves_value({:symbol, _} = v), do: v
  defp to_preserves_value({:record, _} = v), do: v
  defp to_preserves_value({:sequence, _} = v), do: v
  defp to_preserves_value({:set, _} = v), do: v
  defp to_preserves_value({:dictionary, _} = v), do: v
  defp to_preserves_value({:embedded, _} = v), do: v
  defp to_preserves_value({:annotated, _, _} = v), do: v
  # Convert native Elixir types
  defp to_preserves_value(v) when is_binary(v), do: {:string, v}
  defp to_preserves_value(v) when is_integer(v), do: {:integer, v}
  defp to_preserves_value(v) when is_float(v), do: {:double, v}
  defp to_preserves_value(true), do: {:boolean, true}
  defp to_preserves_value(false), do: {:boolean, false}
  defp to_preserves_value(v) when is_atom(v), do: {:symbol, Atom.to_string(v)}

  defp to_preserves_value(v) when is_list(v) do
    {:sequence, Enum.map(v, &to_preserves_value/1)}
  end

  defp to_preserves_value(v) when is_map(v) and not is_struct(v) do
    entries =
      Map.new(v, fn {k, val} ->
        {to_preserves_value(k), to_preserves_value(val)}
      end)

    {:dictionary, entries}
  end

  # Convert Preserves values back to native types where applicable
  defp from_preserves_value({:string, v}), do: {:string, v}
  defp from_preserves_value({:symbol, v}), do: {:symbol, v}
  defp from_preserves_value({:integer, v}), do: {:integer, v}
  defp from_preserves_value({:boolean, v}), do: {:boolean, v}
  defp from_preserves_value({:double, v}), do: {:double, v}
  defp from_preserves_value({:binary, v}), do: {:binary, v}
  defp from_preserves_value({:record, _} = v), do: v
  defp from_preserves_value({:sequence, _} = v), do: v
  defp from_preserves_value({:set, _} = v), do: v
  defp from_preserves_value({:dictionary, _} = v), do: v
  defp from_preserves_value({:embedded, _} = v), do: v
  defp from_preserves_value({:annotated, _, _} = v), do: v
  defp from_preserves_value(other), do: other

  # Convert a caveat to its Preserves representation
  defp caveat_to_preserves(:reject) do
    {:record, {{:symbol, "reject"}, []}}
  end

  defp caveat_to_preserves({:rewrite, pattern, template}) do
    {:record,
     {{:symbol, "rewrite"},
      [
        to_preserves_value(pattern),
        template_to_preserves(template)
      ]}}
  end

  defp caveat_to_preserves({:alts, alternatives}) do
    {:record,
     {{:symbol, "or"},
      [
        {:sequence, Enum.map(alternatives, &caveat_to_preserves/1)}
      ]}}
  end

  # Convert a template to its Preserves representation
  defp template_to_preserves({:lit, value}) do
    {:record, {{:symbol, "lit"}, [to_preserves_value(value)]}}
  end

  defp template_to_preserves({:ref, index}) do
    {:record, {{:symbol, "ref"}, [{:integer, index}]}}
  end

  defp template_to_preserves({:record, label_template, field_templates}) do
    {:record,
     {{:symbol, "rec"},
      [
        template_to_preserves(label_template),
        {:sequence, Enum.map(field_templates, &template_to_preserves/1)}
      ]}}
  end

  defp template_to_preserves({:sequence, templates}) do
    {:record, {{:symbol, "seq"}, [{:sequence, Enum.map(templates, &template_to_preserves/1)}]}}
  end

  defp template_to_preserves({:set, templates}) do
    {:record, {{:symbol, "set"}, [{:sequence, Enum.map(templates, &template_to_preserves/1)}]}}
  end

  defp template_to_preserves({:dictionary, pairs}) do
    pairs_preserves =
      Enum.map(pairs, fn {k, v} ->
        {:sequence, [template_to_preserves(k), template_to_preserves(v)]}
      end)

    {:record, {{:symbol, "dict"}, [{:sequence, pairs_preserves}]}}
  end

  # Extract a field from a dictionary
  defp extract_field({:dictionary, dict}, field_name) do
    key = {:symbol, field_name}

    case Map.fetch(dict, key) do
      {:ok, value} -> {:ok, value}
      :error -> {:error, {:missing_field, field_name}}
    end
  end

  defp extract_field(_, _), do: {:error, :not_a_dictionary}

  # Parse caveats from a sequence
  defp parse_caveats({:sequence, items}) do
    results =
      Enum.reduce_while(items, {:ok, []}, fn item, {:ok, acc} ->
        case parse_caveat(item) do
          {:ok, caveat} -> {:cont, {:ok, [caveat | acc]}}
          {:error, _} = error -> {:halt, error}
        end
      end)

    case results do
      {:ok, caveats} -> {:ok, Enum.reverse(caveats)}
      error -> error
    end
  end

  defp parse_caveats(_), do: {:error, :invalid_caveats_format}

  # Parse a single caveat from Preserves
  defp parse_caveat({:record, {{:symbol, "reject"}, []}}) do
    {:ok, :reject}
  end

  defp parse_caveat({:record, {{:symbol, "rewrite"}, [pattern, template]}}) do
    with {:ok, parsed_template} <- parse_template(template) do
      {:ok, {:rewrite, from_preserves_value(pattern), parsed_template}}
    end
  end

  defp parse_caveat({:record, {{:symbol, "or"}, [{:sequence, alts}]}}) do
    results =
      Enum.reduce_while(alts, {:ok, []}, fn alt, {:ok, acc} ->
        case parse_caveat(alt) do
          {:ok, caveat} -> {:cont, {:ok, [caveat | acc]}}
          {:error, _} = error -> {:halt, error}
        end
      end)

    case results do
      {:ok, alternatives} -> {:ok, {:alts, Enum.reverse(alternatives)}}
      error -> error
    end
  end

  defp parse_caveat(_), do: {:error, :invalid_caveat_format}

  # Parse a template from Preserves
  defp parse_template({:record, {{:symbol, "lit"}, [value]}}) do
    {:ok, {:lit, from_preserves_value(value)}}
  end

  defp parse_template({:record, {{:symbol, "ref"}, [{:integer, index}]}}) do
    {:ok, {:ref, index}}
  end

  defp parse_template(
         {:record, {{:symbol, "rec"}, [label_template, {:sequence, field_templates}]}}
       ) do
    with {:ok, label} <- parse_template(label_template),
         {:ok, fields} <- parse_templates(field_templates) do
      {:ok, {:record, label, fields}}
    end
  end

  defp parse_template({:record, {{:symbol, "seq"}, [{:sequence, templates}]}}) do
    with {:ok, parsed} <- parse_templates(templates) do
      {:ok, {:sequence, parsed}}
    end
  end

  defp parse_template({:record, {{:symbol, "set"}, [{:sequence, templates}]}}) do
    with {:ok, parsed} <- parse_templates(templates) do
      {:ok, {:set, parsed}}
    end
  end

  defp parse_template({:record, {{:symbol, "dict"}, [{:sequence, pairs}]}}) do
    results =
      Enum.reduce_while(pairs, {:ok, []}, fn
        {:sequence, [k, v]}, {:ok, acc} ->
          with {:ok, key_template} <- parse_template(k),
               {:ok, val_template} <- parse_template(v) do
            {:cont, {:ok, [{key_template, val_template} | acc]}}
          else
            error -> {:halt, error}
          end

        _malformed_pair, {:ok, _acc} ->
          {:halt, {:error, :invalid_template_format}}
      end)

    case results do
      {:ok, parsed_pairs} -> {:ok, {:dictionary, Enum.reverse(parsed_pairs)}}
      error -> error
    end
  end

  defp parse_template(_), do: {:error, :invalid_template_format}

  defp parse_templates(templates) do
    results =
      Enum.reduce_while(templates, {:ok, []}, fn t, {:ok, acc} ->
        case parse_template(t) do
          {:ok, parsed} -> {:cont, {:ok, [parsed | acc]}}
          {:error, _} = error -> {:halt, error}
        end
      end)

    case results do
      {:ok, parsed} -> {:ok, Enum.reverse(parsed)}
      error -> error
    end
  end

  # Constant-time comparison to prevent timing attacks
  defp secure_compare(a, b) when byte_size(a) == byte_size(b) do
    :crypto.hash_equals(a, b)
  end

  defp secure_compare(_, _), do: false
end
