defmodule Absynthe.Relay.Membrane do
  @moduledoc """
  Membrane for mapping wire OIDs to local Refs and vice versa.

  A membrane is a bidirectional mapping that manages the translation between
  wire protocol object IDs (OIDs) and local entity references (Refs) across
  a relay connection. Each relay maintains two membranes:

  - **Export membrane**: Maps local Refs to OIDs that we expose to the peer
  - **Import membrane**: Maps remote OIDs to local proxy entities

  ## Wire Symbols

  Each entry in a membrane is a WireSymbol containing:
  - An OID (integer identifier for the wire protocol)
  - A local Ref (the actual entity reference)
  - A reference count (how many times this symbol is mentioned in live assertions)

  A WireSymbol exists only while assertions mentioning its OID exist across
  the relay link. When the refcount hits zero, the symbol is garbage collected.

  ## Attenuation Tracking

  When a previously-exported ref is re-imported with attenuation (caveats),
  we track this separately to ensure attenuation cannot be amplified.

  ## Example Usage

      # Create a new membrane
      membrane = Membrane.new()

      # Export a local ref (gives it an OID for the peer)
      {oid, membrane} = Membrane.export(membrane, ref)

      # Import a remote OID (creates a local proxy)
      {ref, membrane} = Membrane.import(membrane, oid, [])

      # Increment refcount when assertion mentions the OID
      membrane = Membrane.inc_ref(membrane, oid)

      # Decrement refcount when assertion is retracted
      {membrane, :alive} = Membrane.dec_ref(membrane, oid)
      {membrane, :removed} = Membrane.dec_ref(membrane, oid)  # when refcount hits 0
  """

  alias Absynthe.Core.Ref

  @typedoc """
  A wire symbol entry in the membrane.

  Contains the mapping between an OID and a Ref, plus the reference count
  tracking how many live assertions mention this symbol. Also tracks any
  original attenuation to prevent amplification on re-import.
  """
  @type wire_symbol :: %{
          oid: non_neg_integer(),
          ref: Ref.t(),
          refcount: non_neg_integer(),
          original_attenuation: [term()] | nil
        }

  @typedoc """
  The membrane state.

  Contains bidirectional mappings between OIDs and Refs, plus a counter
  for generating new OIDs.
  """
  @type t :: %__MODULE__{
          oid_to_symbol: %{non_neg_integer() => wire_symbol()},
          ref_to_oid: %{Ref.t() => non_neg_integer()},
          next_oid: non_neg_integer()
        }

  defstruct oid_to_symbol: %{},
            ref_to_oid: %{},
            next_oid: 0

  @doc """
  Creates a new empty membrane.

  ## Options

  - `:initial_oid` - Starting OID counter (default: 0)

  ## Examples

      iex> membrane = Membrane.new()
      iex> membrane.next_oid
      0
  """
  @spec new(Keyword.t()) :: t()
  def new(opts \\ []) do
    initial_oid = Keyword.get(opts, :initial_oid, 0)
    %__MODULE__{next_oid: initial_oid}
  end

  @doc """
  Exports a local Ref, assigning it an OID for the peer.

  If the Ref is already exported, returns the existing OID.
  Otherwise, allocates a new OID and creates the wire symbol.

  The refcount starts at 0 - call `inc_ref/2` when the OID is
  actually used in an assertion.

  ## Parameters

  - `membrane` - The membrane state
  - `ref` - The local Ref to export

  ## Returns

  `{oid, updated_membrane}` tuple.

  ## Examples

      iex> membrane = Membrane.new()
      iex> ref = Ref.new(:actor, 1)
      iex> {oid, membrane} = Membrane.export(membrane, ref)
      iex> oid
      0
  """
  @spec export(t(), Ref.t()) :: {non_neg_integer(), t()}
  def export(%__MODULE__{} = membrane, %Ref{} = ref) do
    # Strip attenuation for the key lookup - we export the base ref
    base_ref = Ref.without_attenuation(ref)
    original_attenuation = Ref.attenuation(ref)

    case Map.get(membrane.ref_to_oid, base_ref) do
      nil ->
        # New export - allocate OID and track original attenuation
        oid = membrane.next_oid

        symbol = %{
          oid: oid,
          ref: base_ref,
          refcount: 0,
          original_attenuation: original_attenuation
        }

        membrane = %{
          membrane
          | oid_to_symbol: Map.put(membrane.oid_to_symbol, oid, symbol),
            ref_to_oid: Map.put(membrane.ref_to_oid, base_ref, oid),
            next_oid: oid + 1
        }

        {oid, membrane}

      oid ->
        # Already exported - update original_attenuation if this one is more restrictive
        # (i.e., has attenuation when the original didn't, or has more caveats)
        symbol = Map.get(membrane.oid_to_symbol, oid)

        updated_symbol =
          if should_update_attenuation?(symbol.original_attenuation, original_attenuation) do
            %{symbol | original_attenuation: original_attenuation}
          else
            symbol
          end

        membrane = %{
          membrane
          | oid_to_symbol: Map.put(membrane.oid_to_symbol, oid, updated_symbol)
        }

        {oid, membrane}
    end
  end

  # Determine if we should update the tracked attenuation
  # We update if the new attenuation is more restrictive (longer chain)
  defp should_update_attenuation?(nil, nil), do: false
  defp should_update_attenuation?(nil, _new), do: true
  defp should_update_attenuation?(_old, nil), do: false

  defp should_update_attenuation?(old, new) when is_list(old) and is_list(new) do
    length(new) > length(old)
  end

  @doc """
  Imports a remote OID, returning a local Ref.

  If the OID is already imported, returns the existing Ref (possibly
  with new attenuation applied). Otherwise, the caller must provide
  a factory function to create the proxy entity.

  ## Parameters

  - `membrane` - The membrane state
  - `oid` - The remote OID to import
  - `attenuation` - Caveats to apply to the imported ref
  - `proxy_factory` - Function to create proxy ref if needed: `fn oid -> Ref.t()`

  ## Returns

  `{ref, updated_membrane}` tuple where `ref` has any attenuation applied.

  ## Examples

      iex> membrane = Membrane.new()
      iex> factory = fn oid -> Ref.new(:relay, oid) end
      iex> {ref, membrane} = Membrane.import(membrane, 5, [], factory)
      iex> ref.entity_id
      5
  """
  @spec import(t(), non_neg_integer(), list(), (non_neg_integer() -> Ref.t())) ::
          {Ref.t(), t()}
  def import(%__MODULE__{} = membrane, oid, attenuation, proxy_factory)
      when is_integer(oid) and is_function(proxy_factory, 1) do
    case Map.get(membrane.oid_to_symbol, oid) do
      nil ->
        # New import - create proxy via factory
        base_ref = proxy_factory.(oid)

        symbol = %{
          oid: oid,
          ref: base_ref,
          refcount: 0,
          original_attenuation: nil
        }

        membrane = %{
          membrane
          | oid_to_symbol: Map.put(membrane.oid_to_symbol, oid, symbol),
            ref_to_oid: Map.put(membrane.ref_to_oid, base_ref, oid)
        }

        ref = apply_attenuation(base_ref, attenuation)
        {ref, membrane}

      symbol ->
        # Already imported - return existing ref with attenuation
        ref = apply_attenuation(symbol.ref, attenuation)
        {ref, membrane}
    end
  end

  @doc """
  Re-imports an OID that we previously exported, applying attenuation.

  This is used when a peer sends back a ref we gave them (a "yours" ref).
  We compose the wire attenuation with our original attenuation to prevent
  amplification - the peer cannot remove caveats we originally had.

  ## Parameters

  - `membrane` - The membrane state
  - `oid` - The OID we exported
  - `wire_attenuation` - Attenuation from the wire protocol

  ## Returns

  - `{:ok, ref}` - Successfully re-imported with composed attenuation
  - `{:error, :unknown_oid}` - OID not in our export membrane
  """
  @spec reimport(t(), non_neg_integer(), list()) :: {:ok, Ref.t()} | {:error, :unknown_oid}
  def reimport(%__MODULE__{} = membrane, oid, wire_attenuation) when is_integer(oid) do
    case Map.get(membrane.oid_to_symbol, oid) do
      nil ->
        {:error, :unknown_oid}

      symbol ->
        # Compose wire attenuation with our original attenuation
        # Wire attenuation is applied first (outer), then original (inner)
        # This ensures the peer's restrictions are applied on top of ours
        composed_attenuation =
          compose_attenuation(wire_attenuation, symbol.original_attenuation)

        ref = apply_attenuation(symbol.ref, composed_attenuation)
        {:ok, ref}
    end
  end

  # Compose two attenuation chains
  # The outer attenuation is applied first, then the inner
  defp compose_attenuation(nil, nil), do: nil
  defp compose_attenuation(nil, inner), do: inner
  defp compose_attenuation(outer, nil), do: outer
  defp compose_attenuation([], inner), do: inner
  defp compose_attenuation(outer, []), do: outer

  defp compose_attenuation(outer, inner) when is_list(outer) and is_list(inner) do
    # Outer applied first, so it comes first in the list
    outer ++ inner
  end

  @doc """
  Looks up a Ref by its OID.

  Returns the base ref without any attenuation. For exported refs where
  attenuation should be enforced, use `lookup_by_oid_with_attenuation/2`.

  ## Parameters

  - `membrane` - The membrane state
  - `oid` - The OID to look up

  ## Returns

  `{:ok, ref}` or `:error` if not found.
  """
  @spec lookup_by_oid(t(), non_neg_integer()) :: {:ok, Ref.t()} | :error
  def lookup_by_oid(%__MODULE__{} = membrane, oid) when is_integer(oid) do
    case Map.get(membrane.oid_to_symbol, oid) do
      nil -> :error
      symbol -> {:ok, symbol.ref}
    end
  end

  @doc """
  Looks up a Ref by its OID, including any tracked attenuation.

  This is the secure version of `lookup_by_oid/2` that returns the ref
  with its original attenuation applied. Use this for inbound events
  to ensure attenuation from sturdy refs and other sources is enforced.

  ## Parameters

  - `membrane` - The membrane state
  - `oid` - The OID to look up

  ## Returns

  `{:ok, ref}` or `:error` if not found.
  """
  @spec lookup_by_oid_with_attenuation(t(), non_neg_integer()) :: {:ok, Ref.t()} | :error
  def lookup_by_oid_with_attenuation(%__MODULE__{} = membrane, oid) when is_integer(oid) do
    case Map.get(membrane.oid_to_symbol, oid) do
      nil ->
        :error

      symbol ->
        ref = apply_attenuation(symbol.ref, symbol.original_attenuation)
        {:ok, ref}
    end
  end

  @doc """
  Looks up an OID by its Ref.

  ## Parameters

  - `membrane` - The membrane state
  - `ref` - The Ref to look up

  ## Returns

  `{:ok, oid}` or `:error` if not found.
  """
  @spec lookup_by_ref(t(), Ref.t()) :: {:ok, non_neg_integer()} | :error
  def lookup_by_ref(%__MODULE__{} = membrane, %Ref{} = ref) do
    base_ref = Ref.without_attenuation(ref)

    case Map.get(membrane.ref_to_oid, base_ref) do
      nil -> :error
      oid -> {:ok, oid}
    end
  end

  @doc """
  Increments the reference count for an OID.

  Call this when an assertion is published that mentions this OID.

  ## Parameters

  - `membrane` - The membrane state
  - `oid` - The OID to increment

  ## Returns

  Updated membrane, or unchanged if OID not found.
  """
  @spec inc_ref(t(), non_neg_integer()) :: t()
  def inc_ref(%__MODULE__{} = membrane, oid) when is_integer(oid) do
    case Map.get(membrane.oid_to_symbol, oid) do
      nil ->
        membrane

      symbol ->
        updated_symbol = %{symbol | refcount: symbol.refcount + 1}
        %{membrane | oid_to_symbol: Map.put(membrane.oid_to_symbol, oid, updated_symbol)}
    end
  end

  @doc """
  Decrements the reference count for an OID.

  Call this when an assertion mentioning this OID is retracted.
  When the refcount hits zero, the wire symbol is garbage collected.

  ## Parameters

  - `membrane` - The membrane state
  - `oid` - The OID to decrement

  ## Returns

  `{updated_membrane, status}` where status is:
  - `:alive` - Symbol still has references
  - `:removed` - Symbol was garbage collected
  - `:not_found` - OID not in membrane
  """
  @spec dec_ref(t(), non_neg_integer()) :: {t(), :alive | :removed | :not_found}
  def dec_ref(%__MODULE__{} = membrane, oid) when is_integer(oid) do
    case Map.get(membrane.oid_to_symbol, oid) do
      nil ->
        {membrane, :not_found}

      %{refcount: 1} = symbol ->
        # Last reference - garbage collect
        membrane = %{
          membrane
          | oid_to_symbol: Map.delete(membrane.oid_to_symbol, oid),
            ref_to_oid: Map.delete(membrane.ref_to_oid, symbol.ref)
        }

        {membrane, :removed}

      symbol ->
        # Still has references
        updated_symbol = %{symbol | refcount: symbol.refcount - 1}

        membrane = %{
          membrane
          | oid_to_symbol: Map.put(membrane.oid_to_symbol, oid, updated_symbol)
        }

        {membrane, :alive}
    end
  end

  @doc """
  Returns the current refcount for an OID.

  ## Parameters

  - `membrane` - The membrane state
  - `oid` - The OID to check

  ## Returns

  The refcount, or 0 if not found.
  """
  @spec refcount(t(), non_neg_integer()) :: non_neg_integer()
  def refcount(%__MODULE__{} = membrane, oid) when is_integer(oid) do
    case Map.get(membrane.oid_to_symbol, oid) do
      nil -> 0
      symbol -> symbol.refcount
    end
  end

  @doc """
  Returns the number of symbols in the membrane.
  """
  @spec size(t()) :: non_neg_integer()
  def size(%__MODULE__{} = membrane) do
    map_size(membrane.oid_to_symbol)
  end

  @doc """
  Returns all OIDs currently in the membrane.
  """
  @spec oids(t()) :: [non_neg_integer()]
  def oids(%__MODULE__{} = membrane) do
    Map.keys(membrane.oid_to_symbol)
  end

  # Apply attenuation (caveats) to a ref
  defp apply_attenuation(ref, []), do: ref
  defp apply_attenuation(ref, nil), do: ref

  defp apply_attenuation(ref, attenuation) when is_list(attenuation) do
    # Compose new attenuation with existing
    existing = Ref.attenuation(ref) || []
    Ref.with_attenuation(ref, attenuation ++ existing)
  end
end
