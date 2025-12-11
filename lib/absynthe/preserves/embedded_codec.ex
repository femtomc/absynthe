defmodule Absynthe.Preserves.EmbeddedCodec do
  @moduledoc """
  Behaviour for encoding and decoding embedded values in Preserves.

  Embedded values (`{:embedded, payload}`) allow domain-specific data to be
  included in Preserves values. This behaviour defines how such payloads are
  encoded to and decoded from Preserves Values for wire transmission.

  ## Default Behaviour

  The default implementation (`Absynthe.Preserves.EmbeddedCodec.Strict`) requires
  that all embedded payloads are already valid Preserves Values. This ensures
  canonical form and interoperability with other Preserves implementations.

  ## Custom Codecs

  For domain-specific needs (e.g., Syndicate actor references, capabilities),
  implement this behaviour to define custom mappings:

      defmodule MyApp.RefCodec do
        @behaviour Absynthe.Preserves.EmbeddedCodec

        alias Absynthe.Preserves.Value

        @impl true
        def encode(%MyApp.Ref{id: id, type: type}) do
          {:ok, Value.record(Value.symbol("ref"), [Value.string(id), Value.symbol(type)])}
        end

        def encode(other) do
          {:error, {:unsupported_embedded, other}}
        end

        @impl true
        def decode({:record, {{:symbol, "ref"}, [{:string, id}, {:symbol, type}]}}) do
          {:ok, %MyApp.Ref{id: id, type: String.to_existing_atom(type)}}
        end

        def decode(other) do
          {:error, {:invalid_ref_format, other}}
        end
      end

  ## Configuration

  Configure the codec module globally via application config:

      Application.put_env(:absynthe, :embedded_codec, MyApp.RefCodec)

  Or in your config files:

      config :absynthe, embedded_codec: MyApp.RefCodec

  ## Syndicate Interop

  For Syndicate protocol compatibility, embedded values typically represent:
  - Actor references (capabilities)
  - Assertion handles
  - Dataspace references

  Document your codec's wire format to ensure other implementations can interoperate.
  """

  alias Absynthe.Preserves.Value

  @doc """
  Encodes an embedded payload to a Preserves Value for wire transmission.

  Returns `{:ok, value}` where `value` is a valid Preserves Value,
  or `{:error, reason}` if the payload cannot be encoded.
  """
  @callback encode(payload :: term()) :: {:ok, Value.t()} | {:error, term()}

  @doc """
  Decodes a Preserves Value back to the original embedded payload.

  Returns `{:ok, payload}` or `{:error, reason}` if the value doesn't
  match the expected format.
  """
  @callback decode(value :: Value.t()) :: {:ok, term()} | {:error, term()}

  @doc """
  Returns the configured embedded codec module.

  Reads from application config, defaulting to the strict codec.
  """
  @spec get_codec() :: module()
  def get_codec do
    Application.get_env(:absynthe, :embedded_codec, __MODULE__.Strict)
  end
end

defmodule Absynthe.Preserves.EmbeddedCodec.Strict do
  @moduledoc """
  Default strict embedded codec that only accepts Preserves Values.

  This codec requires all embedded payloads to already be valid Preserves Values,
  ensuring canonical form and maximum interoperability.

  Any term that is not a valid Preserves Value will cause an encoding error.
  """

  @behaviour Absynthe.Preserves.EmbeddedCodec

  @impl true
  def encode(payload) do
    if preserves_value?(payload) do
      {:ok, payload}
    else
      {:error,
       {:invalid_embedded,
        "Embedded values must be Preserves Values. Got: #{inspect(payload)}. " <>
          "Use a custom EmbeddedCodec for domain-specific types."}}
    end
  end

  @impl true
  def decode(value) do
    # In strict mode, the decoded value IS the payload
    {:ok, value}
  end

  # Check if a term is a valid Preserves Value
  defp preserves_value?({:boolean, v}) when is_boolean(v), do: true
  defp preserves_value?({:integer, v}) when is_integer(v), do: true
  defp preserves_value?({:double, v}) when is_float(v), do: true
  defp preserves_value?({:double, v}) when v in [:infinity, :neg_infinity, :nan], do: true
  defp preserves_value?({:string, v}) when is_binary(v), do: true
  defp preserves_value?({:binary, v}) when is_binary(v), do: true
  defp preserves_value?({:symbol, v}) when is_binary(v), do: true

  defp preserves_value?({:record, {label, fields}}) when is_list(fields) do
    preserves_value?(label) and Enum.all?(fields, &preserves_value?/1)
  end

  defp preserves_value?({:sequence, items}) when is_list(items) do
    Enum.all?(items, &preserves_value?/1)
  end

  defp preserves_value?({:set, items}) when is_struct(items, MapSet) do
    Enum.all?(items, &preserves_value?/1)
  end

  defp preserves_value?({:dictionary, items}) when is_map(items) do
    Enum.all?(items, fn {k, v} -> preserves_value?(k) and preserves_value?(v) end)
  end

  defp preserves_value?({:embedded, payload}), do: preserves_value?(payload)

  defp preserves_value?({:annotated, ann, val}) do
    preserves_value?(ann) and preserves_value?(val)
  end

  defp preserves_value?(_), do: false
end
