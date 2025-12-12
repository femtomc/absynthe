defmodule Absynthe.Relay.Packet do
  @moduledoc """
  Wire protocol packet types for the Syndicate relay protocol.

  This module defines the packet types used for communication between
  relay peers over the network. Packets are encoded/decoded as Preserves
  values using the binary format.

  ## Packet Types

  The protocol defines four main packet variants:

  - **Turn** - The primary packet type containing a sequence of turn events
  - **Error** - Indicates the sender has terminated
  - **Extension** - Future-compatible record packets (ignored if unknown)
  - **Nop** - No-operation, used for keepalives (encoded as `false`)

  ## Turn Events

  Turn packets contain a sequence of `TurnEvent` structures, each targeting
  an entity via its OID (Object ID):

      TurnEvent = [@oid Oid @event Event]

  Events within a turn:

  - `<A @assertion Assertion @handle Handle>` - Assert
  - `<R @handle Handle>` - Retract
  - `<M @body Assertion>` - Message
  - `<S @peer #t>` - Sync (peer is always true in wire format)

  ## Wire References

  References embedded in packets use the WireRef format:

      WireRef = @mine [0 @oid Oid] / @yours [1 @oid Oid @attenuation Caveat ...]

  - `mine` variant: Reference managed by the sender (exported from sender)
  - `yours` variant: Reference managed by the receiver (re-importing receiver's export)

  ## Example

      # A turn with one assertion
      turn = %Turn{events: [
        %TurnEvent{
          oid: 0,
          event: %Assert{
            assertion: {:string, "hello"},
            handle: 42
          }
        }
      ]}
  """

  alias Absynthe.Preserves.Value

  # Packet Types

  defmodule Turn do
    @moduledoc """
    A turn packet containing a sequence of events to deliver.

    This is the primary packet type in the Syndicate protocol. Each turn
    represents an atomic batch of events to be processed together.
    """
    defstruct events: []

    @type t :: %__MODULE__{
            events: [Absynthe.Relay.Packet.TurnEvent.t()]
          }
  end

  defmodule Error do
    @moduledoc """
    Error packet indicating the sender has terminated.

    Receipt of an error packet means the sender has crashed and will not
    respond further. The session should be closed.
    """
    defstruct [:message, :detail]

    @type t :: %__MODULE__{
            message: String.t(),
            detail: Value.t()
          }
  end

  defmodule Extension do
    @moduledoc """
    Extension packet for future protocol extensions.

    Peers must ignore extensions they don't understand. This allows the
    protocol to evolve without breaking backwards compatibility.
    """
    defstruct [:label, :fields]

    @type t :: %__MODULE__{
            label: Value.t(),
            fields: [Value.t()]
          }
  end

  # Turn Event Types

  defmodule TurnEvent do
    @moduledoc """
    A single event within a turn, targeting a specific OID.
    """
    defstruct [:oid, :event]

    @type t :: %__MODULE__{
            oid: non_neg_integer(),
            event: Absynthe.Relay.Packet.event()
          }
  end

  defmodule Assert do
    @moduledoc """
    Assert event - publish an assertion with a handle.
    """
    defstruct [:assertion, :handle]

    @type t :: %__MODULE__{
            assertion: Value.t(),
            handle: non_neg_integer()
          }
  end

  defmodule Retract do
    @moduledoc """
    Retract event - remove an assertion by handle.
    """
    defstruct [:handle]

    @type t :: %__MODULE__{
            handle: non_neg_integer()
          }
  end

  defmodule Message do
    @moduledoc """
    Message event - deliver a one-shot message.
    """
    defstruct [:body]

    @type t :: %__MODULE__{
            body: Value.t()
          }
  end

  defmodule Sync do
    @moduledoc """
    Sync event - synchronization barrier.

    The peer field in wire format is always `#t` (true). When processed
    locally, we replace it with the actual peer ref to notify.
    """
    defstruct peer: true

    @type t :: %__MODULE__{
            peer: true | Absynthe.Core.Ref.t()
          }
  end

  # Wire Reference Types

  defmodule WireRef do
    @moduledoc """
    Wire protocol reference representation.

    References on the wire use a two-variant encoding:
    - `mine` (tag 0): Reference exported by sender, OID managed by sender
    - `yours` (tag 1): Re-importing receiver's export, may include attenuation
    """
    defstruct [:variant, :oid, :attenuation]

    @type variant :: :mine | :yours

    @type t :: %__MODULE__{
            variant: variant(),
            oid: non_neg_integer(),
            attenuation: [Value.t()]
          }

    @doc """
    Creates a `mine` wire reference.
    """
    @spec mine(non_neg_integer()) :: t()
    def mine(oid) when is_integer(oid) do
      %__MODULE__{variant: :mine, oid: oid, attenuation: []}
    end

    @doc """
    Creates a `yours` wire reference with optional attenuation.
    """
    @spec yours(non_neg_integer(), [Value.t()]) :: t()
    def yours(oid, attenuation \\ []) when is_integer(oid) do
      %__MODULE__{variant: :yours, oid: oid, attenuation: attenuation}
    end
  end

  # Type definitions

  @type packet :: Turn.t() | Error.t() | Extension.t() | :nop
  @type event :: Assert.t() | Retract.t() | Message.t() | Sync.t()

  # Encoding

  @doc """
  Encodes a packet to Preserves value format.

  ## Examples

      iex> turn = %Turn{events: []}
      iex> {:ok, value} = Packet.encode(turn)
      iex> value
      {:sequence, []}
  """
  @spec encode(packet()) :: {:ok, Value.t()} | {:error, term()}
  def encode(%Turn{events: events}) do
    case encode_events(events) do
      {:ok, encoded_events} -> {:ok, {:sequence, encoded_events}}
      error -> error
    end
  end

  def encode(%Error{message: message, detail: detail}) do
    {:ok,
     {:record, {{:symbol, "error"}, [{:string, message || ""}, detail || {:boolean, false}]}}}
  end

  def encode(%Extension{label: label, fields: fields}) do
    {:ok, {:record, {label, fields}}}
  end

  def encode(:nop) do
    {:ok, {:boolean, false}}
  end

  defp encode_events(events) do
    results =
      Enum.reduce_while(events, {:ok, []}, fn event, {:ok, acc} ->
        case encode_turn_event(event) do
          {:ok, encoded} -> {:cont, {:ok, [encoded | acc]}}
          error -> {:halt, error}
        end
      end)

    case results do
      {:ok, encoded} -> {:ok, Enum.reverse(encoded)}
      error -> error
    end
  end

  defp encode_turn_event(%TurnEvent{oid: oid, event: event}) do
    case encode_event(event) do
      {:ok, encoded_event} ->
        {:ok, {:sequence, [{:integer, oid}, encoded_event]}}

      error ->
        error
    end
  end

  defp encode_event(%Assert{assertion: assertion, handle: handle}) do
    {:ok, {:record, {{:symbol, "A"}, [assertion, {:integer, handle}]}}}
  end

  defp encode_event(%Retract{handle: handle}) do
    {:ok, {:record, {{:symbol, "R"}, [{:integer, handle}]}}}
  end

  defp encode_event(%Message{body: body}) do
    {:ok, {:record, {{:symbol, "M"}, [body]}}}
  end

  defp encode_event(%Sync{}) do
    {:ok, {:record, {{:symbol, "S"}, [{:boolean, true}]}}}
  end

  @doc """
  Encodes a WireRef to Preserves value format.

  ## Examples

      iex> wire_ref = WireRef.mine(5)
      iex> {:ok, value} = Packet.encode_wire_ref(wire_ref)
      iex> value
      {:sequence, [{:integer, 0}, {:integer, 5}]}
  """
  @spec encode_wire_ref(WireRef.t()) :: {:ok, Value.t()}
  def encode_wire_ref(%WireRef{variant: :mine, oid: oid}) do
    {:ok, {:sequence, [{:integer, 0}, {:integer, oid}]}}
  end

  def encode_wire_ref(%WireRef{variant: :yours, oid: oid, attenuation: attenuation}) do
    elements = [{:integer, 1}, {:integer, oid} | attenuation]
    {:ok, {:sequence, elements}}
  end

  # Decoding

  @doc """
  Decodes a Preserves value to a packet.

  ## Examples

      iex> value = {:sequence, []}
      iex> {:ok, packet} = Packet.decode(value)
      iex> packet
      %Turn{events: []}
  """
  @spec decode(Value.t()) :: {:ok, packet()} | {:error, term()}
  def decode({:boolean, false}), do: {:ok, :nop}

  def decode({:sequence, events}) do
    case decode_events(events) do
      {:ok, decoded_events} -> {:ok, %Turn{events: decoded_events}}
      error -> error
    end
  end

  def decode({:record, {{:symbol, "error"}, [message, detail]}}) do
    msg =
      case message do
        {:string, s} -> s
        _ -> inspect(message)
      end

    {:ok, %Error{message: msg, detail: detail}}
  end

  def decode({:record, {label, fields}}) do
    # Unknown record = extension
    {:ok, %Extension{label: label, fields: fields}}
  end

  def decode(other) do
    {:error, {:invalid_packet, other}}
  end

  defp decode_events(events) do
    results =
      Enum.reduce_while(events, {:ok, []}, fn event, {:ok, acc} ->
        case decode_turn_event(event) do
          {:ok, decoded} -> {:cont, {:ok, [decoded | acc]}}
          error -> {:halt, error}
        end
      end)

    case results do
      {:ok, decoded} -> {:ok, Enum.reverse(decoded)}
      error -> error
    end
  end

  defp decode_turn_event({:sequence, [{:integer, oid}, event_value]}) do
    case decode_event(event_value) do
      {:ok, event} -> {:ok, %TurnEvent{oid: oid, event: event}}
      error -> error
    end
  end

  defp decode_turn_event(other) do
    {:error, {:invalid_turn_event, other}}
  end

  defp decode_event({:record, {{:symbol, "A"}, [assertion, {:integer, handle}]}}) do
    {:ok, %Assert{assertion: assertion, handle: handle}}
  end

  defp decode_event({:record, {{:symbol, "R"}, [{:integer, handle}]}}) do
    {:ok, %Retract{handle: handle}}
  end

  defp decode_event({:record, {{:symbol, "M"}, [body]}}) do
    {:ok, %Message{body: body}}
  end

  defp decode_event({:record, {{:symbol, "S"}, [{:boolean, true}]}}) do
    {:ok, %Sync{peer: true}}
  end

  defp decode_event({:record, {{:symbol, "S"}, [_peer]}}) do
    # Accept any peer value for compatibility
    {:ok, %Sync{peer: true}}
  end

  defp decode_event(other) do
    {:error, {:invalid_event, other}}
  end

  @doc """
  Decodes a WireRef from Preserves value format.

  ## Examples

      iex> value = {:sequence, [{:integer, 0}, {:integer, 5}]}
      iex> {:ok, wire_ref} = Packet.decode_wire_ref(value)
      iex> wire_ref.variant
      :mine
      iex> wire_ref.oid
      5
  """
  @spec decode_wire_ref(Value.t()) :: {:ok, WireRef.t()} | {:error, term()}
  def decode_wire_ref({:sequence, [{:integer, 0}, {:integer, oid}]}) do
    {:ok, WireRef.mine(oid)}
  end

  def decode_wire_ref({:sequence, [{:integer, 1}, {:integer, oid} | attenuation]}) do
    {:ok, WireRef.yours(oid, attenuation)}
  end

  def decode_wire_ref(other) do
    {:error, {:invalid_wire_ref, other}}
  end

  # Helpers

  @doc """
  Creates a turn packet with the given events.
  """
  @spec turn([TurnEvent.t()]) :: Turn.t()
  def turn(events \\ []) do
    %Turn{events: events}
  end

  @doc """
  Creates a turn event.
  """
  @spec event(non_neg_integer(), event()) :: TurnEvent.t()
  def event(oid, event) do
    %TurnEvent{oid: oid, event: event}
  end

  @doc """
  Creates an assert event.
  """
  @spec assert(Value.t(), non_neg_integer()) :: Assert.t()
  def assert(assertion, handle) do
    %Assert{assertion: assertion, handle: handle}
  end

  @doc """
  Creates a retract event.
  """
  @spec retract(non_neg_integer()) :: Retract.t()
  def retract(handle) do
    %Retract{handle: handle}
  end

  @doc """
  Creates a message event.
  """
  @spec message(Value.t()) :: Message.t()
  def message(body) do
    %Message{body: body}
  end

  @doc """
  Creates a sync event.
  """
  @spec sync() :: Sync.t()
  def sync do
    %Sync{peer: true}
  end

  @doc """
  Creates an error packet.
  """
  @spec error(String.t(), Value.t()) :: Error.t()
  def error(message, detail \\ {:boolean, false}) do
    %Error{message: message, detail: detail}
  end
end
