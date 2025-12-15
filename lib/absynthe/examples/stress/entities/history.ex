defmodule Absynthe.Examples.Stress.Entities.History do
  @moduledoc """
  Trade history entity that observes fills and maintains an audit log.

  The History entity:
  - Observes Fill messages for configured symbols
  - Maintains a bounded circular buffer of recent trades
  - Tracks trade statistics (count, volume, last price)
  - Optionally re-asserts trades as Trade assertions for other observers

  ## Fill Message Format (input)

      <Fill {
        fill_id: symbol,
        order_id: symbol,
        symbol: symbol,
        side: <bid> | <ask>,
        price: integer,
        qty: integer,
        counterparty: symbol,
        timestamp: integer,
        seq: integer
      }>

  ## Trade Assertion Format (output, if enabled)

      <Trade {
        id: symbol,
        symbol: symbol,
        price: integer,
        qty: integer,
        timestamp: integer,
        side: <bid> | <ask>,
        counterparty: symbol
      }>

  ## Usage

  Spawn one History entity per symbol (or a single one with symbol: nil for all).
  The entity will observe Fill messages and build a trade history.
  """

  alias Absynthe.Preserves.Value
  alias Absynthe.Core.Turn
  alias Absynthe.Protocol.Event
  alias Absynthe.Assertions.Handle

  defstruct [
    # nil means observe all symbols
    :symbol,
    # Reference to dataspace for re-asserting trades
    :dataspace_ref,
    # Whether to re-assert trades
    reassert_trades: false,
    # Circular buffer of recent trades
    trades: :queue.new(),
    # Max trades to keep
    max_trades: 1000,
    # Current count in buffer
    trade_count: 0,
    # Statistics
    total_trades: 0,
    total_volume: 0,
    last_price: nil,
    last_timestamp: nil,
    # Handles for asserted trades (for cleanup)
    trade_handles: %{}
  ]

  @type trade :: %{
          id: term(),
          symbol: term(),
          price: integer(),
          qty: integer(),
          timestamp: integer(),
          side: :bid | :ask,
          counterparty: term()
        }

  @type t :: %__MODULE__{
          symbol: term() | nil,
          dataspace_ref: term() | nil,
          reassert_trades: boolean(),
          trades: :queue.queue(trade()),
          max_trades: pos_integer(),
          trade_count: non_neg_integer(),
          total_trades: non_neg_integer(),
          total_volume: non_neg_integer(),
          last_price: integer() | nil,
          last_timestamp: integer() | nil,
          trade_handles: %{term() => Handle.t()}
        }

  @doc """
  Creates a new History entity.

  Options:
  - `symbol` - Filter to specific symbol (nil for all)
  - `dataspace_ref` - Reference to dataspace for re-asserting trades
  - `reassert_trades` - Whether to re-assert trades (default: false)
  - `max_trades` - Maximum trades to keep in buffer (default: 1000)
  """
  @spec new(keyword()) :: t()
  def new(opts \\ []) do
    %__MODULE__{
      symbol: Keyword.get(opts, :symbol),
      dataspace_ref: Keyword.get(opts, :dataspace_ref),
      reassert_trades: Keyword.get(opts, :reassert_trades, false),
      max_trades: Keyword.get(opts, :max_trades, 1000)
    }
  end

  @doc """
  Parses a Fill message into a trade record.
  """
  @spec parse_fill(term()) :: {:ok, trade()} | :error
  def parse_fill(message) do
    case message do
      {:record, {{:symbol, "Fill"}, fields}} when length(fields) == 9 ->
        [
          {:symbol, fill_id},
          {:symbol, order_id},
          {:symbol, symbol},
          {:record, {{:symbol, side_str}, []}},
          {:integer, price},
          {:integer, qty},
          {:symbol, counterparty},
          {:integer, timestamp},
          {:integer, _seq}
        ] = fields

        side =
          case side_str do
            "bid" -> :bid
            "ask" -> :ask
            _ -> :unknown
          end

        if side == :unknown do
          :error
        else
          {:ok,
           %{
             id: fill_id,
             order_id: order_id,
             symbol: symbol,
             side: side,
             price: price,
             qty: qty,
             counterparty: counterparty,
             timestamp: timestamp
           }}
        end

      _ ->
        :error
    end
  end

  @doc """
  Adds a trade to the history buffer.

  Returns `{updated_history, evicted_handle}` where `evicted_handle` is the
  handle of any evicted Trade assertion that needs to be retracted, or nil.
  """
  @spec add_trade(t(), trade()) :: {t(), Handle.t() | nil}
  def add_trade(history, trade) do
    # Add to queue
    trades = :queue.in(trade, history.trades)
    trade_count = history.trade_count + 1

    # Evict oldest if over limit
    {trades, trade_count, evicted_id} =
      if trade_count > history.max_trades do
        {{:value, oldest}, trades} = :queue.out(trades)
        {trades, trade_count - 1, oldest.id}
      else
        {trades, trade_count, nil}
      end

    # Get handle of evicted trade (if any)
    evicted_handle =
      if evicted_id do
        Map.get(history.trade_handles, evicted_id)
      else
        nil
      end

    # Update stats
    history = %{
      history
      | trades: trades,
        trade_count: trade_count,
        total_trades: history.total_trades + 1,
        total_volume: history.total_volume + trade.qty,
        last_price: trade.price,
        last_timestamp: trade.timestamp
    }

    # Remove evicted trade handle from tracking
    history =
      if evicted_id && Map.has_key?(history.trade_handles, evicted_id) do
        %{history | trade_handles: Map.delete(history.trade_handles, evicted_id)}
      else
        history
      end

    {history, evicted_handle}
  end

  @doc """
  Returns recent trades (most recent first).
  """
  @spec recent_trades(t(), non_neg_integer()) :: [trade()]
  def recent_trades(history, limit \\ 10) do
    history.trades
    |> :queue.to_list()
    |> Enum.reverse()
    |> Enum.take(limit)
  end

  @doc """
  Returns trade statistics.
  """
  @spec stats(t()) :: map()
  def stats(history) do
    %{
      total_trades: history.total_trades,
      total_volume: history.total_volume,
      last_price: history.last_price,
      last_timestamp: history.last_timestamp,
      buffer_size: history.trade_count
    }
  end

  @doc """
  Creates a Trade assertion value for re-asserting.
  """
  @spec make_trade_assertion(trade()) :: Value.t()
  def make_trade_assertion(trade) do
    side_record = Value.record(Value.symbol(Atom.to_string(trade.side)), [])

    Value.record(
      Value.symbol("Trade"),
      [
        Value.symbol(trade.id),
        Value.symbol(trade.symbol),
        Value.integer(trade.price),
        Value.integer(trade.qty),
        Value.integer(trade.timestamp),
        side_record,
        Value.symbol(trade.counterparty)
      ]
    )
  end
end

# Entity Protocol Implementation
defimpl Absynthe.Core.Entity, for: Absynthe.Examples.Stress.Entities.History do
  alias Absynthe.Examples.Stress.Entities.History
  alias Absynthe.Protocol.Event
  alias Absynthe.Core.Turn
  alias Absynthe.Assertions.Handle

  # History primarily responds to messages (Fill notifications)
  def on_publish(history, _assertion, _handle, turn) do
    {history, turn}
  end

  def on_retract(history, _handle, turn) do
    {history, turn}
  end

  def on_message(history, message, turn) do
    case History.parse_fill(message) do
      {:ok, trade} ->
        # Check symbol filter
        if history.symbol == nil || trade.symbol == history.symbol do
          process_trade(history, trade, turn)
        else
          {history, turn}
        end

      :error ->
        {history, turn}
    end
  end

  def on_sync(history, peer_ref, turn) do
    action = Event.message(peer_ref, {:symbol, "synced"})
    turn = Turn.add_action(turn, action)
    {history, turn}
  end

  # Process a new trade
  defp process_trade(history, trade, turn) do
    {history, evicted_handle} = History.add_trade(history, trade)

    # Retract evicted trade assertion (if any)
    turn =
      if evicted_handle && history.dataspace_ref do
        Turn.add_action(turn, Event.retract(history.dataspace_ref, evicted_handle))
      else
        turn
      end

    # Optionally re-assert as Trade
    {history, turn} =
      if history.reassert_trades && history.dataspace_ref do
        trade_assertion = History.make_trade_assertion(trade)
        handle = %Handle{id: {:trade, trade.id}}
        action = Event.assert(history.dataspace_ref, trade_assertion, handle)
        turn = Turn.add_action(turn, action)
        history = %{history | trade_handles: Map.put(history.trade_handles, trade.id, handle)}
        {history, turn}
      else
        {history, turn}
      end

    {history, turn}
  end
end
