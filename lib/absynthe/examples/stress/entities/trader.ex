defmodule Absynthe.Examples.Stress.Entities.Trader do
  @moduledoc """
  Trader bot entity that generates market orders for stress testing.

  The Trader entity simulates a market participant that:
  - Places orders based on a configurable strategy
  - Tracks order state (pending, filled, cancelled)
  - Observes fills to update internal state
  - Reports telemetry for monitoring

  ## Order Generation Strategy

  Traders support multiple strategies:
  - `:random` - Random bid/ask at random prices within a range
  - `:market_maker` - Places both bids and asks around mid-price
  - `:trend` - Places orders following a trend direction

  ## Usage

  Spawn one Trader entity per simulated client. Configure with:
  - `trader_id` - Unique identifier for this trader
  - `symbols` - List of symbols to trade
  - `dataspace_ref` - Reference to the market dataspace
  - `strategy` - Order generation strategy
  """

  alias Absynthe.Preserves.Value
  alias Absynthe.Core.Turn
  alias Absynthe.Protocol.Event
  alias Absynthe.Assertions.Handle

  defstruct [
    :trader_id,
    :dataspace_ref,
    symbols: [],
    strategy: :random,
    # Price range for random strategy
    price_range: {45000, 55000},
    # Quantity range
    qty_range: {1, 100},
    # Random seed for reproducibility
    seed: nil,
    # Current random state
    rand_state: nil,
    # Order state: %{order_id => %{handle: h, status: :pending|:partial|:filled, qty: n}}
    orders: %{},
    # Metrics
    next_order_seq: 1,
    orders_placed: 0,
    orders_filled: 0,
    orders_cancelled: 0,
    total_volume: 0
  ]

  @type strategy :: :random | :market_maker | :trend

  @type t :: %__MODULE__{
          trader_id: term(),
          dataspace_ref: term(),
          symbols: [term()],
          strategy: strategy(),
          price_range: {integer(), integer()},
          qty_range: {integer(), integer()},
          seed: integer() | nil,
          rand_state: :rand.state() | nil,
          orders: %{term() => map()},
          next_order_seq: pos_integer(),
          orders_placed: non_neg_integer(),
          orders_filled: non_neg_integer(),
          orders_cancelled: non_neg_integer(),
          total_volume: non_neg_integer()
        }

  @doc """
  Creates a new Trader entity.

  Options:
  - `trader_id` - Unique identifier (required)
  - `dataspace_ref` - Reference to the market dataspace (required)
  - `symbols` - List of symbols to trade (default: ["BTC-USD"])
  - `strategy` - Order generation strategy (default: :random)
  - `price_range` - Price range for random strategy (default: {45000, 55000})
  - `qty_range` - Quantity range (default: {1, 100})
  - `seed` - Random seed for reproducibility (default: nil, uses system random)
  """
  @spec new(keyword()) :: t()
  def new(opts) do
    trader_id = Keyword.fetch!(opts, :trader_id)
    dataspace_ref = Keyword.fetch!(opts, :dataspace_ref)

    seed = Keyword.get(opts, :seed)

    rand_state =
      if seed do
        :rand.seed_s(:exsss, seed)
      else
        :rand.seed_s(:exsss)
      end

    %__MODULE__{
      trader_id: trader_id,
      dataspace_ref: dataspace_ref,
      symbols: Keyword.get(opts, :symbols, ["BTC-USD"]),
      strategy: Keyword.get(opts, :strategy, :random),
      price_range: Keyword.get(opts, :price_range, {45000, 55000}),
      qty_range: Keyword.get(opts, :qty_range, {1, 100}),
      seed: seed,
      rand_state: rand_state
    }
  end

  @doc """
  Generates a new order based on the trader's strategy.

  Returns `{trader, order_params}` where order_params is a map with:
  - `id` - Order ID
  - `symbol` - Trading symbol
  - `side` - :bid or :ask
  - `price` - Order price
  - `qty` - Order quantity
  - `seq` - Order sequence number
  """
  @spec generate_order(t()) :: {t(), map()}
  def generate_order(trader) do
    {symbol, rand_state} = pick_random(trader.rand_state, trader.symbols)
    {side, rand_state} = random_side(rand_state)
    {price, rand_state} = random_in_range(rand_state, trader.price_range)
    {qty, rand_state} = random_in_range(rand_state, trader.qty_range)

    order_id = "#{trader.trader_id}-#{trader.next_order_seq}"

    order_params = %{
      id: order_id,
      symbol: symbol,
      side: side,
      price: price,
      qty: qty,
      seq: trader.next_order_seq
    }

    trader = %{
      trader
      | rand_state: rand_state,
        next_order_seq: trader.next_order_seq + 1
    }

    {trader, order_params}
  end

  @doc """
  Creates an Order assertion from order parameters.
  """
  @spec make_order_assertion(t(), map()) :: Value.t()
  def make_order_assertion(trader, order_params) do
    side_record = Value.record(Value.symbol(Atom.to_string(order_params.side)), [])

    Value.record(
      Value.symbol("Order"),
      [
        Value.symbol(order_params.id),
        Value.symbol(order_params.symbol),
        side_record,
        Value.integer(order_params.price),
        Value.integer(order_params.qty),
        Value.symbol(trader.trader_id),
        Value.integer(order_params.seq)
      ]
    )
  end

  @doc """
  Places an order by generating one and adding an assert action to the turn.

  Returns `{updated_trader, updated_turn}`.
  """
  @spec place_order(t(), Turn.t()) :: {t(), Turn.t()}
  def place_order(trader, turn) do
    {trader, order_params} = generate_order(trader)
    assertion = make_order_assertion(trader, order_params)

    # Create a handle for this order
    handle = %Handle{id: {:order, trader.trader_id, order_params.id}}

    # Add assert action
    action = Event.assert(trader.dataspace_ref, assertion, handle)
    turn = Turn.add_action(turn, action)

    # Track the order
    order_state = %{
      handle: handle,
      params: order_params,
      status: :pending,
      remaining_qty: order_params.qty
    }

    trader = %{
      trader
      | orders: Map.put(trader.orders, order_params.id, order_state),
        orders_placed: trader.orders_placed + 1
    }

    # Emit telemetry
    :telemetry.execute(
      [:stress, :order, :placed],
      %{count: 1},
      %{
        order_id: order_params.id,
        symbol: order_params.symbol,
        side: order_params.side,
        qty: order_params.qty,
        trader_id: trader.trader_id
      }
    )

    {trader, turn}
  end

  @doc """
  Cancels an existing order by adding a retract action to the turn.

  Returns `{updated_trader, updated_turn, :ok | :not_found}`.
  """
  @spec cancel_order(t(), term(), Turn.t()) :: {t(), Turn.t(), :ok | :not_found}
  def cancel_order(trader, order_id, turn) do
    case Map.get(trader.orders, order_id) do
      nil ->
        {trader, turn, :not_found}

      order_state ->
        # Add retract action
        action = Event.retract(trader.dataspace_ref, order_state.handle)
        turn = Turn.add_action(turn, action)

        # Update order state
        trader = %{
          trader
          | orders: Map.delete(trader.orders, order_id),
            orders_cancelled: trader.orders_cancelled + 1
        }

        # Emit telemetry
        :telemetry.execute(
          [:stress, :order, :cancelled],
          %{count: 1},
          %{order_id: order_id, reason: :manual, trader_id: trader.trader_id}
        )

        {trader, turn, :ok}
    end
  end

  @doc """
  Cancels a random pending order.

  Returns `{updated_trader, updated_turn}`.
  """
  @spec cancel_random_order(t(), Turn.t()) :: {t(), Turn.t()}
  def cancel_random_order(trader, turn) do
    pending_orders =
      trader.orders
      |> Enum.filter(fn {_id, state} -> state.status == :pending end)
      |> Enum.map(fn {id, _state} -> id end)

    if pending_orders == [] do
      {trader, turn}
    else
      {order_id, rand_state} = pick_random(trader.rand_state, pending_orders)
      trader = %{trader | rand_state: rand_state}
      {trader, turn, _result} = cancel_order(trader, order_id, turn)
      {trader, turn}
    end
  end

  @doc """
  Parses a Fill message.
  """
  @spec parse_fill(term()) :: {:ok, map()} | :error
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
          {:integer, seq}
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
             fill_id: fill_id,
             order_id: order_id,
             symbol: symbol,
             side: side,
             price: price,
             qty: qty,
             counterparty: counterparty,
             timestamp: timestamp,
             seq: seq
           }}
        end

      _ ->
        :error
    end
  end

  @doc """
  Handles a fill notification for one of our orders.
  """
  @spec handle_fill(t(), map()) :: t()
  def handle_fill(trader, fill) do
    case Map.get(trader.orders, fill.order_id) do
      nil ->
        # Not our order or already removed
        trader

      order_state ->
        new_remaining = order_state.remaining_qty - fill.qty

        if new_remaining <= 0 do
          # Fully filled - remove from tracking
          trader = %{
            trader
            | orders: Map.delete(trader.orders, fill.order_id),
              orders_filled: trader.orders_filled + 1,
              total_volume: trader.total_volume + fill.qty
          }

          :telemetry.execute(
            [:stress, :order, :filled],
            %{count: 1},
            %{
              order_id: fill.order_id,
              fill_qty: fill.qty,
              remaining_qty: 0,
              trader_id: trader.trader_id
            }
          )

          trader
        else
          # Partial fill - update remaining qty
          updated_order = %{order_state | remaining_qty: new_remaining, status: :partial}

          trader = %{
            trader
            | orders: Map.put(trader.orders, fill.order_id, updated_order),
              total_volume: trader.total_volume + fill.qty
          }

          :telemetry.execute(
            [:stress, :order, :filled],
            %{count: 1},
            %{
              order_id: fill.order_id,
              fill_qty: fill.qty,
              remaining_qty: new_remaining,
              trader_id: trader.trader_id
            }
          )

          trader
        end
    end
  end

  @doc """
  Returns trader statistics.
  """
  @spec stats(t()) :: map()
  def stats(trader) do
    pending_count = Enum.count(trader.orders, fn {_id, s} -> s.status == :pending end)
    partial_count = Enum.count(trader.orders, fn {_id, s} -> s.status == :partial end)

    %{
      trader_id: trader.trader_id,
      orders_placed: trader.orders_placed,
      orders_filled: trader.orders_filled,
      orders_cancelled: trader.orders_cancelled,
      total_volume: trader.total_volume,
      pending_orders: pending_count,
      partial_orders: partial_count
    }
  end

  # Random helpers

  defp pick_random(rand_state, list) do
    {idx, rand_state} = :rand.uniform_s(length(list), rand_state)
    {Enum.at(list, idx - 1), rand_state}
  end

  defp random_side(rand_state) do
    {n, rand_state} = :rand.uniform_s(2, rand_state)
    side = if n == 1, do: :bid, else: :ask
    {side, rand_state}
  end

  defp random_in_range(rand_state, {min, max}) do
    range = max - min + 1
    {n, rand_state} = :rand.uniform_s(range, rand_state)
    {min + n - 1, rand_state}
  end
end

# Entity Protocol Implementation
defimpl Absynthe.Core.Entity, for: Absynthe.Examples.Stress.Entities.Trader do
  alias Absynthe.Examples.Stress.Entities.Trader
  alias Absynthe.Protocol.Event
  alias Absynthe.Core.Turn

  def on_publish(trader, _assertion, _handle, turn) do
    # Trader doesn't observe assertions directly
    # Fill notifications come as messages
    {trader, turn}
  end

  def on_retract(trader, _handle, turn) do
    {trader, turn}
  end

  def on_message(trader, message, turn) do
    case Trader.parse_fill(message) do
      {:ok, fill} ->
        # Check if this fill is for one of our orders
        trader = Trader.handle_fill(trader, fill)
        {trader, turn}

      :error ->
        # Check for command messages
        case message do
          {:symbol, "place_order"} ->
            Trader.place_order(trader, turn)

          {:symbol, "cancel_random"} ->
            Trader.cancel_random_order(trader, turn)

          _ ->
            {trader, turn}
        end
    end
  end

  def on_sync(trader, peer_ref, turn) do
    action = Event.message(peer_ref, {:symbol, "synced"})
    turn = Turn.add_action(turn, action)
    {trader, turn}
  end
end
