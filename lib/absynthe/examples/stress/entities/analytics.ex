defmodule Absynthe.Examples.Stress.Entities.Analytics do
  @moduledoc """
  Reactive analytics entity using Dataflow fields.

  The Analytics entity:
  - Observes Order assertions to track best bid/ask
  - Observes Fill messages to compute volume and VWAP
  - Provides reactive fields for spread, volume, VWAP, etc.
  - Optionally asserts MarketStats snapshots

  ## Dataflow Fields

  Simple fields (updated directly):
  - `best_bid` - Current best bid price
  - `best_ask` - Current best ask price
  - `last_price` - Most recent trade price
  - `volume_24h` - 24-hour volume (simplified: session volume)
  - `total_value` - Sum of price * qty for VWAP calculation

  Computed fields (auto-update):
  - `spread` - best_ask - best_bid
  - `vwap` - total_value / volume_24h (scaled by 1e6)

  ## MarketStats Assertion Format (output, if enabled)

      <MarketStats {
        symbol: symbol,
        volume_24h: integer,
        vwap: integer,
        spread: integer,
        last_price: integer,
        updated_at: integer,
        seq: integer
      }>

  ## Usage

  Spawn one Analytics entity per symbol. Configure with `dataspace_ref` and
  `assert_stats: true` to publish MarketStats assertions.
  """

  alias Absynthe.Dataflow.Field
  alias Absynthe.Preserves.Value
  alias Absynthe.Core.Turn
  alias Absynthe.Protocol.Event
  alias Absynthe.Assertions.Handle

  defstruct [
    :symbol,
    :dataspace_ref,
    :best_bid_field,
    :best_ask_field,
    :last_price_field,
    :volume_field,
    :total_value_field,
    :spread_field,
    :vwap_field,
    assert_stats: false,
    bid_levels: %{},
    ask_levels: %{},
    handle_index: %{},
    stats_handle: nil,
    stats_seq: 0
  ]

  @type t :: %__MODULE__{
          symbol: term(),
          dataspace_ref: term() | nil,
          assert_stats: boolean(),
          best_bid_field: Field.t(),
          best_ask_field: Field.t(),
          last_price_field: Field.t(),
          volume_field: Field.t(),
          total_value_field: Field.t(),
          spread_field: Field.t(),
          vwap_field: Field.t(),
          bid_levels: %{integer() => pos_integer()},
          ask_levels: %{integer() => pos_integer()},
          handle_index: %{Handle.t() => {atom(), integer()}},
          stats_handle: Handle.t() | nil,
          stats_seq: non_neg_integer()
        }

  @doc """
  Creates a new Analytics entity for the given symbol.

  Options:
  - `dataspace_ref` - Reference to dataspace for asserting stats
  - `assert_stats` - Whether to assert MarketStats (default: false)
  """
  @spec new(term(), keyword()) :: t()
  def new(symbol, opts \\ []) do
    # Create simple fields
    best_bid_field = Field.new(nil)
    best_ask_field = Field.new(nil)
    last_price_field = Field.new(nil)
    volume_field = Field.new(0)
    total_value_field = Field.new(0)

    # Create computed fields
    spread_field =
      Field.computed(fn ->
        bid = Field.get(best_bid_field)
        ask = Field.get(best_ask_field)

        if bid && ask do
          ask - bid
        else
          nil
        end
      end)

    vwap_field =
      Field.computed(fn ->
        volume = Field.get(volume_field)
        total_value = Field.get(total_value_field)

        if volume > 0 do
          # Scale by 1e6 for precision
          div(total_value * 1_000_000, volume)
        else
          nil
        end
      end)

    %__MODULE__{
      symbol: symbol,
      dataspace_ref: Keyword.get(opts, :dataspace_ref),
      assert_stats: Keyword.get(opts, :assert_stats, false),
      best_bid_field: best_bid_field,
      best_ask_field: best_ask_field,
      last_price_field: last_price_field,
      volume_field: volume_field,
      total_value_field: total_value_field,
      spread_field: spread_field,
      vwap_field: vwap_field
    }
  end

  @doc """
  Returns current analytics values.
  """
  @spec get_stats(t()) :: map()
  def get_stats(analytics) do
    %{
      symbol: analytics.symbol,
      best_bid: Field.get(analytics.best_bid_field),
      best_ask: Field.get(analytics.best_ask_field),
      spread: Field.get(analytics.spread_field),
      last_price: Field.get(analytics.last_price_field),
      volume: Field.get(analytics.volume_field),
      vwap: Field.get(analytics.vwap_field)
    }
  end

  @doc """
  Parses an Order assertion.
  """
  @spec parse_order(term()) :: {:ok, map()} | :error
  def parse_order(assertion) do
    case assertion do
      {:record, {{:symbol, "Order"}, fields}} when length(fields) == 7 ->
        [
          {:symbol, _id},
          {:symbol, symbol},
          {:record, {{:symbol, side_str}, []}},
          {:integer, price},
          {:integer, _qty},
          {:symbol, _trader},
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
          {:ok, %{symbol: symbol, side: side, price: price}}
        end

      _ ->
        :error
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
          {:symbol, _fill_id},
          {:symbol, _order_id},
          {:symbol, symbol},
          {:record, {{:symbol, _side_str}, []}},
          {:integer, price},
          {:integer, qty},
          {:symbol, _counterparty},
          {:integer, _timestamp},
          {:integer, _seq}
        ] = fields

        {:ok, %{symbol: symbol, price: price, qty: qty}}

      _ ->
        :error
    end
  end

  @doc """
  Updates best bid/ask after adding or removing an order.
  """
  @spec update_best_prices(t(), Turn.t()) :: {t(), Turn.t()}
  def update_best_prices(analytics, turn) do
    # Calculate best bid (highest bid price)
    best_bid =
      if map_size(analytics.bid_levels) == 0 do
        nil
      else
        analytics.bid_levels |> Map.keys() |> Enum.max()
      end

    # Calculate best ask (lowest ask price)
    best_ask =
      if map_size(analytics.ask_levels) == 0 do
        nil
      else
        analytics.ask_levels |> Map.keys() |> Enum.min()
      end

    # Update fields
    turn = Field.set(analytics.best_bid_field, best_bid, turn)
    turn = Field.set(analytics.best_ask_field, best_ask, turn)

    {analytics, turn}
  end

  @doc """
  Adds an order to level tracking.
  """
  @spec add_order(t(), atom(), integer(), Handle.t()) :: t()
  def add_order(analytics, side, price, handle) do
    {levels_field, levels} =
      case side do
        :bid -> {:bid_levels, analytics.bid_levels}
        :ask -> {:ask_levels, analytics.ask_levels}
      end

    count = Map.get(levels, price, 0)
    levels = Map.put(levels, price, count + 1)
    handle_index = Map.put(analytics.handle_index, handle, {side, price})

    analytics
    |> Map.put(levels_field, levels)
    |> Map.put(:handle_index, handle_index)
  end

  @doc """
  Removes an order from level tracking.
  """
  @spec remove_order(t(), Handle.t()) :: t()
  def remove_order(analytics, handle) do
    case Map.get(analytics.handle_index, handle) do
      nil ->
        analytics

      {side, price} ->
        {levels_field, levels} =
          case side do
            :bid -> {:bid_levels, analytics.bid_levels}
            :ask -> {:ask_levels, analytics.ask_levels}
          end

        count = Map.get(levels, price, 0)

        levels =
          if count <= 1 do
            Map.delete(levels, price)
          else
            Map.put(levels, price, count - 1)
          end

        handle_index = Map.delete(analytics.handle_index, handle)

        analytics
        |> Map.put(levels_field, levels)
        |> Map.put(:handle_index, handle_index)
    end
  end

  @doc """
  Records a trade for volume/VWAP calculation.
  """
  @spec record_trade(t(), integer(), integer(), Turn.t()) :: {t(), Turn.t()}
  def record_trade(analytics, price, qty, turn) do
    current_volume = Field.get(analytics.volume_field)
    current_value = Field.get(analytics.total_value_field)

    turn = Field.set(analytics.volume_field, current_volume + qty, turn)
    turn = Field.set(analytics.total_value_field, current_value + price * qty, turn)
    turn = Field.set(analytics.last_price_field, price, turn)

    {analytics, turn}
  end

  @doc """
  Creates a MarketStats assertion value.
  """
  @spec make_stats_assertion(t()) :: Value.t()
  def make_stats_assertion(analytics) do
    stats = get_stats(analytics)

    Value.record(
      Value.symbol("MarketStats"),
      [
        Value.symbol(analytics.symbol),
        Value.integer(stats.volume || 0),
        Value.integer(stats.vwap || 0),
        Value.integer(stats.spread || 0),
        Value.integer(stats.last_price || 0),
        Value.integer(System.monotonic_time(:nanosecond)),
        Value.integer(analytics.stats_seq)
      ]
    )
  end
end

# Entity Protocol Implementation
defimpl Absynthe.Core.Entity, for: Absynthe.Examples.Stress.Entities.Analytics do
  alias Absynthe.Examples.Stress.Entities.Analytics
  alias Absynthe.Dataflow.Field
  alias Absynthe.Protocol.Event
  alias Absynthe.Core.Turn
  alias Absynthe.Assertions.Handle

  def on_publish(analytics, assertion, handle, turn) do
    case Analytics.parse_order(assertion) do
      {:ok, order} ->
        if order.symbol == analytics.symbol do
          # Add order to tracking
          analytics = Analytics.add_order(analytics, order.side, order.price, handle)
          # Update best prices
          {analytics, turn} = Analytics.update_best_prices(analytics, turn)
          # Maybe assert updated stats
          maybe_assert_stats(analytics, turn)
        else
          {analytics, turn}
        end

      :error ->
        {analytics, turn}
    end
  end

  def on_retract(analytics, handle, turn) do
    # Check if this handle was for an order we're tracking
    case Map.get(analytics.handle_index, handle) do
      nil ->
        {analytics, turn}

      {_side, _price} ->
        # Remove order from tracking
        analytics = Analytics.remove_order(analytics, handle)
        # Update best prices
        {analytics, turn} = Analytics.update_best_prices(analytics, turn)
        # Maybe assert updated stats
        maybe_assert_stats(analytics, turn)
    end
  end

  def on_message(analytics, message, turn) do
    case Analytics.parse_fill(message) do
      {:ok, fill} ->
        if fill.symbol == analytics.symbol do
          # Record the trade
          {analytics, turn} = Analytics.record_trade(analytics, fill.price, fill.qty, turn)
          # Maybe assert updated stats
          maybe_assert_stats(analytics, turn)
        else
          {analytics, turn}
        end

      :error ->
        {analytics, turn}
    end
  end

  def on_sync(analytics, peer_ref, turn) do
    action = Event.message(peer_ref, {:symbol, "synced"})
    turn = Turn.add_action(turn, action)
    {analytics, turn}
  end

  # Optionally assert/update MarketStats
  defp maybe_assert_stats(analytics, turn) do
    if analytics.assert_stats && analytics.dataspace_ref do
      stats_assertion = Analytics.make_stats_assertion(analytics)

      # Retract old stats if present
      turn =
        if analytics.stats_handle do
          Turn.add_action(turn, Event.retract(analytics.dataspace_ref, analytics.stats_handle))
        else
          turn
        end

      # Assert new stats
      new_handle = %Handle{id: {:market_stats, analytics.symbol, analytics.stats_seq + 1}}

      turn =
        Turn.add_action(turn, Event.assert(analytics.dataspace_ref, stats_assertion, new_handle))

      analytics = %{
        analytics
        | stats_handle: new_handle,
          stats_seq: analytics.stats_seq + 1
      }

      {analytics, turn}
    else
      {analytics, turn}
    end
  end
end
