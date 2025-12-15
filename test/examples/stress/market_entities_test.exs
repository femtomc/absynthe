defmodule Absynthe.Examples.Stress.MarketEntitiesTest do
  use ExUnit.Case, async: true

  alias Absynthe.Examples.Stress.Entities.{OrderBook, Matcher, History, Analytics}
  alias Absynthe.Assertions.Handle
  alias Absynthe.Core.Turn
  alias Absynthe.Preserves.Value

  # Helper to create an Order assertion
  defp make_order(id, symbol, side, price, qty, trader, seq) do
    side_record = Value.record(Value.symbol(Atom.to_string(side)), [])

    Value.record(
      Value.symbol("Order"),
      [
        Value.symbol(id),
        Value.symbol(symbol),
        side_record,
        Value.integer(price),
        Value.integer(qty),
        Value.symbol(trader),
        Value.integer(seq)
      ]
    )
  end

  # Helper to create a Fill message
  defp make_fill(fill_id, order_id, symbol, side, price, qty, counterparty, timestamp, seq) do
    side_record = Value.record(Value.symbol(Atom.to_string(side)), [])

    Value.record(
      Value.symbol("Fill"),
      [
        Value.symbol(fill_id),
        Value.symbol(order_id),
        Value.symbol(symbol),
        side_record,
        Value.integer(price),
        Value.integer(qty),
        Value.symbol(counterparty),
        Value.integer(timestamp),
        Value.integer(seq)
      ]
    )
  end

  describe "OrderBook" do
    test "new/1 creates an empty order book" do
      book = OrderBook.new("BTC-USD")
      assert book.symbol == "BTC-USD"
      assert OrderBook.best_bid(book) == nil
      assert OrderBook.best_ask(book) == nil
      assert OrderBook.counts(book) == {0, 0}
    end

    test "parse_order/1 parses valid Order assertions" do
      order = make_order("order-1", "BTC-USD", :bid, 50000, 100, "trader-1", 1)
      assert {:ok, parsed} = OrderBook.parse_order(order)
      assert parsed.id == "order-1"
      assert parsed.symbol == "BTC-USD"
      assert parsed.side == :bid
      assert parsed.price == 50000
      assert parsed.qty == 100
      assert parsed.trader == "trader-1"
      assert parsed.seq == 1
    end

    test "parse_order/1 returns error for invalid assertions" do
      assert :error == OrderBook.parse_order({:string, "not an order"})
      assert :error == OrderBook.parse_order({:record, {{:symbol, "NotOrder"}, []}})
    end

    test "add_order/4 adds orders and updates best prices" do
      book = OrderBook.new("BTC-USD")
      handle1 = %Handle{id: 1}
      handle2 = %Handle{id: 2}
      handle3 = %Handle{id: 3}

      # Add a bid
      book =
        OrderBook.add_order(
          book,
          :bid,
          %{id: "o1", price: 50000, qty: 100, trader: "t1", seq: 1},
          handle1
        )

      assert OrderBook.best_bid(book) == 50000
      assert OrderBook.best_ask(book) == nil
      assert OrderBook.counts(book) == {1, 0}

      # Add a higher bid
      book =
        OrderBook.add_order(
          book,
          :bid,
          %{id: "o2", price: 50100, qty: 50, trader: "t2", seq: 2},
          handle2
        )

      assert OrderBook.best_bid(book) == 50100
      assert OrderBook.counts(book) == {2, 0}

      # Add an ask
      book =
        OrderBook.add_order(
          book,
          :ask,
          %{id: "o3", price: 50200, qty: 75, trader: "t3", seq: 3},
          handle3
        )

      assert OrderBook.best_ask(book) == 50200
      assert OrderBook.counts(book) == {2, 1}
      assert OrderBook.spread(book) == 100
    end

    test "remove_order/2 removes orders and updates best prices" do
      book = OrderBook.new("BTC-USD")
      handle1 = %Handle{id: 1}
      handle2 = %Handle{id: 2}

      book =
        OrderBook.add_order(
          book,
          :bid,
          %{id: "o1", price: 50000, qty: 100, trader: "t1", seq: 1},
          handle1
        )

      book =
        OrderBook.add_order(
          book,
          :bid,
          %{id: "o2", price: 50100, qty: 50, trader: "t2", seq: 2},
          handle2
        )

      assert OrderBook.best_bid(book) == 50100

      # Remove the best bid
      book = OrderBook.remove_order(book, handle2)
      assert OrderBook.best_bid(book) == 50000
      assert OrderBook.counts(book) == {1, 0}

      # Remove the last bid
      book = OrderBook.remove_order(book, handle1)
      assert OrderBook.best_bid(book) == nil
      assert OrderBook.counts(book) == {0, 0}
    end

    test "spread/1 returns nil when either side is empty" do
      book = OrderBook.new("BTC-USD")
      assert OrderBook.spread(book) == nil

      handle = %Handle{id: 1}

      book =
        OrderBook.add_order(
          book,
          :bid,
          %{id: "o1", price: 50000, qty: 100, trader: "t1", seq: 1},
          handle
        )

      assert OrderBook.spread(book) == nil
    end
  end

  describe "Matcher" do
    test "new/2 creates an empty matcher" do
      matcher = Matcher.new("BTC-USD")
      assert matcher.symbol == "BTC-USD"
      assert matcher.total_fills == 0
      assert matcher.total_volume == 0
    end

    test "try_match/4 matches crossing orders" do
      matcher = Matcher.new("BTC-USD")
      ask_handle = %Handle{id: 1}

      # Add a resting ask at 50000
      matcher =
        Matcher.add_order(
          matcher,
          :ask,
          %{id: "ask-1", price: 50000, qty: 100, trader: "seller", seq: 1},
          ask_handle
        )

      # Incoming bid at 50000 should match
      bid_handle = %Handle{id: 2}
      bid_order = %{id: "bid-1", price: 50000, qty: 50, trader: "buyer", seq: 2}

      {_matcher, fills, remaining_qty} = Matcher.try_match(matcher, :bid, bid_order, bid_handle)

      assert length(fills) == 1
      assert remaining_qty == 0

      [{bid_info, ask_info, price, qty, opposite_fill_info}] = fills
      assert price == 50000
      assert qty == 50
      assert bid_info.id == "bid-1"
      assert ask_info.id == "ask-1"
      # Opposite order (ask) was partially filled (100 - 50 = 50 remaining)
      assert opposite_fill_info.fully_filled == false
      assert opposite_fill_info.remaining_qty == 50
    end

    test "try_match/4 does not match non-crossing orders" do
      matcher = Matcher.new("BTC-USD")
      ask_handle = %Handle{id: 1}

      # Add a resting ask at 50100
      matcher =
        Matcher.add_order(
          matcher,
          :ask,
          %{id: "ask-1", price: 50100, qty: 100, trader: "seller", seq: 1},
          ask_handle
        )

      # Incoming bid at 50000 should not match
      bid_handle = %Handle{id: 2}
      bid_order = %{id: "bid-1", price: 50000, qty: 50, trader: "buyer", seq: 2}

      {_matcher, fills, remaining_qty} = Matcher.try_match(matcher, :bid, bid_order, bid_handle)

      assert fills == []
      assert remaining_qty == 50
    end

    test "try_match/4 respects price-time priority" do
      matcher = Matcher.new("BTC-USD")

      # Add two asks at different prices
      matcher =
        Matcher.add_order(
          matcher,
          :ask,
          %{id: "ask-1", price: 50100, qty: 100, trader: "seller1", seq: 1},
          %Handle{id: 1}
        )

      matcher =
        Matcher.add_order(
          matcher,
          :ask,
          %{id: "ask-2", price: 50000, qty: 100, trader: "seller2", seq: 2},
          %Handle{id: 2}
        )

      # Bid should match the lower price first
      bid_order = %{id: "bid-1", price: 50200, qty: 50, trader: "buyer", seq: 3}

      {_matcher, fills, _remaining} = Matcher.try_match(matcher, :bid, bid_order, %Handle{id: 3})

      assert length(fills) == 1
      [{_bid, ask_info, price, _qty, _opposite_info}] = fills
      assert price == 50000
      assert ask_info.id == "ask-2"
    end

    test "try_match/4 handles partial fills" do
      matcher = Matcher.new("BTC-USD")
      ask_handle = %Handle{id: 1}

      # Add a resting ask at 50000 with qty 100
      matcher =
        Matcher.add_order(
          matcher,
          :ask,
          %{id: "ask-1", price: 50000, qty: 100, trader: "seller", seq: 1},
          ask_handle
        )

      # Incoming bid for 150 should partially fill (100 from ask, 50 remaining)
      bid_handle = %Handle{id: 2}
      bid_order = %{id: "bid-1", price: 50000, qty: 150, trader: "buyer", seq: 2}

      {_matcher, fills, remaining_qty} = Matcher.try_match(matcher, :bid, bid_order, bid_handle)

      assert length(fills) == 1
      assert remaining_qty == 50

      [{_bid_info, _ask_info, price, qty, opposite_fill_info}] = fills
      assert price == 50000
      assert qty == 100
      # Opposite order (ask) was fully filled
      assert opposite_fill_info.fully_filled == true
    end

    test "try_match/4 partial fill of resting order" do
      matcher = Matcher.new("BTC-USD")
      ask_handle = %Handle{id: 1}

      # Add a resting ask at 50000 with qty 100
      matcher =
        Matcher.add_order(
          matcher,
          :ask,
          %{id: "ask-1", price: 50000, qty: 100, trader: "seller", seq: 1},
          ask_handle
        )

      # Incoming bid for 30 should partially fill resting order
      bid_handle = %Handle{id: 2}
      bid_order = %{id: "bid-1", price: 50000, qty: 30, trader: "buyer", seq: 2}

      {matcher, fills, remaining_qty} = Matcher.try_match(matcher, :bid, bid_order, bid_handle)

      assert length(fills) == 1
      assert remaining_qty == 0

      [{_bid_info, _ask_info, price, qty, opposite_fill_info}] = fills
      assert price == 50000
      assert qty == 30
      # Opposite order (ask) was partially filled
      assert opposite_fill_info.fully_filled == false
      assert opposite_fill_info.remaining_qty == 70

      # Verify internal book still has the remaining qty
      assert map_size(matcher.asks) == 1
    end
  end

  describe "History" do
    test "new/1 creates an empty history" do
      history = History.new(symbol: "BTC-USD")
      assert history.symbol == "BTC-USD"
      assert history.total_trades == 0
      assert history.total_volume == 0
    end

    test "parse_fill/1 parses valid Fill messages" do
      fill = make_fill("fill-1", "order-1", "BTC-USD", :bid, 50000, 100, "seller", 123_456_789, 1)
      assert {:ok, parsed} = History.parse_fill(fill)
      assert parsed.id == "fill-1"
      assert parsed.symbol == "BTC-USD"
      assert parsed.side == :bid
      assert parsed.price == 50000
      assert parsed.qty == 100
    end

    test "add_trade/2 adds trades and updates stats" do
      history = History.new(symbol: "BTC-USD")

      trade1 = %{
        id: "fill-1",
        symbol: "BTC-USD",
        side: :bid,
        counterparty: "seller",
        price: 50000,
        qty: 100,
        timestamp: 1
      }

      trade2 = %{
        id: "fill-2",
        symbol: "BTC-USD",
        side: :ask,
        counterparty: "buyer",
        price: 50100,
        qty: 50,
        timestamp: 2
      }

      {history, evicted} = History.add_trade(history, trade1)
      assert evicted == nil
      assert history.total_trades == 1
      assert history.total_volume == 100
      assert history.last_price == 50000

      {history, evicted} = History.add_trade(history, trade2)
      assert evicted == nil
      assert history.total_trades == 2
      assert history.total_volume == 150
      assert history.last_price == 50100
    end

    test "recent_trades/2 returns trades in reverse order" do
      history = History.new(symbol: "BTC-USD")

      history =
        Enum.reduce(1..5, history, fn i, h ->
          {h, _evicted} =
            History.add_trade(h, %{
              id: "fill-#{i}",
              symbol: "BTC-USD",
              side: :bid,
              counterparty: "cp",
              price: 50000 + i,
              qty: 10 * i,
              timestamp: i
            })

          h
        end)

      recent = History.recent_trades(history, 3)
      assert length(recent) == 3
      assert hd(recent).id == "fill-5"
    end

    test "add_trade/2 evicts old trades when over limit" do
      # Enable reassert_trades so handles are created and tracked
      history = History.new(symbol: "BTC-USD", max_trades: 3, reassert_trades: true)

      # Pre-populate trade_handles to simulate having asserted trades
      # (add_trade doesn't actually create handles - that's done in the entity protocol)
      history =
        Enum.reduce(1..5, history, fn i, h ->
          handle = %Handle{id: {:trade, "fill-#{i}"}}
          %{h | trade_handles: Map.put(h.trade_handles, "fill-#{i}", handle)}
        end)

      {history, evicted_handles} =
        Enum.reduce(1..5, {history, []}, fn i, {h, evicted} ->
          {h, evicted_handle} =
            History.add_trade(h, %{
              id: "fill-#{i}",
              symbol: "BTC-USD",
              side: :bid,
              counterparty: "cp",
              price: 50000,
              qty: 10,
              timestamp: i
            })

          evicted =
            if evicted_handle do
              [evicted_handle | evicted]
            else
              evicted
            end

          {h, evicted}
        end)

      assert history.trade_count == 3
      assert history.total_trades == 5
      # Two trades were evicted (fill-1 and fill-2)
      assert length(evicted_handles) == 2

      recent = History.recent_trades(history, 10)
      ids = Enum.map(recent, & &1.id)
      assert "fill-1" not in ids
      assert "fill-2" not in ids
      assert "fill-5" in ids
    end
  end

  describe "Analytics" do
    test "new/2 creates analytics with dataflow fields" do
      analytics = Analytics.new("BTC-USD")
      assert analytics.symbol == "BTC-USD"

      stats = Analytics.get_stats(analytics)
      assert stats.best_bid == nil
      assert stats.best_ask == nil
      assert stats.spread == nil
      assert stats.volume == 0
    end

    test "add_order/4 and update_best_prices/2 track orders" do
      analytics = Analytics.new("BTC-USD")
      turn = Turn.new(:test_actor, :test_facet)
      handle1 = %Handle{id: 1}
      handle2 = %Handle{id: 2}

      analytics = Analytics.add_order(analytics, :bid, 50000, handle1)
      {analytics, turn} = Analytics.update_best_prices(analytics, turn)

      # Commit field updates
      Absynthe.Dataflow.Field.commit_field_updates(turn)

      stats = Analytics.get_stats(analytics)
      assert stats.best_bid == 50000
      assert stats.best_ask == nil

      turn = Turn.new(:test_actor, :test_facet)
      analytics = Analytics.add_order(analytics, :ask, 50100, handle2)
      {analytics, turn} = Analytics.update_best_prices(analytics, turn)
      Absynthe.Dataflow.Field.commit_field_updates(turn)

      stats = Analytics.get_stats(analytics)
      assert stats.best_bid == 50000
      assert stats.best_ask == 50100
      assert stats.spread == 100
    end

    test "record_trade/4 updates volume and VWAP" do
      analytics = Analytics.new("BTC-USD")
      turn = Turn.new(:test_actor, :test_facet)

      {analytics, turn} = Analytics.record_trade(analytics, 50000, 100, turn)
      Absynthe.Dataflow.Field.commit_field_updates(turn)

      stats = Analytics.get_stats(analytics)
      assert stats.volume == 100
      assert stats.last_price == 50000
      # VWAP = (50000 * 100) / 100 * 1e6 = 50000 * 1e6
      assert stats.vwap == 50_000_000_000

      turn = Turn.new(:test_actor, :test_facet)
      {_analytics, turn} = Analytics.record_trade(analytics, 50200, 100, turn)
      Absynthe.Dataflow.Field.commit_field_updates(turn)

      # Note: analytics struct doesn't get updated by commit_field_updates
      # The fields are in the registry, so we need to re-read
      stats = Analytics.get_stats(analytics)
      assert stats.volume == 200
      # VWAP = (50000*100 + 50200*100) / 200 * 1e6 = 50100 * 1e6
      assert stats.vwap == 50_100_000_000
    end

    test "remove_order/2 updates tracking" do
      analytics = Analytics.new("BTC-USD")
      turn = Turn.new(:test_actor, :test_facet)
      handle1 = %Handle{id: 1}
      handle2 = %Handle{id: 2}

      analytics = Analytics.add_order(analytics, :bid, 50000, handle1)
      analytics = Analytics.add_order(analytics, :bid, 50100, handle2)
      {analytics, turn} = Analytics.update_best_prices(analytics, turn)
      Absynthe.Dataflow.Field.commit_field_updates(turn)

      stats = Analytics.get_stats(analytics)
      assert stats.best_bid == 50100

      turn = Turn.new(:test_actor, :test_facet)
      analytics = Analytics.remove_order(analytics, handle2)
      {analytics, turn} = Analytics.update_best_prices(analytics, turn)
      Absynthe.Dataflow.Field.commit_field_updates(turn)

      stats = Analytics.get_stats(analytics)
      assert stats.best_bid == 50000
    end
  end
end
