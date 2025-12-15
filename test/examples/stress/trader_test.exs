defmodule Absynthe.Examples.Stress.TraderTest do
  use ExUnit.Case, async: true

  alias Absynthe.Examples.Stress.Entities.Trader
  alias Absynthe.Core.Turn
  alias Absynthe.Preserves.Value
  alias Absynthe.Core.Ref

  describe "new/1" do
    test "creates a trader with default options" do
      dataspace_ref = %Ref{actor_id: :test, entity_id: 1}

      trader =
        Trader.new(
          trader_id: "trader-1",
          dataspace_ref: dataspace_ref
        )

      assert trader.trader_id == "trader-1"
      assert trader.dataspace_ref == dataspace_ref
      assert trader.symbols == ["BTC-USD"]
      assert trader.strategy == :random
      assert trader.orders == %{}
      assert trader.orders_placed == 0
    end

    test "creates a trader with custom options" do
      dataspace_ref = %Ref{actor_id: :test, entity_id: 1}

      trader =
        Trader.new(
          trader_id: "trader-2",
          dataspace_ref: dataspace_ref,
          symbols: ["ETH-USD", "SOL-USD"],
          strategy: :market_maker,
          price_range: {1000, 2000},
          qty_range: {10, 50},
          seed: 42
        )

      assert trader.symbols == ["ETH-USD", "SOL-USD"]
      assert trader.strategy == :market_maker
      assert trader.price_range == {1000, 2000}
      assert trader.qty_range == {10, 50}
      assert trader.seed == 42
    end
  end

  describe "generate_order/1" do
    test "generates orders with valid parameters" do
      dataspace_ref = %Ref{actor_id: :test, entity_id: 1}

      trader =
        Trader.new(
          trader_id: "trader-1",
          dataspace_ref: dataspace_ref,
          symbols: ["BTC-USD"],
          price_range: {45000, 55000},
          qty_range: {1, 100},
          seed: 42
        )

      {trader, order} = Trader.generate_order(trader)

      assert order.id == "trader-1-1"
      assert order.symbol == "BTC-USD"
      assert order.side in [:bid, :ask]
      assert order.price >= 45000 and order.price <= 55000
      assert order.qty >= 1 and order.qty <= 100
      assert order.seq == 1

      # Next order should have incremented seq
      {_trader, order2} = Trader.generate_order(trader)
      assert order2.seq == 2
    end

    test "generates reproducible orders with same seed" do
      dataspace_ref = %Ref{actor_id: :test, entity_id: 1}

      trader1 =
        Trader.new(
          trader_id: "trader-1",
          dataspace_ref: dataspace_ref,
          seed: 42
        )

      trader2 =
        Trader.new(
          trader_id: "trader-1",
          dataspace_ref: dataspace_ref,
          seed: 42
        )

      {_, order1} = Trader.generate_order(trader1)
      {_, order2} = Trader.generate_order(trader2)

      assert order1.price == order2.price
      assert order1.qty == order2.qty
      assert order1.side == order2.side
    end
  end

  describe "make_order_assertion/2" do
    test "creates valid Preserves Order record" do
      dataspace_ref = %Ref{actor_id: :test, entity_id: 1}

      trader =
        Trader.new(
          trader_id: "trader-1",
          dataspace_ref: dataspace_ref
        )

      order_params = %{
        id: "order-1",
        symbol: "BTC-USD",
        side: :bid,
        price: 50000,
        qty: 100,
        seq: 1
      }

      assertion = Trader.make_order_assertion(trader, order_params)

      # Should be a valid Order record
      assert {:record, {{:symbol, "Order"}, fields}} = assertion
      assert length(fields) == 7

      [
        {:symbol, id},
        {:symbol, symbol},
        {:record, {{:symbol, side}, []}},
        {:integer, price},
        {:integer, qty},
        {:symbol, trader_id},
        {:integer, seq}
      ] = fields

      assert id == "order-1"
      assert symbol == "BTC-USD"
      assert side == "bid"
      assert price == 50000
      assert qty == 100
      assert trader_id == "trader-1"
      assert seq == 1
    end
  end

  describe "place_order/2" do
    test "places an order and tracks it" do
      dataspace_ref = %Ref{actor_id: :test, entity_id: 1}

      trader =
        Trader.new(
          trader_id: "trader-1",
          dataspace_ref: dataspace_ref,
          seed: 42
        )

      turn = Turn.new(:test_actor, :test_facet)

      {trader, turn} = Trader.place_order(trader, turn)

      # Order should be tracked
      assert map_size(trader.orders) == 1
      assert trader.orders_placed == 1

      # Turn should have an assert action
      actions = Turn.actions(turn)
      assert length(actions) == 1
      assert %Absynthe.Protocol.Event.Assert{} = hd(actions)
    end

    test "increments order sequence" do
      dataspace_ref = %Ref{actor_id: :test, entity_id: 1}

      trader =
        Trader.new(
          trader_id: "trader-1",
          dataspace_ref: dataspace_ref,
          seed: 42
        )

      turn = Turn.new(:test_actor, :test_facet)

      {trader, _turn} = Trader.place_order(trader, turn)
      turn2 = Turn.new(:test_actor, :test_facet)
      {trader, _turn} = Trader.place_order(trader, turn2)

      assert trader.orders_placed == 2
      assert trader.next_order_seq == 3
    end
  end

  describe "cancel_order/3" do
    test "cancels an existing order" do
      dataspace_ref = %Ref{actor_id: :test, entity_id: 1}

      trader =
        Trader.new(
          trader_id: "trader-1",
          dataspace_ref: dataspace_ref,
          seed: 42
        )

      turn = Turn.new(:test_actor, :test_facet)

      {trader, _turn} = Trader.place_order(trader, turn)
      order_id = "trader-1-1"

      turn2 = Turn.new(:test_actor, :test_facet)
      {trader, turn2, result} = Trader.cancel_order(trader, order_id, turn2)

      assert result == :ok
      assert map_size(trader.orders) == 0
      assert trader.orders_cancelled == 1

      # Turn should have a retract action
      actions = Turn.actions(turn2)
      assert length(actions) == 1
      assert %Absynthe.Protocol.Event.Retract{} = hd(actions)
    end

    test "returns :not_found for unknown order" do
      dataspace_ref = %Ref{actor_id: :test, entity_id: 1}

      trader =
        Trader.new(
          trader_id: "trader-1",
          dataspace_ref: dataspace_ref
        )

      turn = Turn.new(:test_actor, :test_facet)

      {trader, turn, result} = Trader.cancel_order(trader, "unknown-order", turn)

      assert result == :not_found
      assert trader.orders_cancelled == 0
      assert Turn.actions(turn) == []
    end
  end

  describe "parse_fill/1" do
    test "parses valid Fill message" do
      fill = make_fill("fill-1", "order-1", "BTC-USD", :bid, 50000, 100, "seller", 123_456, 1)

      assert {:ok, parsed} = Trader.parse_fill(fill)
      assert parsed.fill_id == "fill-1"
      assert parsed.order_id == "order-1"
      assert parsed.symbol == "BTC-USD"
      assert parsed.side == :bid
      assert parsed.price == 50000
      assert parsed.qty == 100
      assert parsed.counterparty == "seller"
    end

    test "returns error for invalid message" do
      assert :error == Trader.parse_fill({:string, "not a fill"})
    end
  end

  describe "handle_fill/2" do
    test "handles full fill" do
      dataspace_ref = %Ref{actor_id: :test, entity_id: 1}

      trader =
        Trader.new(
          trader_id: "trader-1",
          dataspace_ref: dataspace_ref,
          seed: 42
        )

      turn = Turn.new(:test_actor, :test_facet)

      {trader, _turn} = Trader.place_order(trader, turn)
      order_id = "trader-1-1"

      fill = %{
        fill_id: "fill-1",
        order_id: order_id,
        symbol: "BTC-USD",
        side: :bid,
        price: 50000,
        qty: 100,
        counterparty: "seller",
        timestamp: 123_456,
        seq: 1
      }

      # Set the order qty to match fill
      order_state = trader.orders[order_id]
      trader = %{trader | orders: %{order_id => %{order_state | remaining_qty: 100}}}

      trader = Trader.handle_fill(trader, fill)

      # Order should be removed (fully filled)
      assert map_size(trader.orders) == 0
      assert trader.orders_filled == 1
      assert trader.total_volume == 100
    end

    test "handles partial fill" do
      dataspace_ref = %Ref{actor_id: :test, entity_id: 1}

      trader =
        Trader.new(
          trader_id: "trader-1",
          dataspace_ref: dataspace_ref,
          seed: 42
        )

      turn = Turn.new(:test_actor, :test_facet)

      {trader, _turn} = Trader.place_order(trader, turn)
      order_id = "trader-1-1"

      # Set remaining qty to 100
      order_state = trader.orders[order_id]
      trader = %{trader | orders: %{order_id => %{order_state | remaining_qty: 100}}}

      fill = %{
        fill_id: "fill-1",
        order_id: order_id,
        symbol: "BTC-USD",
        side: :bid,
        price: 50000,
        qty: 30,
        counterparty: "seller",
        timestamp: 123_456,
        seq: 1
      }

      trader = Trader.handle_fill(trader, fill)

      # Order should remain with reduced qty
      assert map_size(trader.orders) == 1
      assert trader.orders[order_id].remaining_qty == 70
      assert trader.orders[order_id].status == :partial
      assert trader.total_volume == 30
    end
  end

  describe "stats/1" do
    test "returns trader statistics" do
      dataspace_ref = %Ref{actor_id: :test, entity_id: 1}

      trader =
        Trader.new(
          trader_id: "trader-1",
          dataspace_ref: dataspace_ref,
          seed: 42
        )

      turn = Turn.new(:test_actor, :test_facet)

      {trader, _turn} = Trader.place_order(trader, turn)
      turn2 = Turn.new(:test_actor, :test_facet)
      {trader, _turn} = Trader.place_order(trader, turn2)

      stats = Trader.stats(trader)

      assert stats.trader_id == "trader-1"
      assert stats.orders_placed == 2
      assert stats.orders_filled == 0
      assert stats.orders_cancelled == 0
      assert stats.pending_orders == 2
    end
  end

  # Helper to create Fill messages
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
end
