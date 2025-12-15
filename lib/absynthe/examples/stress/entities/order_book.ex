defmodule Absynthe.Examples.Stress.Entities.OrderBook do
  @moduledoc """
  Per-symbol order book entity that tracks bids and asks.

  The OrderBook maintains the current state of a single trading symbol's order book
  by observing Order assertions. It tracks orders by price level and provides
  best bid/ask queries.

  ## Order Assertion Format

  Orders are expected as Preserves records:

      <Order {
        id: symbol,
        symbol: symbol,
        side: <bid> | <ask>,
        price: integer,
        qty: integer,
        trader: symbol,
        seq: integer
      }>

  ## Design

  - Orders are tracked in two sorted structures (bids desc, asks asc)
  - When an Order assertion is published, it's added to the book
  - When an Order is retracted (cancelled or filled), it's removed
  - The entity exposes best bid/ask via its state for analytics

  ## Usage

  Spawn one OrderBook entity per symbol in the market facet. The entity will
  automatically observe orders for its configured symbol.
  """

  alias Absynthe.Assertions.Handle

  defstruct [
    :symbol,
    # %{price => %{order_id => order_data}}
    bids: %{},
    asks: %{},
    # %{handle => {side, price, order_id}}
    handle_index: %{},
    # Best prices (nil if empty)
    best_bid: nil,
    best_ask: nil,
    # Counts
    bid_count: 0,
    ask_count: 0
  ]

  @type side :: :bid | :ask

  @type order_data :: %{
          id: term(),
          price: integer(),
          qty: integer(),
          trader: term(),
          seq: integer()
        }

  @type t :: %__MODULE__{
          symbol: term(),
          bids: %{integer() => %{term() => order_data()}},
          asks: %{integer() => %{term() => order_data()}},
          handle_index: %{Handle.t() => {side(), integer(), term()}},
          best_bid: integer() | nil,
          best_ask: integer() | nil,
          bid_count: non_neg_integer(),
          ask_count: non_neg_integer()
        }

  @doc """
  Creates a new OrderBook entity for the given symbol.
  """
  @spec new(term()) :: t()
  def new(symbol) do
    %__MODULE__{symbol: symbol}
  end

  @doc """
  Returns the best bid price, or nil if no bids.
  """
  @spec best_bid(t()) :: integer() | nil
  def best_bid(%__MODULE__{best_bid: best}), do: best

  @doc """
  Returns the best ask price, or nil if no asks.
  """
  @spec best_ask(t()) :: integer() | nil
  def best_ask(%__MODULE__{best_ask: best}), do: best

  @doc """
  Returns the spread (best_ask - best_bid), or nil if either side is empty.
  """
  @spec spread(t()) :: integer() | nil
  def spread(%__MODULE__{best_bid: nil}), do: nil
  def spread(%__MODULE__{best_ask: nil}), do: nil
  def spread(%__MODULE__{best_bid: bid, best_ask: ask}), do: ask - bid

  @doc """
  Returns the order count on each side.
  """
  @spec counts(t()) :: {non_neg_integer(), non_neg_integer()}
  def counts(%__MODULE__{bid_count: bids, ask_count: asks}), do: {bids, asks}

  @doc """
  Parses an Order assertion into an order map.
  """
  @spec parse_order(term()) :: {:ok, map()} | :error
  def parse_order(assertion) do
    case assertion do
      {:record, {{:symbol, "Order"}, fields}} when length(fields) == 7 ->
        [
          {:symbol, id},
          {:symbol, symbol},
          {:record, {{:symbol, side_str}, []}},
          {:integer, price},
          {:integer, qty},
          {:symbol, trader},
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
             id: id,
             symbol: symbol,
             side: side,
             price: price,
             qty: qty,
             trader: trader,
             seq: seq
           }}
        end

      _ ->
        :error
    end
  end

  @doc """
  Adds an order to the book.
  """
  @spec add_order(t(), side(), map(), Handle.t()) :: t()
  def add_order(book, side, order, handle) do
    %{id: id, price: price} = order
    order_data = order |> Map.delete(:side) |> Map.delete(:symbol)

    {side_map, count_key} =
      case side do
        :bid -> {book.bids, :bid_count}
        :ask -> {book.asks, :ask_count}
      end

    level = Map.get(side_map, price, %{})
    level = Map.put(level, id, order_data)
    side_map = Map.put(side_map, price, level)

    handle_index = Map.put(book.handle_index, handle, {side, price, id})
    new_count = Map.get(book, count_key) + 1

    book =
      case side do
        :bid -> %{book | bids: side_map}
        :ask -> %{book | asks: side_map}
      end

    book = %{book | handle_index: handle_index}
    book = Map.put(book, count_key, new_count)

    update_best_prices(book)
  end

  @doc """
  Removes an order from the book by its handle.
  """
  @spec remove_order(t(), Handle.t()) :: t()
  def remove_order(book, handle) do
    case Map.get(book.handle_index, handle) do
      nil ->
        book

      {side, price, order_id} ->
        {side_map, count_key} =
          case side do
            :bid -> {book.bids, :bid_count}
            :ask -> {book.asks, :ask_count}
          end

        level = Map.get(side_map, price, %{})
        level = Map.delete(level, order_id)

        side_map =
          if map_size(level) == 0 do
            Map.delete(side_map, price)
          else
            Map.put(side_map, price, level)
          end

        handle_index = Map.delete(book.handle_index, handle)
        new_count = max(0, Map.get(book, count_key) - 1)

        book =
          case side do
            :bid -> %{book | bids: side_map}
            :ask -> %{book | asks: side_map}
          end

        book = %{book | handle_index: handle_index}
        book = Map.put(book, count_key, new_count)

        update_best_prices(book)
    end
  end

  @doc """
  Returns all orders at a given price level for a side.
  """
  @spec orders_at_price(t(), side(), integer()) :: [order_data()]
  def orders_at_price(book, side, price) do
    side_map =
      case side do
        :bid -> book.bids
        :ask -> book.asks
      end

    case Map.get(side_map, price) do
      nil -> []
      level -> Map.values(level)
    end
  end

  # Internal: Recalculate best bid/ask
  defp update_best_prices(book) do
    best_bid =
      if map_size(book.bids) == 0 do
        nil
      else
        book.bids |> Map.keys() |> Enum.max()
      end

    best_ask =
      if map_size(book.asks) == 0 do
        nil
      else
        book.asks |> Map.keys() |> Enum.min()
      end

    %{book | best_bid: best_bid, best_ask: best_ask}
  end
end

# Entity Protocol Implementation
defimpl Absynthe.Core.Entity, for: Absynthe.Examples.Stress.Entities.OrderBook do
  alias Absynthe.Examples.Stress.Entities.OrderBook

  def on_publish(book, assertion, handle, turn) do
    case OrderBook.parse_order(assertion) do
      {:ok, order} ->
        if order.symbol == book.symbol do
          book = OrderBook.add_order(book, order.side, order, handle)
          {book, turn}
        else
          {book, turn}
        end

      :error ->
        {book, turn}
    end
  end

  def on_retract(book, handle, turn) do
    book = OrderBook.remove_order(book, handle)
    {book, turn}
  end

  def on_message(book, _message, turn) do
    {book, turn}
  end

  def on_sync(book, peer_ref, turn) do
    action = Absynthe.Protocol.Event.message(peer_ref, {:symbol, "synced"})
    turn = Absynthe.Core.Turn.add_action(turn, action)
    {book, turn}
  end
end
