defmodule Absynthe.Examples.Stress.Entities.Matcher do
  @moduledoc """
  Order matching engine that processes trades for a symbol.

  The Matcher observes Order assertions and performs matching when orders can
  cross (bid >= ask). When a match occurs:

  1. The Matcher sends Fill messages to both parties
  2. The matched orders are retracted (or partially filled)
  3. Analytics are notified of the trade

  ## Matching Algorithm

  Uses price-time priority:
  - Best prices are matched first (highest bid vs lowest ask)
  - At the same price, earlier orders (by seq) fill first
  - Partial fills are supported via retract + re-assert with reduced qty

  ## Fill Message Format

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

  ## Design

  The Matcher maintains its own view of the order book to perform matching.
  This is separate from the OrderBook entity to allow for different
  matching strategies and to keep concerns separated.
  """

  alias Absynthe.Preserves.Value
  alias Absynthe.Core.Turn
  alias Absynthe.Protocol.Event
  alias Absynthe.Assertions.Handle

  defstruct [
    :symbol,
    :dataspace_ref,
    # %{price => [{seq, order_id, qty, trader, handle}]} sorted by seq
    bids: %{},
    asks: %{},
    # %{handle => {side, price, order_id}}
    handle_index: %{},
    # Monotonic counters
    next_fill_id: 1,
    next_fill_seq: 1,
    # Metrics
    total_fills: 0,
    total_volume: 0
  ]

  @type t :: %__MODULE__{
          symbol: term(),
          dataspace_ref: term(),
          bids: %{integer() => list()},
          asks: %{integer() => list()},
          handle_index: %{Handle.t() => {atom(), integer(), term()}},
          next_fill_id: pos_integer(),
          next_fill_seq: pos_integer(),
          total_fills: non_neg_integer(),
          total_volume: non_neg_integer()
        }

  @doc """
  Creates a new Matcher entity for the given symbol.

  The dataspace_ref is used for sending Fill messages.
  """
  @spec new(term(), term()) :: t()
  def new(symbol, dataspace_ref \\ nil) do
    %__MODULE__{
      symbol: symbol,
      dataspace_ref: dataspace_ref
    }
  end

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
  Adds an order to the matcher's internal book.
  """
  @spec add_order(t(), atom(), map(), Handle.t()) :: t()
  def add_order(matcher, side, order, handle) do
    %{id: id, price: price, qty: qty, trader: trader, seq: seq} = order

    entry = {seq, id, qty, trader, handle}

    {side_map, field} =
      case side do
        :bid -> {matcher.bids, :bids}
        :ask -> {matcher.asks, :asks}
      end

    level = Map.get(side_map, price, [])
    # Insert sorted by seq (earlier orders first)
    level = insert_sorted_by_seq(level, entry)
    side_map = Map.put(side_map, price, level)

    handle_index = Map.put(matcher.handle_index, handle, {side, price, id})

    matcher = Map.put(matcher, field, side_map)
    %{matcher | handle_index: handle_index}
  end

  @doc """
  Removes an order from the matcher's internal book.
  """
  @spec remove_order(t(), Handle.t()) :: t()
  def remove_order(matcher, handle) do
    case Map.get(matcher.handle_index, handle) do
      nil ->
        matcher

      {side, price, order_id} ->
        {side_map, field} =
          case side do
            :bid -> {matcher.bids, :bids}
            :ask -> {matcher.asks, :asks}
          end

        level = Map.get(side_map, price, [])
        level = Enum.reject(level, fn {_seq, id, _qty, _trader, _h} -> id == order_id end)

        side_map =
          if level == [] do
            Map.delete(side_map, price)
          else
            Map.put(side_map, price, level)
          end

        handle_index = Map.delete(matcher.handle_index, handle)

        matcher = Map.put(matcher, field, side_map)
        %{matcher | handle_index: handle_index}
    end
  end

  @doc """
  Attempts to match the given order against the opposite side.

  Returns {updated_matcher, fills, remaining_qty} where fills is a list
  of {bid_order, ask_order, price, qty} tuples.
  """
  @spec try_match(t(), atom(), map(), Handle.t()) ::
          {t(), [{map(), map(), integer(), integer()}], integer()}
  def try_match(matcher, side, order, handle) do
    opposite_side =
      case side do
        :bid -> :asks
        :ask -> :bids
      end

    opposite_map = Map.get(matcher, opposite_side)
    %{price: order_price, qty: order_qty} = order

    # Find matchable price levels
    matchable_prices =
      case side do
        # Buying: match asks <= bid price (lowest first)
        :bid ->
          opposite_map
          |> Map.keys()
          |> Enum.filter(fn ask_price -> ask_price <= order_price end)
          |> Enum.sort()

        # Selling: match bids >= ask price (highest first)
        :ask ->
          opposite_map
          |> Map.keys()
          |> Enum.filter(fn bid_price -> bid_price >= order_price end)
          |> Enum.sort(:desc)
      end

    # Execute matches
    {matcher, fills, remaining_qty} =
      execute_matches(matcher, side, order, handle, matchable_prices, order_qty, [])

    {matcher, Enum.reverse(fills), remaining_qty}
  end

  @doc """
  Creates a Fill message value.
  """
  @spec make_fill(t(), term(), term(), term(), atom(), integer(), integer(), term(), integer()) ::
          {t(), Value.t()}
  def make_fill(matcher, fill_id, order_id, symbol, side, price, qty, counterparty, timestamp) do
    fill_seq = matcher.next_fill_seq

    fill_value =
      Value.record(
        Value.symbol("Fill"),
        [
          Value.symbol(fill_id),
          Value.symbol(order_id),
          Value.symbol(symbol),
          Value.record(Value.symbol(Atom.to_string(side)), []),
          Value.integer(price),
          Value.integer(qty),
          Value.symbol(counterparty),
          Value.integer(timestamp),
          Value.integer(fill_seq)
        ]
      )

    matcher = %{
      matcher
      | next_fill_id: matcher.next_fill_id + 1,
        next_fill_seq: matcher.next_fill_seq + 1,
        total_fills: matcher.total_fills + 1,
        total_volume: matcher.total_volume + qty
    }

    {matcher, fill_value}
  end

  # Internal: Insert into list sorted by seq (first element of tuple)
  defp insert_sorted_by_seq([], entry), do: [entry]

  defp insert_sorted_by_seq(
         [{existing_seq, _, _, _, _} = head | tail] = _list,
         {seq, _, _, _, _} = entry
       ) do
    if seq < existing_seq do
      [entry, head | tail]
    else
      [head | insert_sorted_by_seq(tail, entry)]
    end
  end

  # Internal: Execute matches against matchable price levels
  defp execute_matches(matcher, _side, _order, _handle, [], remaining_qty, fills) do
    {matcher, fills, remaining_qty}
  end

  defp execute_matches(matcher, _side, _order, _handle, _prices, 0, fills) do
    {matcher, fills, 0}
  end

  defp execute_matches(matcher, side, order, handle, [price | rest_prices], remaining_qty, fills) do
    opposite_field =
      case side do
        :bid -> :asks
        :ask -> :bids
      end

    opposite_map = Map.get(matcher, opposite_field)
    level = Map.get(opposite_map, price, [])

    {matcher, level, fills, remaining_qty} =
      match_at_level(matcher, side, order, handle, price, level, remaining_qty, fills)

    # Update the price level
    opposite_map =
      if level == [] do
        Map.delete(opposite_map, price)
      else
        Map.put(opposite_map, price, level)
      end

    matcher = Map.put(matcher, opposite_field, opposite_map)

    if remaining_qty > 0 do
      execute_matches(matcher, side, order, handle, rest_prices, remaining_qty, fills)
    else
      {matcher, fills, 0}
    end
  end

  # Internal: Match at a single price level
  defp match_at_level(matcher, _side, _order, _handle, _price, [], remaining_qty, fills) do
    {matcher, [], fills, remaining_qty}
  end

  defp match_at_level(matcher, _side, _order, _handle, _price, level, 0, fills) do
    {matcher, level, fills, 0}
  end

  defp match_at_level(
         matcher,
         side,
         order,
         _handle,
         price,
         [{seq, opposite_id, opposite_qty, opposite_trader, opposite_handle} | rest],
         remaining_qty,
         fills
       ) do
    fill_qty = min(remaining_qty, opposite_qty)
    opposite_fully_filled = fill_qty == opposite_qty

    # Create fill record with opposite order info
    # Fill format: {bid_info, ask_info, price, qty, opposite_fill_info}
    # opposite_fill_info: %{handle: h, fully_filled: bool, remaining_qty: int, side: atom, original_order: map}
    opposite_fill_info = %{
      handle: opposite_handle,
      fully_filled: opposite_fully_filled,
      remaining_qty: if(opposite_fully_filled, do: 0, else: opposite_qty - fill_qty),
      side: opposite_side(side),
      original_order: %{
        id: opposite_id,
        price: price,
        qty: opposite_qty,
        trader: opposite_trader,
        seq: seq
      }
    }

    fill =
      case side do
        :bid ->
          # order is the bid, opposite is the ask
          {%{id: order.id, trader: order.trader},
           %{id: opposite_id, trader: opposite_trader, handle: opposite_handle}, price, fill_qty,
           opposite_fill_info}

        :ask ->
          # order is the ask, opposite is the bid
          {%{id: opposite_id, trader: opposite_trader, handle: opposite_handle},
           %{id: order.id, trader: order.trader}, price, fill_qty, opposite_fill_info}
      end

    fills = [fill | fills]

    # Remove opposite order from handle_index
    matcher = %{matcher | handle_index: Map.delete(matcher.handle_index, opposite_handle)}

    cond do
      fill_qty == opposite_qty ->
        # Opposite order fully filled, continue with rest of level
        match_at_level(matcher, side, order, nil, price, rest, remaining_qty - fill_qty, fills)

      fill_qty < opposite_qty ->
        # Opposite order partially filled, update qty
        new_entry = {seq, opposite_id, opposite_qty - fill_qty, opposite_trader, opposite_handle}

        # Re-add to handle index with same info
        matcher = %{
          matcher
          | handle_index:
              Map.put(matcher.handle_index, opposite_handle, {
                opposite_side(side),
                price,
                opposite_id
              })
        }

        # remaining_qty is now 0
        {matcher, [new_entry | rest], fills, 0}
    end
  end

  defp opposite_side(:bid), do: :ask
  defp opposite_side(:ask), do: :bid
end

# Entity Protocol Implementation
defimpl Absynthe.Core.Entity, for: Absynthe.Examples.Stress.Entities.Matcher do
  alias Absynthe.Examples.Stress.Entities.Matcher
  alias Absynthe.Preserves.Value
  alias Absynthe.Protocol.Event
  alias Absynthe.Core.Turn

  def on_publish(matcher, assertion, handle, turn) do
    case Matcher.parse_order(assertion) do
      {:ok, order} ->
        if order.symbol == matcher.symbol do
          process_new_order(matcher, order, handle, turn)
        else
          {matcher, turn}
        end

      :error ->
        {matcher, turn}
    end
  end

  def on_retract(matcher, handle, turn) do
    # Order was retracted externally (cancelled), just remove from our book
    matcher = Matcher.remove_order(matcher, handle)
    {matcher, turn}
  end

  def on_message(matcher, _message, turn) do
    {matcher, turn}
  end

  def on_sync(matcher, peer_ref, turn) do
    action = Event.message(peer_ref, {:symbol, "synced"})
    turn = Turn.add_action(turn, action)
    {matcher, turn}
  end

  # Process a new order: try to match, then add remainder to book
  defp process_new_order(matcher, order, handle, turn) do
    # Try to match against opposite side
    {matcher, fills, remaining_qty} = Matcher.try_match(matcher, order.side, order, handle)

    # Send fill messages and retract filled orders
    {matcher, turn} = process_fills(matcher, order, handle, fills, turn)

    # Handle remaining quantity
    cond do
      remaining_qty == 0 ->
        # Fully filled - retract the incoming order assertion
        turn =
          if matcher.dataspace_ref do
            Turn.add_action(turn, Event.retract(matcher.dataspace_ref, handle))
          else
            turn
          end

        {matcher, turn}

      remaining_qty < order.qty ->
        # Partially filled - retract and re-assert with reduced qty
        remaining_order = %{order | qty: remaining_qty}

        turn =
          if matcher.dataspace_ref do
            # Retract old assertion
            turn = Turn.add_action(turn, Event.retract(matcher.dataspace_ref, handle))
            # Re-assert with reduced qty (use new handle)
            new_handle = %Absynthe.Assertions.Handle{id: {:partial, handle.id, remaining_qty}}
            new_assertion = make_order_assertion(remaining_order)
            Turn.add_action(turn, Event.assert(matcher.dataspace_ref, new_assertion, new_handle))
          else
            turn
          end

        # Add to internal book with original handle (since we track internally)
        matcher = Matcher.add_order(matcher, order.side, remaining_order, handle)
        {matcher, turn}

      true ->
        # No fills - add full order to book
        matcher = Matcher.add_order(matcher, order.side, order, handle)
        {matcher, turn}
    end
  end

  # Create an Order assertion value
  defp make_order_assertion(order) do
    side_record = Value.record(Value.symbol(Atom.to_string(order.side)), [])

    Value.record(
      Value.symbol("Order"),
      [
        Value.symbol(order.id),
        Value.symbol(order.symbol),
        side_record,
        Value.integer(order.price),
        Value.integer(order.qty),
        Value.symbol(order.trader),
        Value.integer(order.seq)
      ]
    )
  end

  # Process fill results: send Fill messages and retract/update filled orders
  defp process_fills(matcher, order, _incoming_handle, fills, turn) do
    timestamp = System.monotonic_time(:nanosecond)

    Enum.reduce(fills, {matcher, turn}, fn {bid_info, ask_info, price, qty, opposite_fill_info},
                                           {m, t} ->
      # Generate fill IDs
      fill_id_bid = "fill-#{m.next_fill_id}"
      fill_id_ask = "fill-#{m.next_fill_id + 1}"

      # Create fill messages for both sides
      {m, fill_bid} =
        Matcher.make_fill(
          m,
          fill_id_bid,
          bid_info.id,
          order.symbol,
          :bid,
          price,
          qty,
          ask_info.trader,
          timestamp
        )

      {m, fill_ask} =
        Matcher.make_fill(
          m,
          fill_id_ask,
          ask_info.id,
          order.symbol,
          :ask,
          price,
          qty,
          bid_info.trader,
          timestamp
        )

      # Send fill messages to dataspace (if configured)
      t =
        if m.dataspace_ref do
          t = Turn.add_action(t, Event.message(m.dataspace_ref, fill_bid))
          Turn.add_action(t, Event.message(m.dataspace_ref, fill_ask))
        else
          t
        end

      # Handle the opposing (resting) order
      t =
        if m.dataspace_ref do
          %{
            handle: opposite_handle,
            fully_filled: fully_filled,
            remaining_qty: remaining_qty,
            side: opposite_side,
            original_order: orig_order
          } = opposite_fill_info

          # Always retract the old assertion
          t = Turn.add_action(t, Event.retract(m.dataspace_ref, opposite_handle))

          # If partially filled, re-assert with reduced qty
          if not fully_filled and remaining_qty > 0 do
            new_handle = %Absynthe.Assertions.Handle{
              id: {:partial, opposite_handle.id, remaining_qty}
            }

            updated_order = %{
              id: orig_order.id,
              symbol: order.symbol,
              side: opposite_side,
              price: orig_order.price,
              qty: remaining_qty,
              trader: orig_order.trader,
              seq: orig_order.seq
            }

            new_assertion = make_order_assertion(updated_order)
            Turn.add_action(t, Event.assert(m.dataspace_ref, new_assertion, new_handle))
          else
            t
          end
        else
          t
        end

      {m, t}
    end)
  end
end
