defmodule Absynthe.FlowControl.Account do
  @moduledoc """
  Credit-based flow control account for tracking and limiting work debt.

  An Account tracks pending work (debt) associated with a source (e.g., a relay
  connection, actor, or entity). When a source submits work, it "borrows" from
  its account. When that work is processed, the debt is "repaid."

  ## Overview

  The credit-based flow control system prevents fast producers from overwhelming
  slow consumers by:

  1. Assigning each source an Account with a configurable debt limit
  2. Tracking outstanding work as "loaned items" in the account
  3. Pausing sources when their debt exceeds the high-water mark
  4. Resuming sources when debt falls below the low-water mark

  This implements a form of backpressure that works across process and network
  boundaries, charging work back to its origin.

  ## Hysteresis

  To avoid rapid pause/resume oscillation, the account uses hysteresis:
  - Pause when debt >= high_water_mark
  - Resume when debt < low_water_mark

  The gap between high and low water marks provides a buffer zone.

  ## Integration Points

  - **Relay**: Each connection gets an account. Incoming turn events borrow
    debt; completed turn processing repays. When debt is high, the relay
    stops reading from the socket (TCP backpressure propagates to the sender).

  - **Actor**: Internal routing can optionally use accounts to prevent
    runaway actors from flooding dataspaces.

  ## Example

      # Create an account with 100 unit limit
      account = Account.new(limit: 100, high_water_mark: 80, low_water_mark: 40)

      # Check if we can accept more work
      if Account.can_borrow?(account, 10) do
        {loan, account} = Account.borrow(account, 10, ref: my_work_id)
        # Submit work...
        # Later, when work is processed:
        account = Account.repay(account, loan)
      end

  ## Telemetry

  The account emits telemetry events:
  - `[:absynthe, :flow_control, :account, :borrow]` - debt increased
  - `[:absynthe, :flow_control, :account, :repay]` - debt decreased
  - `[:absynthe, :flow_control, :account, :pause]` - debt exceeded high water
  - `[:absynthe, :flow_control, :account, :resume]` - debt fell below low water
  """

  alias Absynthe.FlowControl.LoanedItem

  @type t :: %__MODULE__{
          id: term(),
          debt: non_neg_integer(),
          limit: non_neg_integer(),
          high_water_mark: non_neg_integer(),
          low_water_mark: non_neg_integer(),
          paused: boolean(),
          loaned_items: %{reference() => LoanedItem.t()},
          pause_callback: (t() -> any()) | nil,
          resume_callback: (t() -> any()) | nil
        }

  defstruct [
    :id,
    debt: 0,
    limit: 1000,
    high_water_mark: 800,
    low_water_mark: 400,
    paused: false,
    loaned_items: %{},
    pause_callback: nil,
    resume_callback: nil
  ]

  @doc """
  Creates a new account with the given options.

  ## Options

  - `:id` - Unique identifier for this account (for logging/metrics)
  - `:limit` - Maximum debt allowed (default: 1000)
  - `:high_water_mark` - Debt level that triggers pause (default: 80% of limit)
  - `:low_water_mark` - Debt level that allows resume (default: 40% of limit)
  - `:pause_callback` - Function called when account is paused
  - `:resume_callback` - Function called when account is resumed

  ## Examples

      Account.new(id: {:relay, connection_id}, limit: 100)
      Account.new(id: :my_actor, limit: 500, pause_callback: &my_pause_fn/1)
  """
  @spec new(Keyword.t()) :: t()
  def new(opts \\ []) do
    limit = Keyword.get(opts, :limit, 1000)
    high = Keyword.get(opts, :high_water_mark, div(limit * 80, 100))
    low = Keyword.get(opts, :low_water_mark, div(limit * 40, 100))

    %__MODULE__{
      id: Keyword.get(opts, :id),
      limit: limit,
      high_water_mark: high,
      low_water_mark: low,
      pause_callback: Keyword.get(opts, :pause_callback),
      resume_callback: Keyword.get(opts, :resume_callback)
    }
  end

  @doc """
  Returns the current debt level.
  """
  @spec debt(t()) :: non_neg_integer()
  def debt(%__MODULE__{debt: debt}), do: debt

  @doc """
  Returns true if the account is currently paused.
  """
  @spec paused?(t()) :: boolean()
  def paused?(%__MODULE__{paused: paused}), do: paused

  @doc """
  Returns true if the account can borrow the given amount without exceeding the limit.
  """
  @spec can_borrow?(t(), non_neg_integer()) :: boolean()
  def can_borrow?(%__MODULE__{debt: debt, limit: limit}, amount) do
    debt + amount <= limit
  end

  @doc """
  Borrows from the account, creating a loaned item.

  Returns `{loaned_item, updated_account}`.

  If the debt exceeds the high water mark after borrowing, the account
  is paused and the pause_callback is invoked.

  ## Options

  - `:cost` - The cost of this work (default: 1)
  - `:ref` - A reference to associate with this loan (for debugging)

  ## Examples

      {loan, account} = Account.borrow(account, cost: 10)
      # Process work...
      account = Account.repay(account, loan)

  ## Returns

  `{:ok, loaned_item, account}` on success.
  `{:error, :over_limit}` if borrowing would exceed the limit.
  """
  @spec borrow(t(), Keyword.t()) :: {:ok, LoanedItem.t(), t()} | {:error, :over_limit}
  def borrow(account, opts \\ []) do
    cost = Keyword.get(opts, :cost, 1)
    ref = Keyword.get(opts, :ref)

    if account.debt + cost > account.limit do
      {:error, :over_limit}
    else
      loan_id = make_ref()

      loaned_item = %LoanedItem{
        id: loan_id,
        account_id: account.id,
        cost: cost,
        ref: ref,
        borrowed_at: System.monotonic_time(:microsecond)
      }

      new_debt = account.debt + cost
      loaned_items = Map.put(account.loaned_items, loan_id, loaned_item)

      account = %{account | debt: new_debt, loaned_items: loaned_items}

      # Emit telemetry
      emit_telemetry(:borrow, account, %{cost: cost, new_debt: new_debt})

      # Check if we should pause
      account =
        if not account.paused and new_debt >= account.high_water_mark do
          emit_telemetry(:pause, account, %{debt: new_debt, high_water: account.high_water_mark})

          account = %{account | paused: true}

          if account.pause_callback do
            account.pause_callback.(account)
          end

          account
        else
          account
        end

      {:ok, loaned_item, account}
    end
  end

  @doc """
  Forcibly borrows from the account, even if it exceeds the limit.

  This is useful for work that has already been accepted and must be tracked,
  even if the account is over limit. The account will be paused if not already.

  Returns `{loaned_item, account}`.
  """
  @spec force_borrow(t(), Keyword.t()) :: {LoanedItem.t(), t()}
  def force_borrow(account, opts \\ []) do
    cost = Keyword.get(opts, :cost, 1)
    ref = Keyword.get(opts, :ref)

    loan_id = make_ref()

    loaned_item = %LoanedItem{
      id: loan_id,
      account_id: account.id,
      cost: cost,
      ref: ref,
      borrowed_at: System.monotonic_time(:microsecond)
    }

    new_debt = account.debt + cost
    loaned_items = Map.put(account.loaned_items, loan_id, loaned_item)

    account = %{account | debt: new_debt, loaned_items: loaned_items}

    emit_telemetry(:borrow, account, %{cost: cost, new_debt: new_debt, forced: true})

    # Pause if over high water mark
    account =
      if not account.paused and new_debt >= account.high_water_mark do
        emit_telemetry(:pause, account, %{debt: new_debt, high_water: account.high_water_mark})

        account = %{account | paused: true}

        if account.pause_callback do
          account.pause_callback.(account)
        end

        account
      else
        account
      end

    {loaned_item, account}
  end

  @doc """
  Repays a loaned item, reducing the account's debt.

  If the account was paused and debt falls below the low water mark,
  the account is resumed and the resume_callback is invoked.
  """
  @spec repay(t(), LoanedItem.t()) :: t()
  def repay(account, %LoanedItem{id: loan_id}) do
    case Map.pop(account.loaned_items, loan_id) do
      {nil, _loaned_items} ->
        # Loan not found - already repaid or invalid
        account

      {%LoanedItem{cost: cost}, loaned_items} ->
        # Use the cost from the stored loan, not the passed-in struct
        # This prevents manipulation and ensures accurate debt tracking
        new_debt = max(0, account.debt - cost)
        account = %{account | debt: new_debt, loaned_items: loaned_items}

        emit_telemetry(:repay, account, %{cost: cost, new_debt: new_debt})

        # Check if we should resume
        if account.paused and new_debt < account.low_water_mark do
          emit_telemetry(:resume, account, %{debt: new_debt, low_water: account.low_water_mark})

          account = %{account | paused: false}

          if account.resume_callback do
            account.resume_callback.(account)
          end

          account
        else
          account
        end
    end
  end

  @doc """
  Returns statistics about the account.
  """
  @spec stats(t()) :: map()
  def stats(%__MODULE__{} = account) do
    %{
      id: account.id,
      debt: account.debt,
      limit: account.limit,
      high_water_mark: account.high_water_mark,
      low_water_mark: account.low_water_mark,
      paused: account.paused,
      outstanding_loans: map_size(account.loaned_items),
      utilization: if(account.limit > 0, do: account.debt / account.limit * 100, else: 0)
    }
  end

  # Emit telemetry events
  defp emit_telemetry(event, account, measurements) do
    :telemetry.execute(
      [:absynthe, :flow_control, :account, event],
      measurements,
      %{account_id: account.id}
    )
  end
end
