defmodule Absynthe.FlowControl.LoanedItem do
  @moduledoc """
  Represents a unit of borrowed work in the credit-based flow control system.

  A LoanedItem is created when work is borrowed from an Account and must be
  repaid when that work is complete. This creates a direct link between
  submitted work and its processing, enabling accurate debt tracking.

  ## Fields

  - `:id` - Unique identifier for this loan (a reference)
  - `:account_id` - The account this loan belongs to
  - `:cost` - The debt units this loan represents
  - `:ref` - Optional application-specific reference (e.g., work item ID)
  - `:borrowed_at` - Monotonic timestamp when the loan was created

  ## Usage

  LoanedItems are typically passed alongside work through the system:

      # At the producer
      {:ok, loan, account} = Account.borrow(account, cost: event_cost(event))
      submit_work(work, loan)

      # At the consumer, after processing
      account = Account.repay(account, loan)

  ## Cost Calculation

  The cost of work should reflect its processing burden:

  - Simple messages: 1 unit
  - Assertions with embedded refs: 1 + count of refs (fan-out potential)
  - Large payloads: proportional to size

  This ensures that work with greater downstream impact carries proportional debt.
  """

  @type t :: %__MODULE__{
          id: reference(),
          account_id: term(),
          cost: non_neg_integer(),
          ref: term(),
          borrowed_at: integer()
        }

  defstruct [
    :id,
    :account_id,
    :ref,
    cost: 1,
    borrowed_at: 0
  ]

  @doc """
  Returns the cost of this loaned item.
  """
  @spec cost(t()) :: non_neg_integer()
  def cost(%__MODULE__{cost: c}), do: c

  @doc """
  Returns the age of this loan in microseconds.
  """
  @spec age_us(t()) :: non_neg_integer()
  def age_us(%__MODULE__{borrowed_at: borrowed_at}) do
    System.monotonic_time(:microsecond) - borrowed_at
  end

  @doc """
  Returns the age of this loan in milliseconds.
  """
  @spec age_ms(t()) :: non_neg_integer()
  def age_ms(loan), do: div(age_us(loan), 1000)
end
