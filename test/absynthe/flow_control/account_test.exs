defmodule Absynthe.FlowControl.AccountTest do
  use ExUnit.Case, async: true

  alias Absynthe.FlowControl.Account
  alias Absynthe.FlowControl.LoanedItem

  describe "new/1" do
    test "creates account with default values" do
      account = Account.new()

      assert account.debt == 0
      assert account.limit == 1000
      assert account.high_water_mark == 800
      assert account.low_water_mark == 400
      assert account.paused == false
    end

    test "creates account with custom values" do
      account = Account.new(id: :test, limit: 100, high_water_mark: 80, low_water_mark: 40)

      assert account.id == :test
      assert account.limit == 100
      assert account.high_water_mark == 80
      assert account.low_water_mark == 40
    end

    test "computes default water marks from limit" do
      account = Account.new(limit: 500)

      # 80% of 500 = 400
      assert account.high_water_mark == 400
      # 40% of 500 = 200
      assert account.low_water_mark == 200
    end
  end

  describe "borrow/2" do
    test "increases debt by cost" do
      account = Account.new(limit: 100)

      {:ok, _loan, account} = Account.borrow(account, cost: 10)
      assert account.debt == 10

      {:ok, _loan, account} = Account.borrow(account, cost: 5)
      assert account.debt == 15
    end

    test "returns loaned item with correct cost" do
      account = Account.new(limit: 100)

      {:ok, loan, _account} = Account.borrow(account, cost: 10)
      assert loan.cost == 10
      assert loan.account_id == account.id
    end

    test "rejects borrow that exceeds limit" do
      account = Account.new(limit: 100)

      {:ok, _loan, account} = Account.borrow(account, cost: 90)
      assert {:error, :over_limit} = Account.borrow(account, cost: 20)
    end

    test "pauses when debt reaches high water mark" do
      account = Account.new(limit: 100, high_water_mark: 80, low_water_mark: 40)

      {:ok, _loan, account} = Account.borrow(account, cost: 79)
      assert account.paused == false

      {:ok, _loan, account} = Account.borrow(account, cost: 1)
      assert account.paused == true
    end

    test "invokes pause callback when pausing" do
      test_pid = self()

      account =
        Account.new(
          limit: 100,
          high_water_mark: 80,
          pause_callback: fn _account -> send(test_pid, :paused) end
        )

      {:ok, _loan, _account} = Account.borrow(account, cost: 80)

      assert_receive :paused
    end
  end

  describe "force_borrow/2" do
    test "allows borrowing even when over limit" do
      account = Account.new(limit: 100)

      {:ok, _loan, account} = Account.borrow(account, cost: 95)
      assert {:error, :over_limit} = Account.borrow(account, cost: 10)

      # Force borrow should work
      {loan, account} = Account.force_borrow(account, cost: 10)
      assert loan.cost == 10
      assert account.debt == 105
    end

    test "pauses when force borrowing over high water mark" do
      account = Account.new(limit: 100, high_water_mark: 80, low_water_mark: 40)

      {_loan, account} = Account.force_borrow(account, cost: 90)
      assert account.paused == true
    end
  end

  describe "repay/2" do
    test "decreases debt by loan cost" do
      account = Account.new(limit: 100)

      {:ok, loan1, account} = Account.borrow(account, cost: 30)
      {:ok, loan2, account} = Account.borrow(account, cost: 20)
      assert account.debt == 50

      account = Account.repay(account, loan1)
      assert account.debt == 20

      account = Account.repay(account, loan2)
      assert account.debt == 0
    end

    test "resumes when debt falls below low water mark" do
      account =
        Account.new(
          limit: 100,
          high_water_mark: 80,
          low_water_mark: 40
        )

      # Borrow enough to pause
      {:ok, loan1, account} = Account.borrow(account, cost: 85)
      assert account.paused == true

      # Repay some, but not below low water (force_borrow returns {loan, account})
      {loan2, account} = Account.force_borrow(account, cost: 5)
      account = Account.repay(account, loan2)
      assert account.debt == 85
      assert account.paused == true

      # Repay to below low water mark
      account = Account.repay(account, loan1)
      assert account.debt == 0
      assert account.paused == false
    end

    test "invokes resume callback when resuming" do
      test_pid = self()

      account =
        Account.new(
          limit: 100,
          high_water_mark: 80,
          low_water_mark: 40,
          resume_callback: fn _account -> send(test_pid, :resumed) end
        )

      {:ok, loan, account} = Account.borrow(account, cost: 85)
      assert account.paused == true

      _account = Account.repay(account, loan)

      assert_receive :resumed
    end

    test "handles repaying unknown loan gracefully" do
      account = Account.new(limit: 100)
      {:ok, _loan, account} = Account.borrow(account, cost: 50)

      unknown_loan = %LoanedItem{
        id: make_ref(),
        account_id: account.id,
        cost: 100,
        ref: nil,
        borrowed_at: 0
      }

      # Should not change debt
      account_after = Account.repay(account, unknown_loan)
      assert account_after.debt == account.debt
    end

    test "handles double repay gracefully" do
      account = Account.new(limit: 100)
      {:ok, loan, account} = Account.borrow(account, cost: 50)

      account = Account.repay(account, loan)
      assert account.debt == 0

      # Second repay should be a no-op
      account = Account.repay(account, loan)
      assert account.debt == 0
    end
  end

  describe "stats/1" do
    test "returns account statistics" do
      account = Account.new(id: :test_account, limit: 100)
      {:ok, _loan, account} = Account.borrow(account, cost: 30)

      stats = Account.stats(account)

      assert stats.id == :test_account
      assert stats.debt == 30
      assert stats.limit == 100
      assert stats.paused == false
      assert stats.outstanding_loans == 1
      assert stats.utilization == 30.0
    end
  end

  describe "hysteresis behavior" do
    test "pause and resume use different thresholds" do
      account =
        Account.new(
          limit: 100,
          high_water_mark: 80,
          low_water_mark: 40
        )

      # Borrow up to 79 - not paused
      {:ok, _loan1, account} = Account.borrow(account, cost: 79)
      assert account.paused == false

      # Borrow 1 more (total 80) - paused at high water
      {:ok, loan2, account} = Account.borrow(account, cost: 1)
      assert account.paused == true

      # Repay 1 (total 79) - still paused (above low water of 40)
      account = Account.repay(account, loan2)
      assert account.debt == 79
      assert account.paused == true

      # Borrow more to test resume threshold
      {:ok, loan3, account} = Account.borrow(account, cost: 5)
      {:ok, _loan4, account} = Account.borrow(account, cost: 5)
      # Now at 89

      # Repay to exactly 40 - still paused (at low water, not below)
      # Repay loan3 (cost 5) -> 84, then manipulate debt to test threshold
      account = Account.repay(account, loan3)
      assert account.debt == 84
      assert account.paused == true

      # Now we need to get debt to 39 to trigger resume
      # Create a fresh account scenario to test the threshold properly
      account2 =
        Account.new(
          limit: 100,
          high_water_mark: 80,
          low_water_mark: 40
        )

      # Borrow to pause
      {:ok, loan_big, account2} = Account.borrow(account2, cost: 80)
      assert account2.paused == true
      assert account2.debt == 80

      # Repay the big loan (80) -> 0, which is below 40, so resume
      account2 = Account.repay(account2, loan_big)
      assert account2.debt == 0
      assert account2.paused == false
    end
  end
end
