defmodule Absynthe.FlowControl do
  @moduledoc """
  Credit-based flow control for Absynthe actors and relays.

  This module provides a credit-based backpressure system that prevents fast
  producers from overwhelming slow consumers. Each source (relay connection,
  actor, etc.) has an Account that tracks pending work as "debt." When debt
  exceeds a threshold, the source is paused until processing catches up.

  ## Architecture

  The flow control system consists of:

  - `Account`: Tracks debt for a source with pause/resume thresholds
  - `LoanedItem`: Represents a unit of borrowed work to be repaid

  Accounts are stored in process state (not in a shared registry), so each
  process manages its own flow control independently.

  ## Usage Patterns

  ### Per-Process Accounts (Simple)

  For processes that manage their own flow control:

      # Store account in process state
      account = FlowControl.Account.new(id: :my_source, limit: 100)

      # When accepting work
      case FlowControl.Account.borrow(account, cost: 1) do
        {:ok, loan, account} ->
          # Submit work, store loan for later repayment
          {:ok, account, loan}
        {:error, :over_limit} ->
          # Reject work or queue it
          {:error, :overloaded}
      end

      # When work completes
      account = FlowControl.Account.repay(account, loan)

  ### Relay Integration

  Relays use flow control to apply TCP-level backpressure:

      # In relay init
      account = FlowControl.Account.new(
        id: {:relay, connection_id},
        limit: 1000,
        pause_callback: fn _ -> :inet.setopts(socket, active: false) end,
        resume_callback: fn _ -> :inet.setopts(socket, active: :once) end
      )

      # When receiving packets
      {:ok, loan, account} = FlowControl.Account.force_borrow(account, cost: packet_cost)
      # Process packet...

      # When turn processing completes
      account = FlowControl.Account.repay(account, loan)

  ## Cost Calculation

  Work cost should reflect processing burden. Helper functions are provided:

      FlowControl.event_cost(event)  # Cost based on event type and payload

  ## Telemetry

  The flow control system emits telemetry events:

  - `[:absynthe, :flow_control, :account, :borrow]` - Work accepted
  - `[:absynthe, :flow_control, :account, :repay]` - Work completed
  - `[:absynthe, :flow_control, :account, :pause]` - Source paused
  - `[:absynthe, :flow_control, :account, :resume]` - Source resumed

  Attach handlers to monitor system health:

      :telemetry.attach("flow-control-logger",
        [:absynthe, :flow_control, :account, :pause],
        &log_pause/4, nil)

  ## Defaults

  Default values are hardcoded in `Account.new/1`:

  - `limit`: 1000
  - `high_water_mark`: 80% of limit (800)
  - `low_water_mark`: 40% of limit (400)

  Pass these options explicitly to `Account.new/1` or `FlowControl.new_account/1`
  to override the defaults.

  ## See Also

  - `Absynthe.FlowControl.Account` - Per-source debt tracking
  - `Absynthe.FlowControl.LoanedItem` - Work debt representation
  """

  alias Absynthe.FlowControl.Account
  alias Absynthe.Protocol.Event

  @doc """
  Creates a new flow control account with sensible defaults.

  See `Absynthe.FlowControl.Account.new/1` for options.
  """
  @spec new_account(Keyword.t()) :: Account.t()
  defdelegate new_account(opts \\ []), to: Account, as: :new

  @doc """
  Calculates the cost of an event for flow control purposes.

  The cost reflects the potential processing burden:

  - `Assert`: 1 base + number of embedded refs (fan-out potential)
  - `Retract`: 1 (simple lookup and removal)
  - `Message`: 1 base + payload size factor
  - `Sync`: 1 (coordination overhead)

  ## Examples

      cost = FlowControl.event_cost(%Assert{assertion: assertion})
  """
  @spec event_cost(Event.t()) :: non_neg_integer()
  def event_cost(%Event.Assert{assertion: assertion}) do
    # Base cost + refs that could cause fan-out
    1 + count_refs(assertion)
  end

  def event_cost(%Event.Retract{}) do
    # Simple retraction
    1
  end

  def event_cost(%Event.Message{body: body}) do
    # Base cost + size factor for large payloads
    1 + size_cost(body)
  end

  def event_cost(%Event.Sync{}) do
    # Coordination overhead
    1
  end

  def event_cost(_unknown) do
    # Unknown event type, default cost
    1
  end

  @doc """
  Calculates the cost of a raw Preserves value.

  Useful for calculating costs of wire-format data before full decoding.
  """
  @spec value_cost(term()) :: non_neg_integer()
  def value_cost(value) do
    1 + count_refs(value) + size_cost(value)
  end

  # Count refs embedded in a value (potential fan-out)
  # Refs can appear as:
  # - Direct %Ref{} structs
  # - {:embedded, %Ref{}} in Preserves values
  defp count_refs(%Absynthe.Core.Ref{}), do: 1

  defp count_refs({:embedded, %Absynthe.Core.Ref{}}), do: 1
  defp count_refs({:embedded, _}), do: 0

  defp count_refs({:record, {label, fields}}) when is_list(fields) do
    count_refs(label) + Enum.sum(Enum.map(fields, &count_refs/1))
  end

  defp count_refs(list) when is_list(list) do
    Enum.sum(Enum.map(list, &count_refs/1))
  end

  defp count_refs(%{} = map) do
    Enum.sum(
      for {k, v} <- map do
        count_refs(k) + count_refs(v)
      end
    )
  end

  defp count_refs({:set, members}) when is_list(members) do
    Enum.sum(Enum.map(members, &count_refs/1))
  end

  defp count_refs(_), do: 0

  # Size-based cost factor (for large payloads)
  # Returns 0 for small values, scales up for larger ones
  defp size_cost(value) do
    size = estimate_size(value)

    cond do
      size < 1_000 -> 0
      size < 10_000 -> 1
      size < 100_000 -> 2
      true -> 3
    end
  end

  # Rough size estimate for flow control (not exact)
  defp estimate_size(value) when is_binary(value), do: byte_size(value)
  defp estimate_size(value) when is_integer(value), do: 8
  defp estimate_size(value) when is_float(value), do: 8
  defp estimate_size(value) when is_atom(value), do: 8
  defp estimate_size(value) when is_boolean(value), do: 1

  defp estimate_size({:record, {label, fields}}) when is_list(fields) do
    estimate_size(label) + Enum.sum(Enum.map(fields, &estimate_size/1))
  end

  defp estimate_size(list) when is_list(list) do
    Enum.sum(Enum.map(list, &estimate_size/1))
  end

  defp estimate_size(%{} = map) do
    Enum.sum(
      for {k, v} <- map do
        estimate_size(k) + estimate_size(v)
      end
    )
  end

  defp estimate_size({:set, members}) when is_list(members) do
    Enum.sum(Enum.map(members, &estimate_size/1))
  end

  defp estimate_size({:bytes, b}) when is_binary(b), do: byte_size(b)
  defp estimate_size({:string, s}) when is_binary(s), do: byte_size(s)
  defp estimate_size({:symbol, s}) when is_binary(s), do: byte_size(s)

  defp estimate_size(_), do: 16
end
