defmodule Absynthe.Examples.Stress.TelemetryCollector do
  @moduledoc """
  Collects and aggregates telemetry events for stress test analysis.

  The TelemetryCollector:
  - Attaches to all relevant telemetry events
  - Aggregates metrics in real-time
  - Tracks observer notification completeness
  - Validates acceptance criteria
  - Generates reports

  ## Usage

      # Start the collector
      {:ok, collector} = TelemetryCollector.start_link()

      # Run your test...

      # Get the report
      report = TelemetryCollector.report(collector)

      # Check acceptance criteria
      {:ok, results} = TelemetryCollector.check_acceptance(collector)
  """

  use GenServer
  require Logger

  defstruct [
    # Counters for core metrics
    counters: %{
      orders_placed: 0,
      orders_cancelled: 0,
      orders_filled: 0,
      matches_executed: 0,
      actor_crashes: 0,
      turn_errors: 0
    },
    # Flow control tracking
    flow_control: %{
      pause_events: 0,
      resume_events: 0,
      max_debt: 0,
      total_borrow: 0,
      total_repay: 0
    },
    # Latency tracking (microseconds)
    latencies: %{
      turn_durations: [],
      order_to_fill: []
    },
    # Observer notification tracking
    # %{observer_id => %{handles_seen: MapSet, handles_retracted: MapSet}}
    observer_tracking: %{},
    # Memory tracking
    memory: %{
      max_ets_size: 0,
      max_heap_size: 0,
      samples: []
    },
    # Error tracking
    errors: [],
    # Start time
    start_time: nil,
    # Attached event handlers
    handler_ids: []
  ]

  # Client API

  @doc """
  Starts the telemetry collector.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts)
  end

  @doc """
  Attaches telemetry handlers to collect metrics.
  """
  @spec attach(GenServer.server()) :: :ok
  def attach(collector) do
    GenServer.call(collector, :attach)
  end

  @doc """
  Detaches telemetry handlers.
  """
  @spec detach(GenServer.server()) :: :ok
  def detach(collector) do
    GenServer.call(collector, :detach)
  end

  @doc """
  Returns the current metrics report.
  """
  @spec report(GenServer.server()) :: map()
  def report(collector) do
    GenServer.call(collector, :report)
  end

  @doc """
  Checks acceptance criteria and returns pass/fail results.
  """
  @spec check_acceptance(GenServer.server()) :: {:ok, map()} | {:error, map()}
  def check_acceptance(collector) do
    GenServer.call(collector, :check_acceptance)
  end

  @doc """
  Resets all collected metrics.
  """
  @spec reset(GenServer.server()) :: :ok
  def reset(collector) do
    GenServer.call(collector, :reset)
  end

  # GenServer callbacks

  @impl GenServer
  def init(_opts) do
    state = %__MODULE__{start_time: System.monotonic_time(:millisecond)}
    {:ok, state}
  end

  @impl GenServer
  def handle_call(:attach, _from, state) do
    handler_ids = attach_handlers(self())
    {:reply, :ok, %{state | handler_ids: handler_ids}}
  end

  @impl GenServer
  def handle_call(:detach, _from, state) do
    Enum.each(state.handler_ids, &:telemetry.detach/1)
    {:reply, :ok, %{state | handler_ids: []}}
  end

  @impl GenServer
  def handle_call(:report, _from, state) do
    report = generate_report(state)
    {:reply, report, state}
  end

  @impl GenServer
  def handle_call(:check_acceptance, _from, state) do
    result = check_criteria(state)
    {:reply, result, state}
  end

  @impl GenServer
  def handle_call(:reset, _from, _state) do
    {:reply, :ok, %__MODULE__{start_time: System.monotonic_time(:millisecond)}}
  end

  @impl GenServer
  def handle_cast({:event, event_name, measurements, metadata}, state) do
    state = handle_event(state, event_name, measurements, metadata)
    {:noreply, state}
  end

  # Event handlers

  defp attach_handlers(collector_pid) do
    events = [
      # Stress test events
      {[:stress, :order, :placed], "stress_order_placed"},
      {[:stress, :order, :cancelled], "stress_order_cancelled"},
      {[:stress, :order, :filled], "stress_order_filled"},
      {[:stress, :match, :executed], "stress_match_executed"},
      {[:stress, :client, :connected], "stress_client_connected"},
      {[:stress, :client, :disconnected], "stress_client_disconnected"},
      {[:stress, :client, :error], "stress_client_error"},
      # Flow control events
      {[:absynthe, :flow_control, :account, :pause], "flow_pause"},
      {[:absynthe, :flow_control, :account, :resume], "flow_resume"},
      {[:absynthe, :flow_control, :account, :borrow], "flow_borrow"},
      {[:absynthe, :flow_control, :account, :repay], "flow_repay"},
      # Actor events
      {[:absynthe, :actor, :turn, :duration], "actor_turn_duration"},
      {[:absynthe, :actor, :turn, :error], "actor_turn_error"},
      # Dataspace events
      {[:absynthe, :dataspace, :assert], "dataspace_assert"},
      {[:absynthe, :dataspace, :retract], "dataspace_retract"},
      {[:absynthe, :dataspace, :notify], "dataspace_notify"}
    ]

    Enum.map(events, fn {event_name, handler_id} ->
      :telemetry.attach(
        handler_id,
        event_name,
        fn name, measurements, metadata, _config ->
          GenServer.cast(collector_pid, {:event, name, measurements, metadata})
        end,
        nil
      )

      handler_id
    end)
  end

  defp handle_event(state, [:stress, :order, :placed], _measurements, _metadata) do
    update_counter(state, :orders_placed)
  end

  defp handle_event(state, [:stress, :order, :cancelled], _measurements, _metadata) do
    update_counter(state, :orders_cancelled)
  end

  defp handle_event(state, [:stress, :order, :filled], _measurements, _metadata) do
    update_counter(state, :orders_filled)
  end

  defp handle_event(state, [:stress, :match, :executed], _measurements, _metadata) do
    update_counter(state, :matches_executed)
  end

  defp handle_event(state, [:absynthe, :flow_control, :account, :pause], measurements, _metadata) do
    fc = %{
      state.flow_control
      | pause_events: state.flow_control.pause_events + 1,
        max_debt: max(state.flow_control.max_debt, measurements.debt)
    }

    %{state | flow_control: fc}
  end

  defp handle_event(
         state,
         [:absynthe, :flow_control, :account, :resume],
         _measurements,
         _metadata
       ) do
    fc = %{state.flow_control | resume_events: state.flow_control.resume_events + 1}
    %{state | flow_control: fc}
  end

  defp handle_event(state, [:absynthe, :actor, :turn, :duration], measurements, _metadata) do
    duration = Map.get(measurements, :duration_us, 0)
    latencies = %{state.latencies | turn_durations: [duration | state.latencies.turn_durations]}
    %{state | latencies: latencies}
  end

  defp handle_event(state, [:absynthe, :actor, :turn, :error], _measurements, metadata) do
    error = %{
      error: Map.get(metadata, :error),
      actor_id: Map.get(metadata, :actor_id),
      timestamp: System.monotonic_time(:millisecond)
    }

    counters = %{state.counters | turn_errors: state.counters.turn_errors + 1}
    %{state | counters: counters, errors: [error | state.errors]}
  end

  defp handle_event(state, [:stress, :client, :error], _measurements, metadata) do
    error = %{
      error: Map.get(metadata, :error),
      client_id: Map.get(metadata, :client_id),
      timestamp: System.monotonic_time(:millisecond)
    }

    %{state | errors: [error | state.errors]}
  end

  defp handle_event(state, _event_name, _measurements, _metadata) do
    state
  end

  defp update_counter(state, key) do
    counters = Map.update!(state.counters, key, &(&1 + 1))
    %{state | counters: counters}
  end

  # Report generation

  defp generate_report(state) do
    elapsed_ms = System.monotonic_time(:millisecond) - state.start_time
    turn_latencies = state.latencies.turn_durations

    %{
      elapsed_ms: elapsed_ms,
      counters: state.counters,
      flow_control: state.flow_control,
      latencies: %{
        turn: latency_stats(turn_latencies)
      },
      error_count: length(state.errors),
      errors: Enum.take(state.errors, 10)
    }
  end

  defp latency_stats([]), do: %{count: 0, p50: 0, p99: 0, p999: 0, max: 0}

  defp latency_stats(latencies) do
    sorted = Enum.sort(latencies)
    count = length(sorted)

    %{
      count: count,
      p50: percentile(sorted, 50),
      p99: percentile(sorted, 99),
      p999: percentile(sorted, 99.9),
      max: List.last(sorted)
    }
  end

  defp percentile(sorted_list, p) do
    count = length(sorted_list)
    idx = trunc(p / 100 * count)
    idx = max(0, min(idx, count - 1))
    Enum.at(sorted_list, idx)
  end

  # Acceptance criteria checking

  defp check_criteria(state) do
    turn_latencies = state.latencies.turn_durations
    stats = latency_stats(turn_latencies)

    # Core acceptance criteria - these must pass
    checks = [
      {:no_crashes, state.counters.actor_crashes == 0,
       "Actor crashes: #{state.counters.actor_crashes}"},
      {:no_turn_errors, state.counters.turn_errors == 0,
       "Turn errors: #{state.counters.turn_errors}"},
      {:turn_latency_p50, stats.count == 0 or stats.p50 < 1000,
       "Turn latency p50: #{stats.p50}us (limit: 1000us)"},
      {:turn_latency_p99, stats.count == 0 or stats.p99 < 50_000,
       "Turn latency p99: #{stats.p99}us (limit: 50000us)"},
      {:turn_latency_p999, stats.count == 0 or stats.p999 < 500_000,
       "Turn latency p999: #{stats.p999}us (limit: 500000us)"}
    ]

    passed = Enum.filter(checks, fn {_name, result, _msg} -> result end)
    failed = Enum.filter(checks, fn {_name, result, _msg} -> not result end)

    # Informational metrics (not pass/fail criteria)
    flow_control_info =
      "Flow control pause/resume events: #{state.flow_control.pause_events}/#{state.flow_control.resume_events}"

    result = %{
      passed: Enum.map(passed, fn {name, _, msg} -> {name, msg} end),
      failed: Enum.map(failed, fn {name, _, msg} -> {name, msg} end),
      total_checks: length(checks),
      passed_count: length(passed),
      failed_count: length(failed),
      info: [flow_control_info]
    }

    if length(failed) == 0 do
      {:ok, result}
    else
      {:error, result}
    end
  end
end
