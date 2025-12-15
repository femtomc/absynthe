# Market Stress Test Runner
#
# Usage:
#   MIX_ENV=test mix run examples/stress/runner.exs --profile steady --seed 42
#   MIX_ENV=test mix run examples/stress/runner.exs --profile burst --seed 123
#   MIX_ENV=test mix run examples/stress/runner.exs --all --report stress_report.json

require Logger

alias Absynthe.Core.Actor
alias Absynthe.Dataspace.Dataspace
alias Absynthe.Examples.Stress.LoadGenerator
alias Absynthe.Examples.Stress.TelemetryCollector
alias Absynthe.Examples.Stress.Entities.{OrderBook, Matcher, History, Analytics}

defmodule StressRunner do
  @moduledoc """
  Runner for market stress tests.
  """

  @profiles %{
    steady: %{
      name: "Steady State",
      clients: 10,
      symbols: ["BTC-USD", "ETH-USD", "SOL-USD"],
      duration: 60_000,
      warmup: 10_000,
      seed: 42
    },
    burst: %{
      name: "Burst Traffic",
      clients: 50,
      symbols: ["BTC-USD", "ETH-USD", "SOL-USD", "DOGE-USD", "ADA-USD"],
      duration: 120_000,
      warmup: 15_000,
      seed: 123
    },
    churn: %{
      name: "High Churn",
      clients: 20,
      symbols: Enum.map(1..50, &"SYM-#{&1}"),
      duration: 180_000,
      warmup: 20_000,
      seed: 456
    },
    chaos: %{
      name: "Chaos Mode",
      clients: 30,
      symbols: Enum.map(1..20, &"SYM-#{&1}"),
      duration: 300_000,
      warmup: 30_000,
      seed: 789,
      config: %{
        disconnect_chance: 5,
        error_chance: 2,
        delay_chance: 10
      }
    },
    negative: %{
      name: "Negative Path",
      clients: 10,
      symbols: ["BTC-USD"],
      duration: 60_000,
      warmup: 5_000,
      seed: 999
    }
  }

  def run(profile_name, opts \\ []) do
    profile = Map.fetch!(@profiles, profile_name)
    seed = Keyword.get(opts, :seed, profile.seed)

    Logger.info("=" |> String.duplicate(60))
    Logger.info("Running stress test: #{profile.name}")
    Logger.info("Profile: #{profile_name}")
    Logger.info("Clients: #{profile.clients}")
    Logger.info("Symbols: #{length(profile.symbols)}")
    Logger.info("Duration: #{profile.duration}ms")
    Logger.info("Seed: #{seed}")
    Logger.info("=" |> String.duplicate(60))

    # Start telemetry collector
    {:ok, collector} = TelemetryCollector.start_link()
    TelemetryCollector.attach(collector)

    # Start the broker actor (hosts dataspace + market entities)
    {:ok, broker} =
      Actor.start_link(
        id: :broker,
        flow_control: [limit: 1000, high_water_mark: 800, low_water_mark: 200]
      )

    # Create dataspace entity
    dataspace = Dataspace.new()
    {:ok, dataspace_ref} = Actor.spawn_entity(broker, :root, dataspace)

    # Spawn market entities for each symbol
    spawn_market_entities(broker, dataspace_ref, profile.symbols)

    # Start load generator
    {:ok, generator} =
      LoadGenerator.start_link(
        dataspace_ref: dataspace_ref,
        actor_pid: broker,
        profile: profile_name,
        clients: profile.clients,
        symbols: profile.symbols,
        seed: seed,
        config: Map.get(profile, :config, %{})
      )

    # Initialize traders
    LoadGenerator.init_traders(generator)

    # Run the test
    Logger.info("Starting load test...")

    LoadGenerator.run(generator,
      duration: profile.duration,
      warmup: profile.warmup
    )

    # Wait for test to complete
    Process.sleep(profile.duration + profile.warmup + 1000)

    # Collect results
    Logger.info("Collecting results...")

    gen_results = LoadGenerator.results(generator)
    report = TelemetryCollector.report(collector)
    acceptance = TelemetryCollector.check_acceptance(collector)

    # Cleanup
    TelemetryCollector.detach(collector)
    GenServer.stop(generator)
    GenServer.stop(broker)

    # Print results
    print_results(profile_name, gen_results, report, acceptance)

    %{
      profile: profile_name,
      generator_results: gen_results,
      telemetry_report: report,
      acceptance: acceptance
    }
  end

  def run_all(opts \\ []) do
    profiles = Keyword.get(opts, :profiles, Map.keys(@profiles))

    results =
      Enum.map(profiles, fn profile ->
        {profile, run(profile, opts)}
      end)

    # Generate summary
    Logger.info("")
    Logger.info("=" |> String.duplicate(60))
    Logger.info("SUMMARY")
    Logger.info("=" |> String.duplicate(60))

    Enum.each(results, fn {profile, result} ->
      status =
        case result.acceptance do
          {:ok, _} -> "✓ PASS"
          {:error, _} -> "✗ FAIL"
        end

      Logger.info("#{profile}: #{status}")
    end)

    results
  end

  defp spawn_market_entities(broker, dataspace_ref, symbols) do
    Enum.each(symbols, fn symbol ->
      # OrderBook
      order_book = OrderBook.new(symbol)
      {:ok, _} = Actor.spawn_entity(broker, :root, order_book)

      # Matcher
      matcher = Matcher.new(symbol, dataspace_ref)
      {:ok, _} = Actor.spawn_entity(broker, :root, matcher)

      # History
      history =
        History.new(
          symbol: symbol,
          dataspace_ref: dataspace_ref,
          reassert_trades: true,
          max_trades: 1000
        )

      {:ok, _} = Actor.spawn_entity(broker, :root, history)

      # Analytics
      analytics =
        Analytics.new(symbol,
          dataspace_ref: dataspace_ref,
          assert_stats: true
        )

      {:ok, _} = Actor.spawn_entity(broker, :root, analytics)
    end)
  end

  defp print_results(profile_name, gen_results, report, acceptance) do
    Logger.info("")
    Logger.info("-" |> String.duplicate(60))
    Logger.info("Results for #{profile_name}")
    Logger.info("-" |> String.duplicate(60))

    Logger.info("Orders sent: #{gen_results.metrics.orders_sent}")
    Logger.info("Cancels sent: #{gen_results.metrics.cancels_sent}")
    Logger.info("Errors: #{gen_results.metrics.errors}")

    Logger.info("")
    Logger.info("Telemetry:")
    Logger.info("  Orders placed: #{report.counters.orders_placed}")
    Logger.info("  Orders filled: #{report.counters.orders_filled}")
    Logger.info("  Turn errors: #{report.counters.turn_errors}")

    if report.latencies.turn.count > 0 do
      Logger.info("")
      Logger.info("Turn Latencies (us):")
      Logger.info("  p50:  #{report.latencies.turn.p50}")
      Logger.info("  p99:  #{report.latencies.turn.p99}")
      Logger.info("  p999: #{report.latencies.turn.p999}")
      Logger.info("  max:  #{report.latencies.turn.max}")
    end

    Logger.info("")
    Logger.info("Flow Control:")
    Logger.info("  Pause events: #{report.flow_control.pause_events}")
    Logger.info("  Resume events: #{report.flow_control.resume_events}")
    Logger.info("  Max debt: #{report.flow_control.max_debt}")

    Logger.info("")

    case acceptance do
      {:ok, results} ->
        Logger.info("Acceptance: PASS (#{results.passed_count}/#{results.total_checks})")

      {:error, results} ->
        Logger.info("Acceptance: FAIL (#{results.passed_count}/#{results.total_checks})")
        Logger.info("Failed checks:")

        Enum.each(results.failed, fn {name, msg} ->
          Logger.info("  - #{name}: #{msg}")
        end)
    end
  end
end

# Parse command line arguments
args = System.argv()

{opts, _, _} =
  OptionParser.parse(args,
    strict: [
      profile: :string,
      seed: :integer,
      all: :boolean,
      report: :string
    ]
  )

cond do
  opts[:all] ->
    results = StressRunner.run_all(opts)

    if opts[:report] do
      json = Jason.encode!(results, pretty: true)
      File.write!(opts[:report], json)
      Logger.info("Report written to #{opts[:report]}")
    end

  opts[:profile] ->
    profile = String.to_existing_atom(opts[:profile])
    StressRunner.run(profile, opts)

  true ->
    IO.puts("""
    Usage:
      mix run examples/stress/runner.exs --profile <profile> [--seed <seed>]
      mix run examples/stress/runner.exs --all [--report <file>]

    Profiles:
      steady   - Steady state baseline (10 clients, 60s)
      burst    - Burst traffic pattern (50 clients, 120s)
      churn    - High churn rate (20 clients, 180s)
      chaos    - Chaos injection (30 clients, 300s)
      negative - Negative path testing (10 clients, 60s)

    Examples:
      mix run examples/stress/runner.exs --profile steady --seed 42
      mix run examples/stress/runner.exs --all --report stress_report.json
    """)
end
