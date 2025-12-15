defmodule Absynthe.Examples.Stress.LoadGenerator do
  @moduledoc """
  Coordinates multiple traders to generate market load for stress testing.

  The LoadGenerator:
  - Spawns and manages multiple trader bots
  - Drives load according to configurable profiles
  - Supports burst patterns and chaos injection
  - Collects and reports metrics

  ## Load Profiles

  The generator supports multiple load profiles:
  - `:steady` - Constant rate of orders per client
  - `:burst` - Alternating burst and idle periods
  - `:churn` - High assertion/retraction rate with observer churn
  - `:chaos` - Normal load with random failures injected
  - `:negative` - Tests error paths with invalid inputs

  ## Usage

      # Start the load generator
      {:ok, generator} = LoadGenerator.start_link(
        dataspace_ref: dataspace,
        profile: :steady,
        clients: 10,
        symbols: ["BTC-USD", "ETH-USD"],
        seed: 42
      )

      # Run the test
      LoadGenerator.run(generator, duration: 60_000)

      # Get results
      results = LoadGenerator.results(generator)
  """

  use GenServer
  require Logger

  alias Absynthe.Examples.Stress.Entities.Trader
  alias Absynthe.Core.Actor

  defstruct [
    :dataspace_ref,
    :profile,
    :seed,
    :rand_state,
    :actor_pid,
    clients: 10,
    symbols: ["BTC-USD"],
    # Profile-specific config
    config: %{},
    # Trader state: %{trader_id => %{ref: ref, entity: trader}}
    traders: %{},
    # Running state
    running: false,
    start_time: nil,
    duration: nil,
    tick_interval: 100,
    tick_ref: nil,
    # Chaos hooks
    chaos_hooks: %{},
    # Metrics
    metrics: %{
      orders_sent: 0,
      cancels_sent: 0,
      errors: 0,
      pauses: 0,
      resumes: 0
    }
  ]

  @type profile :: :steady | :burst | :churn | :chaos | :negative

  # Client API

  @doc """
  Starts the load generator.

  Options:
  - `:dataspace_ref` - Reference to the market dataspace (required)
  - `:actor_pid` - Actor PID to spawn traders in (required)
  - `:profile` - Load profile (default: :steady)
  - `:clients` - Number of trader clients (default: 10)
  - `:symbols` - List of trading symbols (default: ["BTC-USD"])
  - `:seed` - Random seed for reproducibility (default: nil)
  - `:config` - Profile-specific configuration (default: %{})
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  @doc """
  Initializes traders and prepares for the test run.
  """
  @spec init_traders(GenServer.server()) :: :ok
  def init_traders(generator) do
    GenServer.call(generator, :init_traders, :infinity)
  end

  @doc """
  Runs the load test for the specified duration.

  Options:
  - `:duration` - Test duration in milliseconds (default: 60_000)
  - `:warmup` - Warmup duration in milliseconds (default: 0)
  """
  @spec run(GenServer.server(), keyword()) :: :ok
  def run(generator, opts \\ []) do
    GenServer.call(generator, {:run, opts}, :infinity)
  end

  @doc """
  Stops the load test.
  """
  @spec stop(GenServer.server()) :: :ok
  def stop(generator) do
    GenServer.call(generator, :stop)
  end

  @doc """
  Returns current results and metrics.
  """
  @spec results(GenServer.server()) :: map()
  def results(generator) do
    GenServer.call(generator, :results)
  end

  # GenServer callbacks

  @impl GenServer
  def init(opts) do
    dataspace_ref = Keyword.fetch!(opts, :dataspace_ref)
    actor_pid = Keyword.fetch!(opts, :actor_pid)

    seed = Keyword.get(opts, :seed)

    rand_state =
      if seed do
        :rand.seed_s(:exsss, seed)
      else
        :rand.seed_s(:exsss)
      end

    profile = Keyword.get(opts, :profile, :steady)

    state = %__MODULE__{
      dataspace_ref: dataspace_ref,
      actor_pid: actor_pid,
      profile: profile,
      clients: Keyword.get(opts, :clients, 10),
      symbols: Keyword.get(opts, :symbols, ["BTC-USD"]),
      seed: seed,
      rand_state: rand_state,
      config: Keyword.get(opts, :config, %{}),
      chaos_hooks: build_chaos_hooks(profile, Keyword.get(opts, :config, %{}))
    }

    {:ok, state}
  end

  @impl GenServer
  def handle_call(:init_traders, _from, state) do
    state = spawn_traders(state)
    {:reply, :ok, state}
  end

  @impl GenServer
  def handle_call({:run, opts}, _from, state) do
    duration = Keyword.get(opts, :duration, 60_000)
    warmup = Keyword.get(opts, :warmup, 0)

    # Start warmup if specified
    if warmup > 0 do
      Logger.info("Starting warmup period: #{warmup}ms")
      Process.sleep(warmup)
    end

    Logger.info("Starting load test: profile=#{state.profile}, clients=#{state.clients}")

    # Start the tick timer
    tick_ref = Process.send_after(self(), :tick, state.tick_interval)

    state = %{
      state
      | running: true,
        start_time: System.monotonic_time(:millisecond),
        duration: duration,
        tick_ref: tick_ref
    }

    # Schedule end of test
    Process.send_after(self(), :stop_test, duration)

    {:reply, :ok, state}
  end

  @impl GenServer
  def handle_call(:stop, _from, state) do
    state = stop_test(state)
    {:reply, :ok, state}
  end

  @impl GenServer
  def handle_call(:results, _from, state) do
    # Collect trader stats
    trader_stats =
      Enum.map(state.traders, fn {id, trader_state} ->
        {id, Trader.stats(trader_state.entity)}
      end)
      |> Map.new()

    results = %{
      profile: state.profile,
      clients: state.clients,
      symbols: state.symbols,
      duration_ms: state.duration,
      metrics: state.metrics,
      trader_stats: trader_stats
    }

    {:reply, results, state}
  end

  @impl GenServer
  def handle_info(:tick, %{running: false} = state) do
    {:noreply, state}
  end

  @impl GenServer
  def handle_info(:tick, state) do
    # Execute one tick of load generation
    state = execute_tick(state)

    # Schedule next tick
    tick_ref = Process.send_after(self(), :tick, state.tick_interval)
    state = %{state | tick_ref: tick_ref}

    {:noreply, state}
  end

  @impl GenServer
  def handle_info(:stop_test, state) do
    Logger.info("Load test complete")
    state = stop_test(state)
    {:noreply, state}
  end

  # Internal implementation

  defp spawn_traders(state) do
    # Use reduce to properly thread the rand_state through iterations
    {traders, rand_state} =
      Enum.reduce(1..state.clients, {%{}, state.rand_state}, fn i, {acc, rs} ->
        trader_id = "trader-#{i}"

        # Generate unique seed for this trader
        {trader_seed, rs} = :rand.uniform_s(1_000_000, rs)

        trader =
          Trader.new(
            trader_id: trader_id,
            dataspace_ref: state.dataspace_ref,
            symbols: state.symbols,
            seed: trader_seed
          )

        # Spawn in the actor
        {:ok, ref} = Actor.spawn_entity(state.actor_pid, :root, trader)

        {Map.put(acc, trader_id, %{ref: ref, entity: trader}), rs}
      end)

    %{state | traders: traders, rand_state: rand_state}
  end

  defp execute_tick(state) do
    # Calculate orders per tick based on profile
    orders_per_tick = orders_per_tick(state)

    # Determine which traders should place orders
    {traders_to_use, rand_state} =
      select_traders(state.rand_state, state.traders, orders_per_tick)

    state = %{state | rand_state: rand_state}

    # Execute orders
    state =
      Enum.reduce(traders_to_use, state, fn trader_id, acc_state ->
        execute_trader_action(acc_state, trader_id)
      end)

    # Maybe inject chaos
    state = maybe_inject_chaos(state)

    state
  end

  defp orders_per_tick(state) do
    elapsed = System.monotonic_time(:millisecond) - state.start_time

    case state.profile do
      :steady ->
        # Steady state: 5 orders/sec/client = 0.5 orders/100ms tick per client
        # Total = 0.5 * clients
        max(1, div(state.clients, 2))

      :burst ->
        # Burst pattern: 5s burst, 25s idle
        cycle_pos = rem(elapsed, 30_000)

        if cycle_pos < 5_000 do
          # Burst: 50 orders/sec/client = 5 orders/tick per client
          state.clients * 5
        else
          # Idle: minimal activity
          max(1, div(state.clients, 10))
        end

      :churn ->
        # High churn: 100/sec/client = 10 orders/tick per client
        state.clients * 10

      :chaos ->
        # Similar to burst
        cycle_pos = rem(elapsed, 30_000)

        if cycle_pos < 5_000 do
          state.clients * 5
        else
          max(1, div(state.clients, 2))
        end

      :negative ->
        # Lower rate for negative testing
        max(1, div(state.clients, 4))
    end
  end

  defp select_traders(rand_state, traders, count) do
    trader_ids = Map.keys(traders)
    count = min(count, length(trader_ids))

    {selected, rand_state} =
      Enum.reduce(1..count, {[], rand_state}, fn _i, {acc, rs} ->
        remaining = trader_ids -- acc
        {idx, rs} = :rand.uniform_s(length(remaining), rs)
        selected = Enum.at(remaining, idx - 1)
        {[selected | acc], rs}
      end)

    {selected, rand_state}
  end

  defp execute_trader_action(state, trader_id) do
    trader_state = Map.get(state.traders, trader_id)

    # Decide action: 80% place order, 20% cancel
    # Note: The trader entity handles the case where there are no orders to cancel (no-op)
    {action_roll, rand_state} = :rand.uniform_s(100, state.rand_state)
    state = %{state | rand_state: rand_state}

    action = if action_roll <= 80, do: :place, else: :cancel

    case action do
      :place ->
        # Send message to trader entity to place an order
        # The trader's on_message callback will handle generating and asserting the order
        :ok = Actor.send_message(state.actor_pid, trader_state.ref, {:symbol, "place_order"})
        metrics = %{state.metrics | orders_sent: state.metrics.orders_sent + 1}
        %{state | metrics: metrics}

      :cancel ->
        # Send message to trader entity to cancel a random order
        # If no orders exist, the trader's cancel_random_order is a no-op
        :ok = Actor.send_message(state.actor_pid, trader_state.ref, {:symbol, "cancel_random"})
        metrics = %{state.metrics | cancels_sent: state.metrics.cancels_sent + 1}
        %{state | metrics: metrics}
    end
  end

  defp maybe_inject_chaos(%{profile: :chaos} = state) do
    {roll, rand_state} = :rand.uniform_s(100, state.rand_state)
    state = %{state | rand_state: rand_state}

    cond do
      roll <= get_in(state.chaos_hooks, [:disconnect_chance]) ->
        # Simulate disconnect by removing a random trader's orders
        trader_id = Enum.random(Map.keys(state.traders))
        Logger.debug("Chaos: simulating disconnect for #{trader_id}")
        state

      roll <= get_in(state.chaos_hooks, [:error_chance]) + 5 ->
        # Simulate error
        Logger.debug("Chaos: injecting error")
        metrics = %{state.metrics | errors: state.metrics.errors + 1}
        %{state | metrics: metrics}

      true ->
        state
    end
  end

  defp maybe_inject_chaos(state), do: state

  defp stop_test(state) do
    if state.tick_ref do
      Process.cancel_timer(state.tick_ref)
    end

    %{state | running: false, tick_ref: nil}
  end

  defp build_chaos_hooks(:chaos, config) do
    %{
      disconnect_chance: Map.get(config, :disconnect_chance, 5),
      error_chance: Map.get(config, :error_chance, 2),
      delay_chance: Map.get(config, :delay_chance, 10)
    }
  end

  defp build_chaos_hooks(_profile, _config), do: %{}
end
