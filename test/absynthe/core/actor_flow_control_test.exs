defmodule Absynthe.Core.ActorFlowControlTest do
  @moduledoc """
  Tests for per-actor internal flow control.

  The flow control system tracks debt for actions enqueued during turn commit
  and repays when those actions complete. This provides backpressure for
  internal cascades and observer fan-out.
  """

  use ExUnit.Case, async: true

  alias Absynthe.Core.Actor
  alias Absynthe.Core.Entity
  alias Absynthe.Core.Turn
  alias Absynthe.Protocol.Event

  # Simple echo entity for testing
  defmodule EchoEntity do
    defstruct [:messages, :target_ref]

    defimpl Entity do
      def on_message(entity, msg, turn) do
        # Record the message
        entity = %{entity | messages: [msg | entity.messages || []]}

        # If we have a target, forward the message (creates cascade)
        turn =
          if entity.target_ref do
            Turn.add_action(turn, Event.message(entity.target_ref, msg))
          else
            turn
          end

        {entity, turn}
      end

      def on_publish(entity, _assertion, _handle, turn), do: {entity, turn}
      def on_retract(entity, _handle, turn), do: {entity, turn}

      def on_sync(entity, peer, turn) do
        turn = Turn.add_action(turn, Event.message(peer, {:symbol, "synced"}))
        {entity, turn}
      end
    end
  end

  # Fan-out entity: forwards a message to multiple targets
  defmodule FanOutEntity do
    defstruct [:targets]

    defimpl Entity do
      def on_message(entity, msg, turn) do
        # Forward to all targets
        turn =
          Enum.reduce(entity.targets || [], turn, fn target_ref, acc_turn ->
            Turn.add_action(acc_turn, Event.message(target_ref, msg))
          end)

        {entity, turn}
      end

      def on_publish(entity, _assertion, _handle, turn), do: {entity, turn}
      def on_retract(entity, _handle, turn), do: {entity, turn}
      def on_sync(entity, _peer, turn), do: {entity, turn}
    end
  end

  # Entity that spawns other entities on message
  defmodule SpawnEntity do
    defstruct [:count]

    defimpl Entity do
      def on_message(entity, _msg, turn) do
        # Spawn 'count' new entities
        turn =
          Enum.reduce(1..(entity.count || 1), turn, fn _, acc_turn ->
            Turn.add_action(
              acc_turn,
              %Absynthe.Protocol.Event.Spawn{
                facet_id: :root,
                entity: %Absynthe.Core.ActorFlowControlTest.EchoEntity{messages: []},
                callback: nil
              }
            )
          end)

        {entity, turn}
      end

      def on_publish(entity, _assertion, _handle, turn), do: {entity, turn}
      def on_retract(entity, _handle, turn), do: {entity, turn}
      def on_sync(entity, _peer, turn), do: {entity, turn}
    end
  end

  describe "flow_control initialization" do
    test "actor without flow_control option has nil account" do
      {:ok, actor} = Actor.start_link(id: :no_fc_actor)
      stats = Actor.flow_control_stats(actor)
      assert stats == nil
      GenServer.stop(actor)
    end

    test "actor with flow_control: true gets default account" do
      {:ok, actor} = Actor.start_link(id: :fc_true_actor, flow_control: true)
      stats = Actor.flow_control_stats(actor)

      assert stats != nil
      assert stats.debt == 0
      assert stats.limit == 1000
      assert stats.paused == false
      assert stats.mailbox_length >= 0

      GenServer.stop(actor)
    end

    test "actor with custom flow_control options" do
      {:ok, actor} =
        Actor.start_link(
          id: :fc_custom_actor,
          flow_control: [limit: 100, high_water_mark: 80, low_water_mark: 20]
        )

      stats = Actor.flow_control_stats(actor)

      assert stats != nil
      assert stats.limit == 100
      assert stats.high_water_mark == 80
      assert stats.low_water_mark == 20

      GenServer.stop(actor)
    end
  end

  describe "debt tracking" do
    test "external message does not affect debt" do
      {:ok, actor} =
        Actor.start_link(id: :debt_test_actor, flow_control: true)

      # Spawn entity
      {:ok, ref} = Actor.spawn_entity(actor, :root, %EchoEntity{messages: []})

      # Send external message - this shouldn't borrow or repay debt
      Actor.send_message(actor, ref, {:string, "hello"})

      # Give time for processing
      Process.sleep(50)

      # Debt should be zero since external messages don't borrow
      stats = Actor.flow_control_stats(actor)
      assert stats.debt == 0

      GenServer.stop(actor)
    end

    test "debt increases during fan-out then returns to zero" do
      test_pid = self()

      # Use low limits to trigger pause during fan-out
      {:ok, actor} =
        Actor.start_link(
          id: :debt_fanout_actor,
          flow_control: [limit: 10, high_water_mark: 3, low_water_mark: 1]
        )

      # Spawn receiver entities
      receivers =
        for _ <- 1..5 do
          {:ok, ref} = Actor.spawn_entity(actor, :root, %EchoEntity{messages: []})
          ref
        end

      # Spawn fan-out entity that will send to all receivers
      {:ok, fanout_ref} = Actor.spawn_entity(actor, :root, %FanOutEntity{targets: receivers})

      # Attach telemetry to capture pause event (indicates debt increased)
      :telemetry.attach(
        "debt-fanout-test",
        [:absynthe, :flow_control, :account, :pause],
        fn _event, measurements, _metadata, _config ->
          send(test_pid, {:paused, measurements.debt})
        end,
        nil
      )

      # Send message to fan-out entity - this creates 5 internal messages
      # Total cost should be 5 (one per message action)
      Actor.send_message(actor, fanout_ref, {:string, "broadcast"})

      # Should receive pause since debt (5) >= high_water_mark (3)
      assert_receive {:paused, debt}, 500
      assert debt >= 3

      # Give time for all processing to complete
      Process.sleep(100)

      # After all processing, debt should return to zero
      stats = Actor.flow_control_stats(actor)
      assert stats.debt == 0

      :telemetry.detach("debt-fanout-test")
      GenServer.stop(actor)
    end

    test "fan-out causes debt accumulation" do
      {:ok, actor} = Actor.start_link(id: :fanout_test_actor, flow_control: [limit: 100])

      # Spawn multiple receiver entities
      receivers =
        for _ <- 1..10 do
          {:ok, ref} = Actor.spawn_entity(actor, :root, %EchoEntity{messages: []})
          ref
        end

      # Spawn fan-out entity
      {:ok, fanout_ref} = Actor.spawn_entity(actor, :root, %FanOutEntity{targets: receivers})

      # Send message to fan-out entity
      Actor.send_message(actor, fanout_ref, {:string, "broadcast"})

      # Give time for processing
      Process.sleep(100)

      # After all processing, debt should return to zero
      stats = Actor.flow_control_stats(actor)
      assert stats.debt == 0

      GenServer.stop(actor)
    end
  end

  describe "pause/resume" do
    test "pause callback is invoked when debt exceeds high water mark" do
      test_pid = self()

      {:ok, actor} =
        Actor.start_link(
          id: :pause_test_actor,
          flow_control: [
            limit: 20,
            high_water_mark: 10,
            low_water_mark: 5,
            pause_callback: fn _account -> send(test_pid, :paused) end,
            resume_callback: fn _account -> send(test_pid, :resumed) end
          ]
        )

      # Spawn many receiver entities to create high fan-out
      receivers =
        for _ <- 1..15 do
          {:ok, ref} = Actor.spawn_entity(actor, :root, %EchoEntity{messages: []})
          ref
        end

      # Spawn fan-out entity
      {:ok, fanout_ref} = Actor.spawn_entity(actor, :root, %FanOutEntity{targets: receivers})

      # Send message to fan-out entity - this should trigger pause
      Actor.send_message(actor, fanout_ref, {:string, "broadcast"})

      # We should receive pause notification
      assert_receive :paused, 500

      # After processing completes, we should receive resume
      assert_receive :resumed, 500

      GenServer.stop(actor)
    end
  end

  describe "flow_control_stats/1" do
    test "returns nil for actors without flow control" do
      {:ok, actor} = Actor.start_link(id: :stats_nil_actor)
      assert Actor.flow_control_stats(actor) == nil
      GenServer.stop(actor)
    end

    test "returns stats map with mailbox_length for actors with flow control" do
      {:ok, actor} = Actor.start_link(id: :stats_actor, flow_control: true)
      stats = Actor.flow_control_stats(actor)

      assert is_map(stats)
      assert Map.has_key?(stats, :debt)
      assert Map.has_key?(stats, :limit)
      assert Map.has_key?(stats, :paused)
      assert Map.has_key?(stats, :mailbox_length)
      assert Map.has_key?(stats, :utilization)

      GenServer.stop(actor)
    end
  end

  describe "spawn action repayment" do
    test "spawn actions repay debt immediately" do
      {:ok, actor} =
        Actor.start_link(id: :spawn_repay_actor, flow_control: [limit: 100])

      # Spawn an entity that will spawn more entities on message
      {:ok, spawner_ref} = Actor.spawn_entity(actor, :root, %SpawnEntity{count: 5})

      # Trigger the spawns
      Actor.send_message(actor, spawner_ref, {:symbol, "spawn"})

      # Give time for processing
      Process.sleep(100)

      # Debt should return to zero
      stats = Actor.flow_control_stats(actor)
      assert stats.debt == 0
      # No loans tracked since we use aggregate debt
      assert stats.outstanding_loans == 0

      GenServer.stop(actor)
    end
  end

  describe "telemetry" do
    test "emits overload telemetry when paused" do
      test_pid = self()

      # Attach telemetry handler
      :telemetry.attach(
        "overload-test-handler",
        [:absynthe, :flow_control, :actor, :overload],
        fn _event, measurements, metadata, _config ->
          send(test_pid, {:overload, measurements, metadata})
        end,
        nil
      )

      {:ok, actor} =
        Actor.start_link(
          id: :telemetry_test_actor,
          flow_control: [limit: 5, high_water_mark: 3, low_water_mark: 1]
        )

      # Spawn many receiver entities to create high fan-out
      receivers =
        for _ <- 1..10 do
          {:ok, ref} = Actor.spawn_entity(actor, :root, %EchoEntity{messages: []})
          ref
        end

      # Spawn fan-out entity
      {:ok, fanout_ref} = Actor.spawn_entity(actor, :root, %FanOutEntity{targets: receivers})

      # Send message to fan-out entity - this should cause overload
      Actor.send_message(actor, fanout_ref, {:string, "broadcast"})

      # We should receive overload telemetry
      assert_receive {:overload, measurements, metadata}, 500
      assert Map.has_key?(measurements, :debt)
      assert Map.has_key?(measurements, :mailbox_length)
      assert metadata.actor_id == :telemetry_test_actor

      :telemetry.detach("overload-test-handler")
      GenServer.stop(actor)
    end
  end
end
