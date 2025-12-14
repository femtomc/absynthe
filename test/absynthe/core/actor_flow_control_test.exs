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
    test "external messages borrow then repay debt" do
      {:ok, actor} =
        Actor.start_link(id: :debt_test_actor, flow_control: true)

      # Spawn entity
      {:ok, ref} = Actor.spawn_entity(actor, :root, %EchoEntity{messages: []})

      # Send external message - now tracks debt for incoming load
      Actor.send_message(actor, ref, {:string, "hello"})

      # Give time for processing
      Process.sleep(50)

      # After processing, debt should return to zero (borrow then repay)
      stats = Actor.flow_control_stats(actor)
      assert stats.debt == 0

      GenServer.stop(actor)
    end

    test "single incoming message triggers pause when high_water_mark is 1" do
      test_pid = self()

      # Use high_water_mark of 1 so a single message triggers pause
      {:ok, actor} =
        Actor.start_link(
          id: :"single_msg_pause_#{:erlang.unique_integer([:positive])}",
          flow_control: [limit: 10, high_water_mark: 1, low_water_mark: 0]
        )

      # Attach telemetry to capture pause event
      handler_id = "single-msg-pause-test-#{:erlang.unique_integer()}"

      :telemetry.attach(
        handler_id,
        [:absynthe, :flow_control, :account, :pause],
        fn _event, measurements, _metadata, _config ->
          send(test_pid, {:paused, measurements.debt})
        end,
        nil
      )

      # Spawn a simple entity that does nothing on message (no fan-out)
      {:ok, ref} = Actor.spawn_entity(actor, :root, %EchoEntity{messages: []})

      # Send a single message - incoming borrow should trigger pause at debt=1
      Actor.send_message(actor, ref, {:string, "trigger_pause"})

      # Should receive pause telemetry from the incoming event borrow
      # (not from any fan-out actions since EchoEntity has no target)
      assert_receive {:paused, debt}, 500
      assert debt >= 1

      # Give time for processing
      Process.sleep(50)

      # After processing, debt returns to zero
      stats = Actor.flow_control_stats(actor)
      assert stats.debt == 0

      :telemetry.detach(handler_id)
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

  describe "inter-actor flow control" do
    # Entity that creates fan-out when receiving from another actor
    defmodule InterActorFanOutEntity do
      defstruct [:targets]

      defimpl Entity do
        def on_message(entity, _msg, turn) do
          # Forward to all targets - this creates internal fan-out
          turn =
            Enum.reduce(entity.targets || [], turn, fn target_ref, acc_turn ->
              Turn.add_action(acc_turn, Event.message(target_ref, {:symbol, "forwarded"}))
            end)

          {entity, turn}
        end

        def on_publish(entity, _assertion, _handle, turn), do: {entity, turn}
        def on_retract(entity, _handle, turn), do: {entity, turn}
        def on_sync(entity, _peer, turn), do: {entity, turn}
      end
    end

    test "receiver tracks debt for fan-out triggered by inter-actor messages" do
      test_pid = self()

      # Use unique atom names to avoid global registration conflicts
      sender_id = :"sender_actor_#{:erlang.unique_integer([:positive])}"
      receiver_id = :"receiver_actor_#{:erlang.unique_integer([:positive])}"

      # Start sender actor (no flow control, registered with unique name)
      {:ok, sender} = Actor.start_link(id: sender_id, name: sender_id)

      # Start receiver actor with flow control
      {:ok, receiver} =
        Actor.start_link(
          id: receiver_id,
          name: receiver_id,
          flow_control: [limit: 20, high_water_mark: 5, low_water_mark: 2]
        )

      # Create receiver entities
      receivers =
        for _ <- 1..10 do
          {:ok, ref} = Actor.spawn_entity(receiver, :root, %EchoEntity{messages: []})
          ref
        end

      # Create fan-out entity that forwards to all receivers
      {:ok, fanout_ref} =
        Actor.spawn_entity(receiver, :root, %InterActorFanOutEntity{targets: receivers})

      # Attach telemetry to capture pause event
      :telemetry.attach(
        "inter-actor-fanout-test-#{:erlang.unique_integer()}",
        [:absynthe, :flow_control, :account, :pause],
        fn _event, measurements, metadata, _config ->
          if metadata.account_id == {:actor, receiver_id} do
            send(test_pid, {:paused, measurements.debt})
          end
        end,
        nil
      )

      # Send message from sender to receiver's fan-out entity
      # This triggers internal fan-out of 10 messages
      Actor.send_message(sender, fanout_ref, {:symbol, "trigger"})

      # Should trigger pause from the internal fan-out
      assert_receive {:paused, debt}, 500
      assert debt >= 5

      # After processing completes, debt should return to zero
      Process.sleep(100)
      stats = Actor.flow_control_stats(receiver)
      assert stats.debt == 0

      GenServer.stop(sender)
      GenServer.stop(receiver)
    end

    test "external API calls borrow and repay debt correctly" do
      test_pid = self()

      # Use unique atom name to avoid global registration conflicts
      actor_id = :"external_api_actor_#{:erlang.unique_integer([:positive])}"

      # Start actor with flow control and low threshold to trigger pause
      {:ok, actor} =
        Actor.start_link(
          id: actor_id,
          flow_control: [limit: 10, high_water_mark: 3, low_water_mark: 1]
        )

      # Attach telemetry to verify borrowing actually occurs
      :telemetry.attach(
        "external-api-borrow-test-#{:erlang.unique_integer()}",
        [:absynthe, :flow_control, :account, :pause],
        fn _event, measurements, _metadata, _config ->
          send(test_pid, {:paused, measurements.debt})
        end,
        nil
      )

      # Create a fan-out entity that will generate enough internal messages
      # to trigger pause (proving borrowing occurs)
      receivers =
        for _ <- 1..5 do
          {:ok, ref} = Actor.spawn_entity(actor, :root, %EchoEntity{messages: []})
          ref
        end

      {:ok, fanout_ref} =
        Actor.spawn_entity(actor, :root, %InterActorFanOutEntity{targets: receivers})

      # Send message to trigger fan-out
      Actor.send_message(actor, fanout_ref, {:symbol, "trigger"})

      # Should receive pause telemetry (proves borrowing occurred)
      assert_receive {:paused, debt}, 500
      assert debt >= 3

      # Give time for processing
      Process.sleep(100)

      # After processing, debt should return to zero (proves repayment)
      stats = Actor.flow_control_stats(actor)
      assert stats.debt == 0

      GenServer.stop(actor)
    end

    test "inter-actor messages with internal fan-out trigger overload telemetry" do
      test_pid = self()

      # Use unique atom names for actor IDs to avoid global registration conflicts
      sender_id = :"overload_sender_#{:erlang.unique_integer([:positive])}"
      receiver_id = :"overload_receiver_#{:erlang.unique_integer([:positive])}"

      # Start sender actor (no flow control, registered with unique name)
      {:ok, sender} = Actor.start_link(id: sender_id, name: sender_id)

      # Start receiver with low limits (registered with unique name)
      {:ok, receiver} =
        Actor.start_link(
          id: receiver_id,
          name: receiver_id,
          flow_control: [limit: 10, high_water_mark: 5, low_water_mark: 2]
        )

      # Create receiver entities
      receivers =
        for _ <- 1..15 do
          {:ok, ref} = Actor.spawn_entity(receiver, :root, %EchoEntity{messages: []})
          ref
        end

      # Create fan-out entity
      {:ok, fanout_ref} =
        Actor.spawn_entity(receiver, :root, %InterActorFanOutEntity{targets: receivers})

      # Attach telemetry for overload events
      :telemetry.attach(
        "overload-inter-actor-test-#{:erlang.unique_integer()}",
        [:absynthe, :flow_control, :actor, :overload],
        fn _event, measurements, metadata, _config ->
          if metadata.actor_id == receiver_id do
            send(test_pid, {:overload, measurements})
          end
        end,
        nil
      )

      # Send message from sender to trigger fan-out
      Actor.send_message(sender, fanout_ref, {:symbol, "trigger"})

      # Should receive overload telemetry from fan-out
      assert_receive {:overload, measurements}, 500
      assert Map.has_key?(measurements, :debt)
      assert Map.has_key?(measurements, :mailbox_length)

      GenServer.stop(sender)
      GenServer.stop(receiver)
    end
  end
end
