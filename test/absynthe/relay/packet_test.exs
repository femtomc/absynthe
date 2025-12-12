defmodule Absynthe.Relay.PacketTest do
  use ExUnit.Case, async: true

  alias Absynthe.Relay.Packet
  alias Absynthe.Relay.Packet.{Turn, Error, TurnEvent, Assert, Retract, Message, Sync, WireRef}

  describe "Turn encoding/decoding" do
    test "encodes and decodes empty turn" do
      turn = %Turn{events: []}

      {:ok, value} = Packet.encode(turn)
      {:ok, decoded} = Packet.decode(value)

      assert %Turn{events: []} = decoded
    end

    test "encodes and decodes turn with assert event" do
      turn = %Turn{
        events: [
          %TurnEvent{
            oid: 0,
            event: %Assert{
              assertion: {:string, "hello"},
              handle: 42
            }
          }
        ]
      }

      {:ok, value} = Packet.encode(turn)
      {:ok, decoded} = Packet.decode(value)

      assert %Turn{events: [event]} = decoded
      assert event.oid == 0
      assert %Assert{assertion: {:string, "hello"}, handle: 42} = event.event
    end

    test "encodes and decodes turn with retract event" do
      turn = %Turn{
        events: [
          %TurnEvent{
            oid: 5,
            event: %Retract{handle: 123}
          }
        ]
      }

      {:ok, value} = Packet.encode(turn)
      {:ok, decoded} = Packet.decode(value)

      assert %Turn{events: [event]} = decoded
      assert event.oid == 5
      assert %Retract{handle: 123} = event.event
    end

    test "encodes and decodes turn with message event" do
      turn = %Turn{
        events: [
          %TurnEvent{
            oid: 1,
            event: %Message{body: {:record, {{:symbol, "Ping"}, []}}}
          }
        ]
      }

      {:ok, value} = Packet.encode(turn)
      {:ok, decoded} = Packet.decode(value)

      assert %Turn{events: [event]} = decoded
      assert event.oid == 1
      assert %Message{body: {:record, {{:symbol, "Ping"}, []}}} = event.event
    end

    test "encodes and decodes turn with sync event" do
      turn = %Turn{
        events: [
          %TurnEvent{
            oid: 0,
            event: %Sync{peer: true}
          }
        ]
      }

      {:ok, value} = Packet.encode(turn)
      {:ok, decoded} = Packet.decode(value)

      assert %Turn{events: [event]} = decoded
      assert %Sync{} = event.event
    end

    test "encodes and decodes turn with multiple events" do
      turn = %Turn{
        events: [
          %TurnEvent{oid: 0, event: %Assert{assertion: {:integer, 1}, handle: 1}},
          %TurnEvent{oid: 0, event: %Assert{assertion: {:integer, 2}, handle: 2}},
          %TurnEvent{oid: 1, event: %Message{body: {:string, "test"}}}
        ]
      }

      {:ok, value} = Packet.encode(turn)
      {:ok, decoded} = Packet.decode(value)

      assert %Turn{events: events} = decoded
      assert length(events) == 3
    end
  end

  describe "Nop encoding/decoding" do
    test "encodes nop as false" do
      {:ok, value} = Packet.encode(:nop)
      assert value == {:boolean, false}
    end

    test "decodes false as nop" do
      {:ok, decoded} = Packet.decode({:boolean, false})
      assert decoded == :nop
    end
  end

  describe "Error encoding/decoding" do
    test "encodes and decodes error packet" do
      error = %Error{message: "something went wrong", detail: {:integer, 42}}

      {:ok, value} = Packet.encode(error)
      {:ok, decoded} = Packet.decode(value)

      assert %Error{message: "something went wrong", detail: {:integer, 42}} = decoded
    end
  end

  describe "WireRef encoding/decoding" do
    test "encodes and decodes mine variant" do
      wire_ref = WireRef.mine(5)

      {:ok, value} = Packet.encode_wire_ref(wire_ref)
      {:ok, decoded} = Packet.decode_wire_ref(value)

      assert decoded.variant == :mine
      assert decoded.oid == 5
    end

    test "encodes and decodes yours variant without attenuation" do
      wire_ref = WireRef.yours(10)

      {:ok, value} = Packet.encode_wire_ref(wire_ref)
      {:ok, decoded} = Packet.decode_wire_ref(value)

      assert decoded.variant == :yours
      assert decoded.oid == 10
      assert decoded.attenuation == []
    end

    test "encodes and decodes yours variant with attenuation" do
      caveat = {:record, {{:symbol, "rewrite"}, [{:symbol, "$"}, {:symbol, "$"}]}}
      wire_ref = WireRef.yours(10, [caveat])

      {:ok, value} = Packet.encode_wire_ref(wire_ref)
      {:ok, decoded} = Packet.decode_wire_ref(value)

      assert decoded.variant == :yours
      assert decoded.oid == 10
      assert length(decoded.attenuation) == 1
    end
  end

  describe "helper constructors" do
    test "turn/1 creates turn packet" do
      turn = Packet.turn([])
      assert %Turn{events: []} = turn
    end

    test "event/2 creates turn event" do
      event = Packet.event(5, %Assert{assertion: {:integer, 1}, handle: 1})
      assert %TurnEvent{oid: 5} = event
    end

    test "assert/2 creates assert event" do
      event = Packet.assert({:string, "test"}, 42)
      assert %Assert{assertion: {:string, "test"}, handle: 42} = event
    end

    test "retract/1 creates retract event" do
      event = Packet.retract(42)
      assert %Retract{handle: 42} = event
    end

    test "message/1 creates message event" do
      event = Packet.message({:string, "hello"})
      assert %Message{body: {:string, "hello"}} = event
    end

    test "sync/0 creates sync event" do
      event = Packet.sync()
      assert %Sync{} = event
    end

    test "error/2 creates error packet" do
      error = Packet.error("oops", {:integer, 1})
      assert %Error{message: "oops", detail: {:integer, 1}} = error
    end
  end
end
