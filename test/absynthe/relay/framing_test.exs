defmodule Absynthe.Relay.FramingTest do
  use ExUnit.Case, async: true

  alias Absynthe.Relay.{Framing, Packet}

  describe "encode/1" do
    test "encodes turn packet to binary" do
      turn = Packet.turn([])

      {:ok, binary} = Framing.encode(turn)

      assert is_binary(binary)
    end

    test "encodes nop packet to binary" do
      {:ok, binary} = Framing.encode(:nop)

      assert is_binary(binary)
    end
  end

  describe "decode_from_buffer/1" do
    test "decodes complete packet" do
      {:ok, binary} = Framing.encode(Packet.turn([]))

      {:ok, packet, rest} = Framing.decode_from_buffer(binary)

      assert %Packet.Turn{events: []} = packet
      assert rest == <<>>
    end

    test "returns incomplete for partial data" do
      # Use a turn with content to get a longer binary
      event = Packet.event(0, Packet.assert({:string, "hello world"}, 42))
      {:ok, binary} = Framing.encode(Packet.turn([event]))
      # Take only first half
      partial = binary_part(binary, 0, div(byte_size(binary), 2))

      {:incomplete, buffer} = Framing.decode_from_buffer(partial)

      assert buffer == partial
    end

    test "returns incomplete for empty buffer" do
      {:incomplete, <<>>} = Framing.decode_from_buffer(<<>>)
    end

    test "preserves remaining bytes after packet" do
      {:ok, binary1} = Framing.encode(Packet.turn([]))
      {:ok, binary2} = Framing.encode(:nop)

      {:ok, packet, rest} = Framing.decode_from_buffer(binary1 <> binary2)

      assert %Packet.Turn{} = packet
      assert rest == binary2
    end
  end

  describe "decode_all/1" do
    test "decodes multiple packets" do
      {:ok, binary1} = Framing.encode(Packet.turn([]))
      {:ok, binary2} = Framing.encode(:nop)
      {:ok, binary3} = Framing.encode(Packet.turn([]))

      {:ok, packets, rest} = Framing.decode_all(binary1 <> binary2 <> binary3)

      assert length(packets) == 3
      assert rest == <<>>
    end

    test "returns remaining partial data" do
      {:ok, binary1} = Framing.encode(Packet.turn([]))
      # Use a longer second packet that can be meaningfully split
      event = Packet.event(0, Packet.assert({:string, "hello world"}, 42))
      {:ok, binary2} = Framing.encode(Packet.turn([event]))
      # Take only first half of second packet
      partial = binary_part(binary2, 0, div(byte_size(binary2), 2))

      {:ok, packets, rest} = Framing.decode_all(binary1 <> partial)

      assert length(packets) == 1
      assert rest == partial
    end

    test "returns empty list for empty buffer" do
      {:ok, packets, rest} = Framing.decode_all(<<>>)

      assert packets == []
      assert rest == <<>>
    end

    test "returns error for corrupted data" do
      # Invalid Preserves binary - starts with an invalid tag byte
      corrupted = <<0xFF, 0x00, 0x01, 0x02>>

      {:error, reason, packets, buffer} = Framing.decode_all(corrupted)

      assert packets == []
      assert buffer == corrupted
      assert {:preserves_decode_error, _} = reason
    end

    test "returns error with successfully decoded packets" do
      {:ok, valid_binary} = Framing.encode(Packet.turn([]))
      # Add some corrupted data after the valid packet
      corrupted = <<0xFF, 0x00, 0x01, 0x02>>

      {:error, reason, packets, buffer} = Framing.decode_all(valid_binary <> corrupted)

      # Should have decoded the first valid packet
      assert length(packets) == 1
      # Buffer contains the corrupted data
      assert buffer == corrupted
      assert {:preserves_decode_error, _} = reason
    end
  end

  describe "append_and_decode/2" do
    test "accumulates data and extracts packets" do
      {:ok, binary1} = Framing.encode(Packet.turn([]))
      {:ok, binary2} = Framing.encode(:nop)

      buffer = Framing.new_state()

      # First chunk - partial packet
      {:ok, packets1, buffer} = Framing.append_and_decode(buffer, binary_part(binary1, 0, 1))
      assert packets1 == []

      # Rest of first packet + second packet
      {:ok, packets2, buffer} =
        Framing.append_and_decode(
          buffer,
          binary_part(binary1, 1, byte_size(binary1) - 1) <> binary2
        )

      assert length(packets2) == 2
      assert buffer == <<>>
    end

    test "returns error for corrupted data" do
      buffer = Framing.new_state()
      corrupted = <<0xFF, 0x00, 0x01, 0x02>>

      {:error, reason, packets, _buffer} = Framing.append_and_decode(buffer, corrupted)

      assert packets == []
      assert {:preserves_decode_error, _} = reason
    end
  end

  describe "roundtrip" do
    test "turn with events roundtrips correctly" do
      event = Packet.event(0, Packet.assert({:string, "hello"}, 42))
      original = Packet.turn([event])

      {:ok, binary} = Framing.encode(original)
      {:ok, decoded, <<>>} = Framing.decode_from_buffer(binary)

      assert %Packet.Turn{events: [decoded_event]} = decoded
      assert decoded_event.oid == 0
      assert %Packet.Assert{assertion: {:string, "hello"}, handle: 42} = decoded_event.event
    end

    test "error packet roundtrips correctly" do
      original = Packet.error("test error", {:integer, 123})

      {:ok, binary} = Framing.encode(original)
      {:ok, decoded, <<>>} = Framing.decode_from_buffer(binary)

      assert %Packet.Error{message: "test error", detail: {:integer, 123}} = decoded
    end
  end
end
