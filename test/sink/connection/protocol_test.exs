defmodule Sink.Connection.ProtocolTest do
  use ExUnit.Case, async: true
  use ExUnitProperties
  alias Sink.Connection.Protocol

  describe "encode_frame/1 - connection request" do
    test "encodes connection request with application version and no instance_id" do
      assert <<0::4, 8::4, rest::binary>> =
               Protocol.encode_frame({:connection_request, {"v1.0.0", nil}})

      assert {"v1.0.0", <<>>} = Protocol.Helpers.decode_chunk(rest)
    end

    test "encodes connection request with application version and instance_id" do
      assert <<0::4, 8::4, rest::binary>> =
               Protocol.encode_frame({:connection_request, {"v1.0.0", 1}})

      assert {"v1.0.0", <<1::32>>} = Protocol.Helpers.decode_chunk(rest)
    end

    test "can explicitly pass protocol version" do
      assert <<0::4, 8::4, rest::binary>> =
               Protocol.encode_frame({:connection_request, 8, {"v1.0.0", 1}})

      assert {"v1.0.0", <<1::32>>} = Protocol.Helpers.decode_chunk(rest)
    end

    test "only protocol version 8 is allowed" do
      assert_raise RuntimeError, "Received invalid protocol version: 7", fn ->
        Protocol.encode_frame({:connection_request, 7, {"v1.0.0", 1}})
      end
    end
  end

  describe "encode_frame/1 - connection response" do
    test "encodes connection response when successfully connected" do
      assert <<1::4, 0::4>> = Protocol.encode_frame({:connection_response, :connected})
    end

    test "encodes connection response when new client connection" do
      assert <<1::4, 1::4, 1::32>> =
               Protocol.encode_frame({:connection_response, {:hello_new_client, 1}})

      assert <<1::4, 1::4, 4_294_967_295::32>> =
               Protocol.encode_frame({:connection_response, {:hello_new_client, 4_294_967_295}})
    end

    test "encodes connection response when server identifiers mismatched" do
      assert <<1::4, 2::4>> = Protocol.encode_frame({:connection_response, :instance_id_mismatch})
    end

    test "encodes connection response when client is quarantined" do
      assert <<1::4, 3::4, "abc"::binary>> =
               Protocol.encode_frame({:connection_response, {:quarantined, "abc"}})
    end

    test "encodes connection response when protocol version is unsupported" do
      assert <<1::4, 4::4, 0::8>> =
               Protocol.encode_frame({:connection_response, {:unsupported_protocol_version, 0}})

      assert <<1::4, 4::4, 255::8>> =
               Protocol.encode_frame({:connection_response, {:unsupported_protocol_version, 255}})
    end

    test "encodes connection response when application version is unsupported" do
      assert <<1::4, 5::4>> =
               Protocol.encode_frame({:connection_response, :unsupported_application_version})
    end
  end

  describe "encode_frame/1 - other" do
    test "encodes ack message" do
      assert <<3::4, 0::12>> = Protocol.encode_frame({:ack, 0})
      assert <<3::4, 4095::12>> = Protocol.encode_frame({:ack, 4095})
    end

    test "encodes publish message" do
      assert <<4::4, 0::12>> = Protocol.encode_frame({:publish, 0, <<>>})
      assert <<4::4, 4095::12, "abc"::binary>> = Protocol.encode_frame({:publish, 4095, "abc"})
    end

    test "encodes ping message" do
      assert <<5::4, 0::12>> = Protocol.encode_frame(:ping)
    end

    test "encodes pong message" do
      assert <<6::4, 0::12>> = Protocol.encode_frame(:pong)
    end

    test "encodes nack message" do
      assert <<7::4, 0::12>> = Protocol.encode_frame({:nack, 0, <<>>})
      assert <<7::4, 4095::12, 1::32>> = Protocol.encode_frame({:nack, 4095, <<1::32>>})
    end
  end

  describe "decode_frame/1 - connection request" do
    test "decodes connection request with application version and no instance_id" do
      encoded = Protocol.encode_frame({:connection_request, {"v1.0.0", nil}})
      assert {:connection_request, 8, {"v1.0.0", nil}} = Protocol.decode_frame(encoded)
    end

    test "decodes connection request with application version and instance_id" do
      encoded = Protocol.encode_frame({:connection_request, {"v1.0.0", 1}})
      assert {:connection_request, 8, {"v1.0.0", 1}} = Protocol.decode_frame(encoded)
    end
  end

  describe "decode_frame/1 - connection response" do
    test "decodes connection response when successfully connected" do
      encoded = Protocol.encode_frame({:connection_response, :connected})
      assert {:connection_response, :connected} = Protocol.decode_frame(encoded)
    end

    test "decodes connection response when new client connection" do
      encoded = Protocol.encode_frame({:connection_response, {:hello_new_client, 1}})
      assert {:connection_response, {:hello_new_client, 1}} = Protocol.decode_frame(encoded)

      encoded = Protocol.encode_frame({:connection_response, {:hello_new_client, 4_294_967_295}})

      assert {:connection_response, {:hello_new_client, 4_294_967_295}} =
               Protocol.decode_frame(encoded)
    end

    test "decodes connection response when server identifiers mismatched" do
      encoded = Protocol.encode_frame({:connection_response, :instance_id_mismatch})
      assert {:connection_response, :instance_id_mismatch} = Protocol.decode_frame(encoded)
    end

    test "decodes connection response when client is quarantined" do
      encoded = Protocol.encode_frame({:connection_response, {:quarantined, "abc"}})
      assert {:connection_response, {:quarantined, "abc"}} = Protocol.decode_frame(encoded)
    end

    test "decodes connection response when protocol version is unsupported" do
      encoded = Protocol.encode_frame({:connection_response, {:unsupported_protocol_version, 0}})

      assert {:connection_response, {:unsupported_protocol_version, 0}} =
               Protocol.decode_frame(encoded)

      encoded =
        Protocol.encode_frame({:connection_response, {:unsupported_protocol_version, 255}})

      assert {:connection_response, {:unsupported_protocol_version, 255}} =
               Protocol.decode_frame(encoded)
    end

    test "decodes connection response when application version is unsupported" do
      encoded = Protocol.encode_frame({:connection_response, :unsupported_application_version})

      assert {:connection_response, :unsupported_application_version} =
               Protocol.decode_frame(encoded)
    end
  end

  describe "decode_frame/1 - other" do
    test "decodes ack message" do
      encoded = Protocol.encode_frame({:ack, 0})
      assert {:ack, 0} = Protocol.decode_frame(encoded)

      encoded = Protocol.encode_frame({:ack, 4095})
      assert {:ack, 4095} = Protocol.decode_frame(encoded)
    end

    test "decodes publish message" do
      encoded = Protocol.encode_frame({:publish, 0, <<>>})
      assert {:publish, 0, <<>>} = Protocol.decode_frame(encoded)

      encoded = Protocol.encode_frame({:publish, 4095, "abc"})
      assert {:publish, 4095, "abc"} = Protocol.decode_frame(encoded)
    end

    test "decodes ping message" do
      encoded = Protocol.encode_frame(:ping)
      assert :ping = Protocol.decode_frame(encoded)
    end

    test "decodes pong message" do
      encoded = Protocol.encode_frame(:pong)
      assert :pong = Protocol.decode_frame(encoded)
    end

    test "decodes nack message" do
      encoded = Protocol.encode_frame({:nack, 0, <<>>})
      assert {:nack, 0, <<>>} = Protocol.decode_frame(encoded)

      encoded = Protocol.encode_frame({:nack, 4095, <<1::32>>})
      assert {:nack, 4095, <<1::32>>} = Protocol.decode_frame(encoded)
    end
  end

  property "all messages can be encoded and decoded back to their original value" do
    check all message <- Sink.Generators.messages(), max_runs: 1000 do
      encoded = Protocol.encode_frame(message)
      decoded = Protocol.decode_frame(encoded)
      assert message == decoded
    end
  end
end
