defmodule Sink.Generators do
  @moduledoc """
  Streamdata generators useful for testing
  """
  use ExUnitProperties
  alias Sink.Connection.Protocol

  def messages do
    one_of([
      connection_request_message(0),
      connection_response_message(),
      ack_message(),
      publish_message(),
      ping_pong_message(),
      nack_message()
    ])
  end

  def connection_request_message(protocol_version) do
    gen all version <- string(:printable),
            maybe_instance_id <- one_of([constant(nil), instance_id()]) do
      {:connection_request, protocol_version, {version, maybe_instance_id}}
    end
  end

  def connection_response_message do
    gen all response <-
              one_of([
                constant(:connected),
                tuple({constant(:hello_new_client), instance_id()}),
                constant(:instance_id_mismatch),
                tuple({constant(:quarantined), binary()}),
                constant(:unsupported_protocol_version),
                constant(:unsupported_application_version)
              ]) do
      {:connection_response, response}
    end
  end

  def ack_message do
    gen all message_id <- message_id() do
      {:ack, message_id}
    end
  end

  def publish_message do
    gen all message_id <- message_id(), payload <- binary() do
      {:publish, message_id, payload}
    end
  end

  def ping_pong_message do
    member_of([:ping, :pong])
  end

  def nack_message do
    gen all message_id <- message_id(), payload <- binary() do
      {:nack, message_id, payload}
    end
  end

  def message_id do
    integer(0..4095)
  end

  def protocol_version do
    integer(0..15)
  end

  def supported_protocol_version do
    member_of(Protocol.supported_protocol_versions())
  end

  def unsupported_protocol_version do
    gen all protocol_version <- protocol_version(),
            protocol_version not in Protocol.supported_protocol_versions() do
      protocol_version
    end
  end

  def instance_id do
    integer(0..4_294_967_295)
  end

  def nack_data do
    gen all machine <- binary(),
            human <- string(:printable) do
      {machine, human}
    end
  end
end
