defmodule Sink.Generators do
  @moduledoc """
  Streamdata generators useful for testing
  """
  use ExUnitProperties

  def messages do
    one_of([
      connection_request_message(),
      connection_response_message(),
      ack_message(),
      publish_message(),
      ping_pong_message(),
      nack_message()
    ])
  end

  def connection_request_message do
    gen all version <- string(:printable),
            maybe_server_identifier <- one_of([constant(nil), server_identifier()]) do
      {:connection_request, 8, {version, maybe_server_identifier}}
    end
  end

  def connection_response_message do
    gen all response <-
              one_of([
                constant(:connected),
                tuple({constant(:hello_new_client), server_identifier()}),
                constant(:server_identifier_mismatch),
                tuple({constant(:quarantined), binary()}),
                tuple({constant(:unsupported_protocol_version), protocol_version()}),
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
    integer(0..255)
  end

  def server_identifier do
    integer(0..4_294_967_295)
  end

  def nack_data do
    gen all machine <- binary(),
            human <- string(:printable) do
      {machine, human}
    end
  end
end
