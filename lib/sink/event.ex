defmodule Sink.Event do
  @moduledoc """
  An event, which can be sent by sink.
  """
  # TODO consider moving to a behaviour implemented by the event log schemas
  # once the event logs return those instead of tuples
  @type t :: %__MODULE__{
          event_type_id: non_neg_integer(),
          key: binary(),
          offset: non_neg_integer(),
          timestamp: non_neg_integer(),
          event_data: binary(),
          schema_version: non_neg_integer()
        }
  fields = [:event_type_id, :key, :offset, :timestamp, :event_data, :schema_version]
  @enforce_keys fields
  defstruct fields
end
