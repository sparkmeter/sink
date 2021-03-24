defmodule Slim.Event do
  @moduledoc """
  Behaviour for all slim handled events
  """
  @type event :: struct()
  @type key :: binary()
  @type offset :: non_neg_integer()

  @callback avro_schema() :: String.t()
  @callback key(event) :: key
  @callback offset(event) :: offset
  @callback set_key(event, key) :: event
  @callback set_offset(event, offset) :: event
end
