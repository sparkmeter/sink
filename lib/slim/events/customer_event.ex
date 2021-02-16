defmodule Slim.Events.CustomerEvent do
  defstruct id: nil,
            name: nil,
            portfolio_id: nil,
            code: nil,
            updated_by_id: nil,
            offset: nil,
            timestamp: nil

  @type t() :: %__MODULE__{
          id: <<_::128>>,
          name: String.t(),
          portfolio_id: <<_::128>> | nil,
          code: String.t() | nil,
          updated_by_id: <<_::16>>,
          offset: non_neg_integer(),
          timestamp: non_neg_integer()
        }

  def key(event), do: event.id

  def offset(event), do: event.offset

  def set_key(event, encoded_key), do: %__MODULE__{event | id: encoded_key}

  def set_offset(event, encoded_offset), do: %__MODULE__{event | offset: encoded_offset}
end
