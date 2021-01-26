defmodule Sink.TestMigrations do
  use Ecto.Migration

  def change do
    create table(:test_log, primary_key: false) do
      add(:key, :binary, null: false, primary_key: true)
      add(:event_type, :string, null: false, primary_key: true)
      add(:offset, :integer, null: false, primary_key: true)
      add(:serialized, :binary)
    end

    create table(:test_ecto_generic_event_log, primary_key: false) do
      add(:key, :binary, null: false, primary_key: true)
      add(:event_type_id, :integer, null: false, primary_key: true)
      add(:offset, :integer, null: false, primary_key: true)
      add(:event_data, :binary)
    end

    create table(:test_ecto_client_event_log, primary_key: false) do
      add(:client_id, :string, null: false, primary_key: true)
      add(:key, :binary, null: false, primary_key: true)
      add(:event_type_id, :integer, null: false, primary_key: true)
      add(:offset, :integer, null: false, primary_key: true)
      add(:event_data, :binary)
    end

    create table(:test_client_log, primary_key: false) do
      add(:key, :binary, null: false, primary_key: true)
      add(:client_id, :string, null: false, primary_key: true)
      add(:event_type, :string, null: false, primary_key: true)
      add(:offset, :integer, null: false, primary_key: true)
      add(:serialized, :binary)
    end

    create table(:test_log_subscription, primary_key: false) do
      add(:key, :binary, null: false, primary_key: true)
      add(:client_id, :string, null: false, primary_key: true)
      add(:event_type, :string, null: false, primary_key: true)
      add(:consumer_offset, :integer, null: false)
      add(:producer_offset, :integer, null: false)
    end

    create table(:test_ecto_generic_event_subscriptions, primary_key: false) do
      add(:key, :binary, null: false, primary_key: true)
      add(:event_type_id, :integer, null: false, primary_key: true)
      add(:consumer_offset, :integer, null: false)
      add(:producer_offset, :integer, null: false)
    end

    create table(:test_ecto_client_event_subscriptions, primary_key: false) do
      add(:client_id, :string, null: false, primary_key: true)
      add(:key, :binary, null: false, primary_key: true)
      add(:event_type_id, :integer, null: false, primary_key: true)
      add(:consumer_offset, :integer, null: false)
      add(:producer_offset, :integer, null: false)
    end

    create table(:test_ecto_event_type_configs, primary_key: false) do
      add(:event_type_id, :integer, null: false, primary_key: true)
      add(:order, :integer, null: false)
    end

    # for end to end server - client tests

    # tables on the cloud

    create table(:cloud_outgoing_log, primary_key: false) do
      add(:key, :binary, null: false, primary_key: true)
      add(:client_id, :string, null: false, primary_key: true)
      add(:event_type, :string, null: false, primary_key: true)
      add(:offset, :integer, null: false, primary_key: true)
      add(:serialized, :binary)
    end

    create table(:cloud_outgoing_subscription, primary_key: false) do
      add(:key, :binary, null: false, primary_key: true)
      add(:client_id, :string, null: false, primary_key: true)
      add(:event_type, :string, null: false, primary_key: true)
      add(:consumer_offset, :integer, null: false)
      add(:producer_offset, :integer, null: false)
    end

    create table(:ground_incoming_log, primary_key: false) do
      add(:key, :binary, null: false, primary_key: true)
      add(:client_id, :string, null: false, primary_key: true)
      add(:event_type, :string, null: false, primary_key: true)
      add(:offset, :integer, null: false, primary_key: true)
      add(:serialized, :binary)
    end

    create table(:ground_incoming_subscription, primary_key: false) do
      add(:key, :binary, null: false, primary_key: true)
      add(:client_id, :string, null: false, primary_key: true)
      add(:event_type, :string, null: false, primary_key: true)
      add(:consumer_offset, :integer, null: false)
      add(:producer_offset, :integer, null: false)
    end

    # tables on the ground

    create table(:cloud_incoming_log, primary_key: false) do
      add(:key, :binary, null: false, primary_key: true)
      add(:event_type, :string, null: false, primary_key: true)
      add(:offset, :integer, null: false, primary_key: true)
      add(:serialized, :binary)
    end

    create table(:cloud_incoming_subscription, primary_key: false) do
      add(:key, :binary, null: false, primary_key: true)
      add(:event_type, :string, null: false, primary_key: true)
      add(:consumer_offset, :integer, null: false)
      add(:producer_offset, :integer, null: false)
    end

    create table(:ground_outgoing_log, primary_key: false) do
      add(:key, :binary, null: false, primary_key: true)
      add(:event_type, :string, null: false, primary_key: true)
      add(:offset, :integer, null: false, primary_key: true)
      add(:serialized, :binary)
    end

    create table(:ground_outgoing_subscription, primary_key: false) do
      add(:key, :binary, null: false, primary_key: true)
      add(:event_type, :string, null: false, primary_key: true)
      add(:consumer_offset, :integer, null: false)
      add(:producer_offset, :integer, null: false)
    end
  end
end
