defmodule SlimTest do
  use ExUnit.Case, async: true
  alias Slim.Events

  @cloud_credit_id "00000000-3b9e-4135-ae3f-6d3ee79ea197" |> Ecto.UUID.dump() |> elem(1)
  @cm_config_id "44445ae7-3b9e-4135-ae3f-6d3ee79ea197" |> Ecto.UUID.dump() |> elem(1)
  @customer_id "0cb15ae7-3b9e-4135-ae3f-6d3ee79ea197" |> Ecto.UUID.dump() |> elem(1)
  @gid_mac Varint.LEB128.encode(1234)
  @meter_id "00015ae7-3b9e-4135-ae3f-6d3ee79ea197" |> Ecto.UUID.dump() |> elem(1)
  @portfolio_id "11111ae7-3b9e-4135-ae3f-6d3ee79ea197" |> Ecto.UUID.dump() |> elem(1)
  @tariff_id "0cb15ae7-3b9e-4135-ae3f-6d3ee79ea197" |> Ecto.UUID.dump() |> elem(1)
  @user_id "8cb15ae7-3b9e-4135-ae3f-6d3ee79ea197" |> Ecto.UUID.dump() |> elem(1)

  describe "encode/decode events" do
    test "CloudCreditEvent" do
      event = %Events.CloudCreditEvent{
        cloud_credit_id: @cloud_credit_id,
        customer_id: @customer_id,
        amount: 1_000,
        balance: 1_000_000,
        portfolio_id: @portfolio_id,
        external_identifier: "payment 1",
        memo: "this is a payment",
        offset: 1,
        updated_by_id: @user_id,
        credited_at: 1_586_632_400,
        timestamp: 1_586_632_500
      }

      assert {:ok, event_type_id, key, offset, event_data} = Slim.encode_event(event)
      assert @customer_id == key
      assert event.offset == offset
      assert {:ok, event} == Slim.decode_event({event_type_id, key}, offset, event_data)
    end

    test "CustomerEvent (with optional fields)" do
      event = %Events.CustomerEvent{
        id: @customer_id,
        name: "Homer Simpson",
        portfolio_id: @portfolio_id,
        code: "test",
        offset: 1,
        updated_by_id: @user_id
      }

      assert {:ok, event_type_id, key, offset, event_data} = Slim.encode_event(event)
      assert event.id == key
      assert event.offset == offset
      assert {:ok, event} == Slim.decode_event({event_type_id, key}, offset, event_data)
    end

    test "CustomerEvent (without optional fields)" do
      event = %Events.CustomerEvent{
        id: @customer_id,
        name: "Homer Simpson",
        portfolio_id: nil,
        code: nil,
        offset: 1,
        updated_by_id: @user_id
      }

      assert {:ok, event_type_id, key, offset, event_data} = Slim.encode_event(event)
      assert event.id == key
      assert event.offset == offset
      assert {:ok, event} == Slim.decode_event({event_type_id, key}, offset, event_data)
    end

    test "CustomerMeterBillEvent" do
      event = %Events.CustomerMeterBillEvent{
        customer_meter_config_id: @cm_config_id,
        customer_meter_config_offset: 3,
        meter_reading_offset: 2,
        amount: 1_000,
        balance: 1_000_000,
        timestamp: 1_586_632_500,
        offset: 1
      }

      assert {:ok, event_type_id, key, offset, event_data} = Slim.encode_event(event)
      assert event.customer_meter_config_id == key
      assert event.offset == offset
      assert {:ok, event} == Slim.decode_event({event_type_id, key}, offset, event_data)
    end

    test "MeterEvent" do
      event = %Events.MeterEvent{
        id: @meter_id,
        serial_number: "SM60R-01-00001551",
        address: "742 Evergreen Terrace",
        offset: 1,
        updated_by_id: @user_id
      }

      assert {:ok, event_type_id, key, offset, event_data} = Slim.encode_event(event)
      assert event.id == key
      assert event.offset == offset
      assert {:ok, event} == Slim.decode_event({event_type_id, key}, offset, event_data)
    end

    test "CustomerMeterConfigEvent (with optional fields)" do
      event = %Events.CustomerMeterConfigEvent{
        id: @cm_config_id,
        meter_id: @meter_id,
        customer_id: @customer_id,
        tariff_id: @tariff_id,
        operating_mode: "auto",
        offset: 1,
        updated_by_id: @user_id
      }

      assert {:ok, event_type_id, key, offset, event_data} = Slim.encode_event(event)
      assert event.id == key
      assert event.offset == offset
      assert {:ok, event} == Slim.decode_event({event_type_id, key}, offset, event_data)
    end

    test "CustomerMeterConfigEvent (without optional fields)" do
      event = %Events.CustomerMeterConfigEvent{
        id: @cm_config_id,
        meter_id: nil,
        customer_id: @customer_id,
        tariff_id: nil,
        operating_mode: "auto",
        offset: 1,
        updated_by_id: @user_id
      }

      assert {:ok, event_type_id, key, offset, event_data} = Slim.encode_event(event)
      assert event.id == key
      assert event.offset == offset
      assert {:ok, event} == Slim.decode_event({event_type_id, key}, offset, event_data)
    end

    test "MeterConfigEvent" do
      event = %Events.MeterConfigEvent{
        meter_id: @meter_id,
        enabled: true,
        power_limit: 1_000.0,
        current_limit: 65.0,
        startup_delay: 0,
        throttle_on_time: 10,
        throttle_off_time: 60,
        throttle_count_limit: 10,
        offset: 1,
        updated_at: 1_586_632_500
      }

      assert {:ok, event_type_id, key, offset, event_data} = Slim.encode_event(event)
      assert event.meter_id == key
      assert event.offset == offset
      assert {:ok, event} == Slim.decode_event({event_type_id, key}, offset, event_data)
    end

    test "MeterConfigEvent 2" do
      # there is maybe a bug with avrora where if the first byte is 0 it doesn't decode properly
      event = %Slim.Events.MeterConfigEvent{
        current_limit: 65.0,
        enabled: false,
        meter_id: <<153, 145, 90, 231, 59, 158, 65, 53, 174, 63, 109, 62, 231, 158, 161, 151>>,
        offset: 2,
        power_limit: 1.0e3,
        startup_delay: 0,
        throttle_count_limit: 10,
        throttle_off_time: 60,
        throttle_on_time: 10,
        updated_at: 1_612_828_123
      }

      assert {:ok, event_type_id, key, offset, event_data} = Slim.encode_event(event)
      assert event.meter_id == key
      assert event.offset == offset
      assert {:ok, event} == Slim.decode_event({event_type_id, key}, offset, event_data)
    end

    test "MeterConfigAppliedEvent" do
      event = %Events.MeterConfigAppliedEvent{
        meter_id: @meter_id,
        last_meter_config_offset: 5,
        offset: 3,
        updated_at: 1_586_632_500
      }

      assert {:ok, event_type_id, key, offset, event_data} = Slim.encode_event(event)
      assert event.meter_id == key
      assert event.offset == offset
      assert {:ok, event} == Slim.decode_event({event_type_id, key}, offset, event_data)
    end

    test "MeterReadingEvent" do
      event = %Events.MeterReadingEvent{
        apparent_power_avg: 0.0,
        current_avg: 0.0,
        current_max: 0.0,
        current_min: 0.0,
        energy: 0.0,
        frequency: 0.0,
        gid_mac: @gid_mac,
        offset: 1,
        period_end: 1_586_632_500,
        period_start: 1_586_631_600,
        power_factor_avg: 0.0,
        state: "ElectricalMeterStateOn",
        true_power_avg: 0.0,
        true_power_inst: 0.0,
        uptime_secs: 99_999,
        voltage_avg: 0.0,
        voltage_max: 0.0,
        voltage_min: 0.0
      }

      assert {:ok, event_type_id, key, offset, event_data} = Slim.encode_event(event)

      assert event.gid_mac == key
      assert event.offset == offset
      assert {:ok, event} == Slim.decode_event({event_type_id, key}, offset, event_data)
    end

    test "SystemConfigEvent" do
      event = %Events.SystemConfigEvent{
        nerves_hub_link_enabled: true,
        offset: 1,
        updated_at: 1_586_632_500,
        updated_by_id: @user_id
      }

      assert {:ok, event_type_id, key, offset, event_data} = Slim.encode_event(event)
      assert <<>> == key
      assert event.offset == offset
      assert {:ok, event} == Slim.decode_event({event_type_id, key}, offset, event_data)
    end

    test "TariffEvent" do
      event = %Events.TariffEvent{
        id: @tariff_id,
        name: "Tariff 1",
        load_limit: 9_000,
        rate: :erlang.float_to_binary(10.50),
        offset: 1,
        updated_by_id: @user_id
      }

      assert {:ok, event_type_id, key, offset, event_data} = Slim.encode_event(event)
      assert event.id == key
      assert event.offset == offset
      assert {:ok, event} == Slim.decode_event({event_type_id, key}, offset, event_data)
    end

    test "UpdateFirmwareEvent" do
      event = %Events.UpdateFirmwareEvent{
        to: "nerves_hub",
        offset: 1,
        updated_at: 1_586_632_500,
        updated_by_id: @user_id
      }

      assert {:ok, event_type_id, key, offset, event_data} = Slim.encode_event(event)
      assert <<>> == key
      assert event.offset == offset
      assert {:ok, event} == Slim.decode_event({event_type_id, key}, offset, event_data)
    end

    test "UserEvent" do
      event = %Events.UserEvent{
        id: @user_id,
        email: "slim@sparkmeter.io",
        offset: 1,
        username: "slim",
        shared_secret: "12345",
        updated_by_id: @user_id
      }

      assert {:ok, event_type_id, key, offset, event_data} = Slim.encode_event(event)
      assert event.id == key
      assert event.offset == offset
      assert {:ok, event} == Slim.decode_event({event_type_id, key}, offset, event_data)
    end
  end
end
