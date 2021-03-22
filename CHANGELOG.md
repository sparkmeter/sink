# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [Unreleased]

### Added

- Adds telemetry hooks for connection start and stop events
- Splits Sink.Connection.Client into two processes - one to bring up the connection, another to handle connection data. Also traps exits which should allow notifying SinkHandler when a connection errors. [CH46694]
- Mox for testing
- Server processes will send the `sink_handler` a message when a connection goes up/down.
- Adds ServerConnectionHandler behavior
- Adds aes_key and controller_id to SystemConfig message


### Fixes

- If a client is connected and the same client attempts to connect this will kill the previous connection and boot that client.
