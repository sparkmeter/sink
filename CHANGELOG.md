# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [Unreleased]

### Added

- Adds telemetry hooks for connection start and stop events

- Splits Sink.Connection.Client into two processes - one to bring up the connection, another to handle connection data. Also traps exits which should allow notifying SinkHandler when a connection errors. [CH46694]
