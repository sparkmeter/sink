# Protocol Description

This document describes the Sink protocol. It is a work in progress.

## Connection Request

When a client connects to a server is sends a connection request to tell the server what protocol version it wants to use and to confirm the client and server have not reset their database or been reset since the last connection. This prevents mistakes such as pointing a device that used to connect to a staging environment from sending data to productino or a device that has been wiped from receiving or sending events to a server that expects the device to be in a different state.

## Connection Response

The server responds to a client's connection with one of the following responses:

### OK

The server agrees that the client is who the server expects and the server is who the client expected.

### Hello New Client

The server has accepted the new clien'ts connection and is telling the client its `instantiated_at` value.

### Mismatched Client

The client's instantiated_at was not what the server expected. Events should not be sent or received.

### Mismatched Server

The server's instantiated_at was not what the client expected. Events should not be sent or received.

### Quarantined

The client should either disconnect or stay connected with an infrequent ping, but not send or receive messages. The server will not send messages to or receive messages from the client until the client has been removed from quarantine by the server. The client is removed from quarantine when the server sends a `UNQUARANTINED` connection response.

### Unquarantined

The client has been removed from quarantine and may send and receive events.

### Unsupported Protocol Version

The client requested to use a protocol version the server does not support. The client should either disconnect or stay connected with an infrequent ping.

## PUBLISH

Send a single event.

Requires an ACK or NACK response.

## ACK

Acknowledge a single event.

An ACK means everything was received and processed as expected and the subscription can consider the event delivered.

## NACK

Report a problem with a single event.

A NACK should be considered to be an exceptional state and contains information about why the event was NACK'd. There is a field for a "machine message", which is meant to be a machine readable binary message, like a http status,  determined by the implementer. The purpose is to have known error codes which might have a recovery path (ex: upgrading a device's firmware) There is also a field for a "human message", which is meant to be a human friendly description of the error and may be blank if bandwidth usage is a concern.

## PING

Request a pong from a peer.

### PONG

Respond to a pong from a peer.