Mox.defmock(Sink.Connection.Transport.SSLMock, for: Sink.Connection.Transport)

Mox.defmock(Sink.Connection.ServerConnectionHandlerMock,
  for: Sink.Connection.ServerConnectionHandler
)

Mox.defmock(Sink.Connection.ClientConnectionHandlerMock,
  for: Sink.Connection.ClientConnectionHandler
)

Mox.defmock(Sink.Connection.Client.BackoffMock,
  for: Sink.Connection.Client.Backoff
)

Mox.defmock(Sink.Connection.ServerHandlerMock,
  for: :ranch_protocol
)
