ExUnit.start()

# Configure the mock module for testing
Application.put_env(:ex_pmtiles, :pmtiles_module, ExPmtiles.CacheMock)

Mox.defmock(ExPmtiles.CacheMock, for: ExPmtiles.Behaviour)
