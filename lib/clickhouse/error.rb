module Clickhouse

  class Error < StandardError
  end

  class InvalidConnectionError < Error
  end

  class ConnectionError < Error
  end

end
