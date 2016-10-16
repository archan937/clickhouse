module Clickhouse

  class Error < StandardError
  end

  class InvalidConnectionError < Error
  end

  class ConnectionError < Error
  end

  class InvalidQueryError < Error
  end

  class QueryError < Error
  end

end
