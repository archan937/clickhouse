module Clickhouse

  class Error < StandardError
  end

  class ConnectionError < Error
  end

  class InvalidConnectionError < ConnectionError
  end

  class QueryError < Error
  end

  class InvalidQueryError < QueryError
  end

end
