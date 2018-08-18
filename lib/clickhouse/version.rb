module Clickhouse
  MAJOR = 0
  MINOR = 1
  TINY  = 10

  VERSION = [MAJOR, MINOR, TINY].join(".") + '-sessionId'
end
