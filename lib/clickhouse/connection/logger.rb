module Clickhouse
  class Connection
    module Logger
    private

      def log(type, msg)
        Clickhouse.logger.send(type, msg) if Clickhouse.logger
      end

    end
  end
end
