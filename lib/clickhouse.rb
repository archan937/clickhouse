require "uri"
require "forwardable"
require "csv"

require "faraday"
require "pond"

require "clickhouse/cluster"
require "clickhouse/connection"
require "clickhouse/error"
require "clickhouse/version"

module Clickhouse

  def self.logger=(logger)
    @logger = logger
  end

  def self.logger
    @logger if instance_variables.include?(:@logger)
  end

  def self.configurations=(configurations)
    @configurations = configurations.inject({}){|h, (k, v)| h[k.to_s] = v; h}
  end

  def self.configurations
    @configurations if instance_variables.include?(:@configurations)
  end

  def self.establish_connection(arg = {})
    config = arg.is_a?(Hash) ? arg : (configurations || {})[arg.to_s]
    if config
      connect!(config)
    else
      raise InvalidConnectionError, "Invalid connection specified: #{arg.inspect}"
    end
  end

  def self.connection
    @connection if instance_variables.include?(:@connection)
  end

# private

  def self.connect!(config)
    klass = (config[:urls] || config["urls"]) ? Cluster : Connection
    @connection = klass.new(config)
    @connection.connect!
  end

  private_class_method :connect!

end
