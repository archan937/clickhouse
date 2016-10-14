require "clickhouse/version"

module Clickhouse

  def self.logger=(logger)
    @logger = logger
  end

  def self.logger
    @logger
  end

  def self.configurations=(configurations)
    @configurations = configurations.inject({}){|h, (k, v)| h[k.to_s] = v; h}
  end

  def self.configurations
    @configurations
  end

end
