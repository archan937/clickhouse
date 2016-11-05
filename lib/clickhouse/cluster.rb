module Clickhouse
  class Cluster < BasicObject

    attr_reader :pond

    def initialize(config)
      config = config.dup
      urls = config.delete(:urls) || config.delete("urls")
      urls.collect!{|url| ::Clickhouse::Utils.normalize_url(url)}

      @pond = ::Pond.new :maximum_size => urls.size, :timeout => 5.0
      block = ::Proc.new do
        url = (urls - pond.available.collect(&:url)).first || urls.sample
        ::Clickhouse::Connection.new(config.merge(:url => url))
      end

      pond.instance_variable_set :@block, block
      pond.maximum_size.times do
        pond.available << block.call
      end

      pond.detach_if = ::Proc.new do |connection|
        begin
          connection.ping!
          false
        rescue
          true
        end
      end
    end

  private

    def method_missing(*args, &block)
      pond.checkout do |connection|
        connection.send(*args, &block)
      end
    rescue ::Clickhouse::ConnectionError
      retry if pond.available.any?
    end

  end
end
