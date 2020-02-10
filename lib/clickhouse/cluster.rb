module Clickhouse
  class Cluster < BasicObject

    attr_reader :pond, :use_session, :session_connection

    def initialize(config)
      config = config.dup
      @use_session = config[:use_session]
      urls = config.delete(:urls) || config.delete("urls")
      urls.collect!{|url| ::Clickhouse::Utils.normalize_url(url)}
      urls.shuffle! if use_session

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
      use_session ? call_in_session(*args,&block) : call_with_retry(*args,&block)
    end

    def call_in_session *args, &block

      if @session_connection
        @session_connection.send(*args, &block)
      else
        pond.checkout do |connection|
          checked = connection.send(*args, &block)
          @session_connection = connection
          checked
        end
      end

    end

    def call_with_retry *args, &block
      begin
        pond.checkout do |connection|
          connection.send(*args, &block)
        end
      rescue ::Clickhouse::ConnectionError
        retry if pond.available.any?
      end
    end



  end
end
