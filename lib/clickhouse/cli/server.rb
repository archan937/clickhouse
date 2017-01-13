require "sinatra"
require "erubis"

module Clickhouse
  class CLI < Thor
    class Server < Sinatra::Base
      include Client

      set :views, File.expand_path("../server/views", __FILE__)
      set :public_folder, File.expand_path("../server/assets", __FILE__)

      get "/" do
        erb :index
      end

      post "/" do
        sql = prettify(params[:sql]).gsub(/\s+;$/, ";")
        alter_history(sql, false)
        begin
          execute(sql) do |result, log|
            content_type :json
            {
              :urls => Clickhouse.connection.pond.available.collect(&:url),
              :history => Readline::HISTORY.to_a.collect(&:strip),
              :names => result.names,
              :data => result.to_a,
              :stats => log.sub("\e[1m\e[36m", "").sub("\e[0m", "").strip
            }.to_json
          end
        rescue Clickhouse::Error => e
          halt 500, e.message
        end
      end

    end
  end
end
