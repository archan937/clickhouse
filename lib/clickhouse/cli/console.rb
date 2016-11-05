require "readline"

module Clickhouse
  class CLI < Thor
    module Console
      extend self
      extend Client

      CLR = "\r\e[A\e[K"

      def run!(options = {})
        readline
      end

    # private

      def readline(buffer = nil)
        prompt = buffer ? ":-] " : ":) "
        line = Readline.readline(prompt, true)

        exit! unless line && !%w(exit quit).include?(line = line.strip)

        line = prettify(line)
        sql = [buffer, line].compact.join("\n").gsub(/\s+;$/, ";")

        puts "#{CLR}#{prompt}#{line}"
        alter_history(sql)

        buffer = begin
          execute(sql)
        rescue Clickhouse::Error => e
          puts "ERROR: #{e.message}"
        end

        readline buffer
      end

      def process_result(result, log)
        if result.is_a?(Clickhouse::Connection::Query::ResultSet)
          if result.size > 0
            array = [result.names].concat(result.to_a)
            lengths = array.inject([]) do |lengths, row|
              row.each_with_index do |value, index|
                length = value.to_s.strip.length
                lengths[index] = [lengths[index].to_i, length].max
              end
              lengths
            end
            puts
            array.each_with_index do |row, i|
              values = [nil]
              lengths.each_with_index do |length, index|
                values << row[index].to_s.ljust(length, " ")
              end
              values << nil
              separator = (i == 0) ? "+" : "|"
              puts values.join(" #{separator} ")
            end
          end
        else
          puts result == true ? "Ok." : (result || "Fail.")
        end

        if log
          puts
          puts log.strip
        end
        puts
      end

    end
  end
end
