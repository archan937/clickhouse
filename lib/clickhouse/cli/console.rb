require "readline"

module Clickhouse
  class CLI < Thor
    class Console

      HISTORY_FILE = "#{ENV["HOME"]}/.clickhouse_history"
      CLR = "\r\e[A\e[K"

      def self.run!(options = {})
        Clickhouse.logger = self
        load_history
        readline
      end

      def self.info(message)
        @log = message.split("\n").detect{|line| line.include?("36m")}
      end

    # private

      def self.load_history
        File.readlines(HISTORY_FILE).each do |line|
          Readline::HISTORY.push line.gsub(";;", "\n").strip
        end if File.exists?(HISTORY_FILE)
      end

      def self.alter_history(sql)
        Readline::HISTORY.pop
        Readline::HISTORY.to_a.count{|line| line[-1] != ";"}.times do
          Readline::HISTORY.pop
        end
        Readline::HISTORY.push(sql)
      end

      def self.dump_history
        File.open(HISTORY_FILE, "w+") do |file|
          Readline::HISTORY.each do |line|
            file.puts line.strip.gsub("\n", ";;")
          end
        end
      end

      def self.readline(buffer = nil)
        prompt = buffer ? ":-] " : ":) "
        line = Readline.readline(prompt, true)

        exit! unless line && !%w(exit quit).include?(line = line.strip)
        sql = [buffer, prettify(line)].compact.join("\n").gsub(/\s+;$/, ";")
        puts "#{CLR}#{prompt}#{prettify(line)}"

        alter_history(sql)
        readline execute(sql)
      end

      def self.prettify(sql)
        sql, replaced = numerize_patterns(sql)

        preserved_words = %w(
          USE
          SHOW
          DATABASES
          TABLES
          PROCESSLIST
          INSERT
          INTO
          FORMAT
          SELECT
          COUNT
          DISTINCT
          SAMPLE
          AS
          FROM
          JOIN
          UNION
          ALL
          PREWHERE
          WHERE
          AND
          OR
          NOT
          IN
          GROUP
          BY
          HAVING
          ORDER
          LIMIT
          CREATE
          DESCRIBE
          ALTER
          RENAME
          DROP
          DETACH
          ATTACH
          TABLE
          VIEW
          PARTITION
          EXISTS
          SET
          OPTIMIZE
          WITH
          TOTALS
        )

        sql.gsub!(/(#{preserved_words.join("|")})/i) do |match|
          match.upcase
        end

        interpolate_patterns(sql, replaced)
      end

      def self.numerize_patterns(sql, replaced = [])
        sql = sql.gsub(/(["'])(?:(?=(\\?))\2.)*?\1/) do |match|
          replaced << match
          "${#{replaced.size - 1}}"
        end

        parenthesized = false

        sql = sql.gsub(/\([^\(\)]*?\)/) do |match|
          parenthesized = true
          replaced << match
          "%{#{replaced.size - 1}}"
        end

        parenthesized ? numerize_patterns(sql, replaced) : [sql, replaced]
      end

      def self.interpolate_patterns(sql, replaced)
        matched = false

        sql = sql.gsub(/($|%)\{(\d+)\}/) do |match|
          matched = true
          replaced[$1.to_i]
        end

        matched ? interpolate_patterns(sql, replaced) : sql
      end

      def self.execute(sql)
        if sql[-1] == ";"
          dump_history
          method = sql.match(/^(SELECT|SHOW|DESCRIBE)/i) ? :query : :execute
          print_result Clickhouse.connection.send(method, sql[0..-2])
          @log = nil
        else
          sql
        end
      rescue Clickhouse::Error => e
        puts "ERROR: #{e.message}"
      end

      def self.print_result(result_set)
        array = [result_set.names].concat(result_set.to_a)

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

        if @log
          puts
          puts @log.strip
        end

        puts
      end

      private_class_method :load_history
      private_class_method :alter_history
      private_class_method :dump_history
      private_class_method :readline
      private_class_method :prettify
      private_class_method :numerize_patterns
      private_class_method :interpolate_patterns
      private_class_method :execute
      private_class_method :print_result

    end
  end
end

# puts "\u{250C}\u{2500} name \u{2500}\u{252C}\u{2500} log_id \u{2500}\u{2510}\n\u{2502} Paul   \u{2502} 2e84c    \u{2502}\n\u{2514}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2534}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2518}"
