module Clickhouse
  class CLI < Thor
    module Client

      HISTORY_FILE = "#{ENV["HOME"]}/.clickhouse_history"

      def self.included(base)
        extended(base)
      end

      def self.extended(base)
        Clickhouse.logger = self
        load_history
      end

      def self.debug(message = nil)
        @log = message.split("\n").detect{|line| line.include?("36m")}
      end

      def self.log
        @log
      end

      def self.load_history
        File.readlines(HISTORY_FILE).each do |line|
          Readline::HISTORY.push line.gsub(";;", "\n").strip
        end if File.exists?(HISTORY_FILE)
      end

      def alter_history(sql, current = true)
        (Readline::HISTORY.to_a.count{|line| line[-1] != ";"} + (current ? 1 : 0)).times do
          Readline::HISTORY.pop
        end
        unless Readline::HISTORY.to_a[-1] == sql
          Readline::HISTORY.push(sql)
        end
      end

      def dump_history
        File.open(HISTORY_FILE, "w+") do |file|
          Readline::HISTORY.each do |line|
            file.puts line.strip.gsub("\n", ";;")
          end
        end
      end

      def prettify(sql)
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
        ).sort{|a, b| [b.size, a] <=> [a.size, b]}

        sql.gsub!(/(\b)(#{preserved_words.join("|")})(\b)/i) do
          "#{$1}#{$2.upcase}#{$3}"
        end

        interpolate_patterns(sql, replaced)
      end

      def numerize_patterns(sql, replaced = [])
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

      def interpolate_patterns(sql, replaced)
        matched = false

        sql = sql.gsub(/(\$|%)\{(\d+)\}/) do |match|
          matched = true
          replaced[$2.to_i]
        end

        matched ? interpolate_patterns(sql, replaced) : sql
      end

      def execute(sql, &block)
        if sql[-1] == ";"
          dump_history
          method = sql.match(/^(SELECT|SHOW|DESCRIBE)/i) ? :query : :execute
          result = Clickhouse.connection.send(method, sql[0..-2])

          if block_given?
            block.call(result, Client.log)
          else
            process_result(result, Client.log)
          end
        else
          sql
        end
      end

    end
  end
end
