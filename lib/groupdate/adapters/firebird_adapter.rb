module Groupdate
  module Adapters
    class FirebirdAdapter < BaseAdapter
      def group_clause

        query =
          case period
          when :minute_of_hour
            ["EXTRACT(MINUTE FROM #{column})"]
          when :hour_of_day
            ["EXTRACT(HOUR FROM #{column})"]
          when :day_of_week
            ["EXTRACT(WEEKDAY FROM #{column})"]
          when :day_of_month
            ["EXTRACT(DAY FROM #{column})"]
          when :day_of_year
            ["EXTRACT(YEARDAY FROM #{column})"]
          when :month_of_year
            ["EXTRACT(MONTH FROM #{column})"]
          when :week
            ["EXTRACT(WEEK FROM #{column})"]
          when :custom
            ["", n_seconds, n_seconds]
          when :day
            ["CAST(#{column}) - EXTRACT(HOUR FROM #{column}))/24 - EXTRACT(MINUTE FROM #{column}))/1440 - EXTRACT(SECOND FROM #{column}))/86400 AS DATE)"]
          when :month
            ["CAST(((#{column} - (EXTRACT(DAY FROM #{column}) - 1)) - (EXTRACT(HOUR   FROM #{column}) / 24) - (EXTRACT(MINUTE FROM #{column}) / 1440) - (EXTRACT(SECOND FROM #{column}) / 86400)) AS DATE)"]
          when :quarter
            ["CAST(((#{column} - (EXTRACT(MONTH FROM #{column}) - 1) % 3 * 30) - (EXTRACT(HOUR   FROM #{column}) / 24) - (EXTRACT(MINUTE FROM #{column}) / 1440) - (EXTRACT(SECOND FROM #{column}) / 86400)) AS DATE)"]
          when :year
            ["CAST(((#{column} - (EXTRACT(DAY FROM #{column}) - 1)) - (EXTRACT(HOUR   FROM #{column}) / 24) - (EXTRACT(MINUTE FROM #{column}) / 1440) - (EXTRACT(SECOND FROM #{column}) / 86400)) AS DATE)"]
          end

        @relation.send(:sanitize_sql_array, query)
      end

    end
  end
end
