module PLSQL
  class PipelinedFunctionCall < SubprogramCall
    def exec(&block)
      @cursor = @schema.connection.parse(@sql)

      @binds[:values].each do |arg, value|
        @cursor.bind_param(":#{arg}", value, @binds[:metadata][arg])
      end

      @cursor.exec

      if block_given?
        fetch_all_rows.each(&block)
        nil
      else
        fetch_all_rows
      end
    ensure
      @cursor.close if @cursor
    end

    private

    def construct_sql(arguments)
      prepare_sql_construction
      @sql = <<-SQL
        SELECT *
        FROM   TABLE(#{full_subprogram_name}(#{add_arguments(arguments)}))
      SQL
    end

    def fetch_all_rows
      result = []
      cols = @cursor.raw_cursor.get_col_names.map { |name| name =~ /[a-z]/ ? name : name.downcase }

      while (row = @cursor.fetch)
        tmp_hash = {}
        cols.each_with_index do |col, i|
          tmp_hash[col] =
            case row[i]
            when OCI8::LOB
              row[i].read
            when OraDate
              row[i].to_time
            when OraNumber
              row[i].to_s.to_d
            else
              row[i]
            end
        end

        result << tmp_hash
      end

      result
    end

    def add_arguments(arguments)
      if (@schema.connection.database_version <=> [11, 0, 0, 0]) > 0
        super
      else
        if arguments.first.is_a?(Hash)
          # in 10g you cannot use named arguments in SQL for pipelined functions
          arguments = make_sequential_arguments(arguments.first)
          add_sequential_arguments(arguments)
        else
          super
        end
      end
    end

    def make_sequential_arguments(arguments)
      record_fields_sorted_by_position(arguments_metadata).map {|name| arguments[name] }
    end
  end
end