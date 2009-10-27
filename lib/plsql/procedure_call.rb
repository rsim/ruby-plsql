module PLSQL
  class ProcedureCall

    def initialize(procedure, args = [])
      @procedure = procedure
      @overload = get_overload_from_arguments_list(args)
      construct_sql(args)
    end

    def exec
      @cursor = @procedure.schema.connection.parse(@sql)

      @bind_values.each do |arg, value|
        @cursor.bind_param(":#{arg}", value, @bind_metadata[arg])
      end

      if return_metadata
        @cursor.bind_param(":return", nil, return_metadata)
      end

      @cursor.exec

      get_return_value
    ensure
      @cursor.close if @cursor
    end

    private

    def get_overload_from_arguments_list(args)
      # find which overloaded definition to use
      # if definition is overloaded then match by number of arguments
      if @procedure.overloaded?
        # named arguments
        if args.size == 1 && args[0].is_a?(Hash)
          number_of_args = args[0].keys.size
          overload = overload_argument_list.keys.detect do |ov|
            overload_argument_list[ov].size == number_of_args &&
            overload_arguments[ov].keys.sort_by{|k| k.to_s} == args[0].keys.sort_by{|k| k.to_s}
          end
        # sequential arguments
        # TODO: should try to implement matching by types of arguments
        else
          number_of_args = args.size
          overload = overload_argument_list.keys.detect do |ov|
            overload_argument_list[ov].size == number_of_args
          end
        end
        raise ArgumentError, "Wrong number of arguments passed to overloaded PL/SQL procedure" unless overload
        overload
      else
        0
      end
    end

    def construct_sql(args)
      @declare_sql = "DECLARE\n"
      @assignment_sql = "BEGIN\n"
      @call_sql = ""
      @call_sql << ":return := " if return_metadata
      @call_sql << "#{schema_name}." if schema_name
      @call_sql << "#{package_name}." if package_name
      @call_sql << "#{procedure_name}("

      @bind_values = {}
      @bind_metadata = {}

      # Named arguments
      if args.size == 1 && args[0].is_a?(Hash) &&
            # do not use named arguments if procedure has just one PL/SQL record argument -
            # in that case passed Hash should be used as value for this PL/SQL record argument
            # (which will be processed in sequential arguments bracnh)
            !(argument_list.size == 1 &&
              arguments[(only_argument=argument_list[0])][:data_type] == 'PL/SQL RECORD' &&
              args[0].keys != [only_argument])
        @call_sql << args[0].map do |arg, value|
          "#{arg} => " << add_argument(arg, value)
        end.join(', ')

      # Sequential arguments
      else
        argument_count = argument_list.size
        raise ArgumentError, "Too many arguments passed to PL/SQL procedure" if args.size > argument_count
        # Add missing arguments with nil value
        args += [nil] * (argument_count - args.size) if args.size < argument_count
        @call_sql << (0...args.size).map do |i|
          arg = argument_list[i]
          value = args[i]
          add_argument(arg, value)
        end.join(', ')
      end

      @call_sql << ");\n"
      @sql = "" << @declare_sql << @assignment_sql << @call_sql << "END;\n"
      # puts "DEBUG: sql = #{@sql.gsub "\n", "<br/>\n"}"
    end

    def add_argument(arg, value)
      argument_metadata = arguments[arg]
      raise ArgumentError, "Wrong argument passed to PL/SQL procedure" unless argument_metadata
      case argument_metadata[:data_type]
      when 'PL/SQL RECORD'
        @declare_sql << record_declaration_sql(arg, argument_metadata)
        record_assignment_sql, record_bind_values, record_bind_metadata =
          record_assignment_sql_values_metadata(arg, argument_metadata, value)
        @assignment_sql << record_assignment_sql
        @bind_values.merge!(record_bind_values)
        @bind_metadata.merge!(record_bind_metadata)
        "l_#{arg}"
      else
        @bind_values[arg] = value
        @bind_metadata[arg] = argument_metadata
        ":#{arg}"
      end
    end

    def record_declaration_sql(argument, argument_metadata)
      fields_metadata = argument_metadata[:fields]
      sql = "TYPE t_#{argument} IS RECORD (\n"
      fields_sorted_by_position = fields_metadata.keys.sort_by{|k| fields_metadata[k][:position]}
      sql << fields_sorted_by_position.map do |field|
        metadata = fields_metadata[field]
        "#{field} #{type_to_sql(metadata)}"
      end.join(",\n")
      sql << ");\n"
      sql << "l_#{argument} t_#{argument};\n"
    end

    def record_assignment_sql_values_metadata(argument, argument_metadata, record_value)
      sql = ""
      bind_values = {}
      bind_metadata = {}
      argument_metadata[:fields].each do |field, metadata|
        if value = record_value[field] || record_value[field.to_s]
          bind_variable = :"#{argument}_#{field}"
          sql << "l_#{argument}.#{field} := :#{bind_variable};\n"
          bind_values[bind_variable] = value
          bind_metadata[bind_variable] = metadata
        end
      end
      [sql, bind_values, bind_metadata]
    end

    def type_to_sql(metadata)
      case metadata[:data_type]
      when 'NUMBER'
        precision, scale = metadata[:data_precision], metadata[:data_scale]
        "NUMBER#{precision ? "(#{precision.to_i}#{scale ? ",#{scale.to_i}": ""})" : ""}"
      when 'VARCHAR2', 'CHAR', 'NVARCHAR2', 'NCHAR'
        if length = metadata[:data_length]
          length = length.to_i
        end
        if length && (char_used = metadata[:char_used])
          length = "#{length} #{char_used == 'C' ? 'CHAR' : 'BYTE'}"
        end
        "#{metadata[:data_type]}#{length ? "(#{length})": ""}"
      else
        metadata[:data_type]
      end
    end

    def get_return_value
      # if function with output parameters
      if return_metadata && out_list.size > 0
        result = [@cursor[':return'], {}]
        out_list.each do |k|
          result[1][k] = @cursor[":#{k}"]
        end
      # if function without output parameters
      elsif return_metadata
        result = @cursor[':return']
      # if procedure with output parameters
      elsif out_list.size > 0
        result = {}
        out_list.each do |k|
          result[k] = @cursor[":#{k}"]
        end
      # if procedure without output parameters
      else
        result = nil
      end
      result
    end

    def overload_argument_list
      @overload_argument_list ||= @procedure.argument_list
    end

    def overload_arguments
      @overload_arguments ||= @procedure.arguments
    end

    def argument_list
      @argument_list ||= overload_argument_list[@overload]
    end

    def arguments
      @arguments ||= overload_arguments[@overload]
    end

    def return_metadata
      @return_metadata ||= @procedure.return[@overload]
    end

    def out_list
      @out_list ||= @procedure.out_list[@overload]
    end

    def schema_name
      @schema_name ||= @procedure.schema_name
    end

    def package_name
      @package_name ||= @procedure.package
    end

    def procedure_name
      @procedure_name ||= @procedure.procedure
    end

  end

end