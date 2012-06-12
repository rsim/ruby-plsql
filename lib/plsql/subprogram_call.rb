module PLSQL
  # Call of any function or procedure
  # TODO: need to be refactored
  class SubprogramCall
    def initialize(subprogram, args = [], options = {})
      @subprogram = subprogram
      @schema = @subprogram.schema
      @skip_self = options[:skip_self]
      @self = options[:self]
      @overload = get_overload_from_arguments_list(args)
      @subprogram.ensure_tmp_tables_created(@overload) if @subprogram.respond_to?(:ensure_tmp_tables_created)
      construct_sql(args)
    end

    private

    def prepare_sql_construction
      @declare_sql = ""
      @assignment_sql = ""
      @binds = {:values => {}, :metadata => {}}
      @return = {:sql => '', :variables => [], :metadata => {}}
    end

    def get_overload_from_arguments_list(args)
      # if not overloaded then overload index 0 is used
      return 0 unless @subprogram.overloaded?
      # If named arguments are used then
      # there should be just one Hash argument with symbol keys
      if args.size == 1 && args[0].is_a?(Hash) && args[0].keys.all?{|k| k.is_a?(Symbol)}
        args_keys = args[0].keys
        # implicit SELF argument for object instance procedures
        args_keys << :self if @self && !args_keys.include?(:self)
        number_of_args = args_keys.size
        matching_overloads = [] # overloads with exact or smaller number of matching named arguments
        overload_argument_list.keys.each do |ov|
          # assume that missing arguments have default value
          missing_arguments_count = overload_argument_list[ov].size - number_of_args
          if missing_arguments_count >= 0 &&
              args_keys.all?{|k| overload_argument_list[ov].include?(k)}
            matching_overloads << [ov, missing_arguments_count]
          end
        end
        # pick first matching overload with smallest missing arguments count
        # (hoping that missing arguments will be defaulted - cannot find default value from all_arguments)
        overload = matching_overloads.sort_by{|ov, score| score}[0][0]
      # otherwise try matching by sequential arguments count and types
      else
        number_of_args = args.size
        matching_types = []
        # if implicit SELF argument for object instance procedures should be passed
        # then it should be added as first argument to find matches
        if @self
          number_of_args += 1
          matching_types << ['OBJECT']
        end
        args.each do |arg|
          matching_types << matching_oracle_types_for_ruby_value(arg)
        end
        exact_overloads = [] # overloads with exact number of matching arguments
        smaller_overloads = [] # overloads with smaller number of matching arguments
        # overload = overload_argument_list.keys.detect do |ov|
        #   overload_argument_list[ov].size == number_of_args
        # end
        overload_argument_list.keys.each do |ov|
          score = 0 # lower score is better match
          ov_arg_list_size = overload_argument_list[ov].size
          if (number_of_args <= ov_arg_list_size &&
              (0..(number_of_args-1)).all? do |i|
                ov_arg = overload_argument_list[ov][i]
                matching_types[i] == :all || # either value matches any type
                (ind = matching_types[i].index(overload_arguments[ov][ov_arg][:data_type])) &&
                (score += ind) # or add index of matched type
              end)
            if number_of_args == ov_arg_list_size
              exact_overloads << [ov, score]
            else
              smaller_overloads << [ov, score]
            end
          end
        end
        # pick either first exact matching overload of first matching with smaller argument count
        # (hoping that missing arguments will be defaulted - cannot find default value from all_arguments)
        overload = if !exact_overloads.empty?
          exact_overloads.sort_by{|ov, score| score}[0][0]
        elsif !smaller_overloads.empty?
          smaller_overloads.sort_by{|ov, score| score}[0][0]
        end
      end
      raise ArgumentError, "Wrong number or types of arguments passed to overloaded PL/SQL procedure" unless overload
      overload
    end

    MATCHING_TYPES = {
      :integer => ['NUMBER', 'PLS_INTEGER', 'BINARY_INTEGER'],
      :decimal => ['NUMBER', 'BINARY_FLOAT', 'BINARY_DOUBLE'],
      :string => ['VARCHAR2', 'NVARCHAR2', 'CHAR', 'NCHAR', 'CLOB', 'BLOB'],
      :date => ['DATE'],
      :time => ['DATE', 'TIMESTAMP', 'TIMESTAMP WITH TIME ZONE', 'TIMESTAMP WITH LOCAL TIME ZONE'],
      :boolean => ['PL/SQL BOOLEAN'],
      :hash => ['PL/SQL RECORD', 'OBJECT', 'PL/SQL TABLE'],
      :array => ['TABLE', 'VARRAY'],
      :cursor => ['REF CURSOR']
    }

    def matching_oracle_types_for_ruby_value(value)
      case value
      when NilClass
        :all
      when Fixnum, Bignum
        MATCHING_TYPES[:integer]
      when BigDecimal, Float
        MATCHING_TYPES[:decimal]
      when String
        MATCHING_TYPES[:string]
      when Date
        MATCHING_TYPES[:date]
      when Time
        MATCHING_TYPES[:time]
      when TrueClass, FalseClass
        MATCHING_TYPES[:boolean]
      when Hash
        MATCHING_TYPES[:hash]
      when Array
        MATCHING_TYPES[:array]
      when CursorCommon
        MATCHING_TYPES[:cursor]
      end
    end

    def add_arguments(arguments)
      # Named arguments
      # there should be just one Hash argument with symbol keys
      if use_named_arguments?(arguments)
        add_named_arguments(arguments.first)
      # Sequential arguments
      else
        add_sequential_arguments(arguments)
      end
    end

    def use_named_arguments?(arguments)
      return false unless arguments.size == 1
      arguments = arguments.first
      return false unless arguments.is_a?(Hash) && arguments.keys.all?{|k| k.is_a?(Symbol)}
      # do not use named arguments if procedure has just one PL/SQL record PL/SQL table or object type argument -
      # in that case passed Hash should be used as value for this PL/SQL record argument
      # (which will be processed in sequential arguments branch)

      # ensure that first argument is not record/table/object
      return true if argument_list.size != 1
      only_argument = argument_list.first
      data_type = arguments_metadata[only_argument][:data_type]
      !['PL/SQL RECORD','PL/SQL TABLE','OBJECT'].include?(data_type) || arguments.keys == [only_argument]
    end

    def add_named_arguments(arguments)
      # Add missing output arguments with nil value
      arguments_metadata.each do |arg, metadata|
        arguments[arg] = nil if !arguments.has_key?(arg) && metadata[:in_out] == 'OUT'
      end

      # Add SELF argument if provided
      arguments[:self] = @self if @self
      # Add passed parameters to procedure call with parameter names
      arguments.map {|arg, value| "#{arg} => " << add_argument(arg, value)}.join(', ')
    end

    def add_sequential_arguments(arguments)
      # add SELF as first argument if provided
      arguments.unshift(@self) if @self
      argument_count = argument_list.size
      raise ArgumentError, "Too many arguments passed to PL/SQL procedure" if arguments.size > argument_count
      # Add missing output arguments with nil value
      if arguments.size < argument_count &&
                    (arguments.size...argument_count).all?{|i| arguments_metadata[argument_list[i]][:in_out] == 'OUT'}
        arguments += [nil] * (argument_count - arguments.size)
      end
      # Add passed parameters to procedure call in sequence
      arguments.map.each_with_index {|arg, idx| add_argument(argument_list[idx], arg)}.join(', ')
    end

    def add_argument(argument, value, argument_metadata=nil)
      argument_metadata ||= arguments_metadata[argument]
      raise ArgumentError, "Wrong argument #{argument.inspect} passed to PL/SQL procedure" unless argument_metadata
      case argument_metadata[:data_type]
      when 'PL/SQL RECORD'
        add_record_declaration(argument, argument_metadata)
        record_assignment_sql, record_bind_values, record_bind_metadata =
          record_assignment_sql_values_metadata(argument, argument_metadata, value)
        @assignment_sql << record_assignment_sql
        bind_values(record_bind_values, record_bind_metadata)
        "l_#{argument}"
      when 'PL/SQL BOOLEAN'
        add_variable_declaration(argument, 'boolean')
        @assignment_sql << "l_#{argument} := (:#{argument} = 1);\n"
        value = value.nil? ? nil : (value ? 1 : 0)
        metadata = argument_metadata.merge(:data_type => "NUMBER", :data_precision => 1)
        bind_value(argument, value, metadata)
        "l_#{argument}"
      else
        # TABLE or PL/SQL TABLE type defined inside package
        if argument_metadata[:tmp_table_name]
          add_table_declaration_and_assignment(argument, argument_metadata)
          insert_values_into_tmp_table(argument, argument_metadata, value)
          "l_#{argument}"
        else
          bind_value(argument, value, argument_metadata)
          ":#{argument}"
        end
      end
    end

    ## Next methods adds declaration in DECLARE block

    def add_variable_declaration(name, type, options = {})
      variable_name = options[:output_variable] ? 'o_' : 'l_'
      variable_name << name.to_s << ' ' << type.to_s.upcase
      variable_name << ' := '<< options[:value].to_s if options[:value]
      variable_name << ";\n"
      @declare_sql << variable_name
    end

    def add_type_declaration(name, fields_metadata)
      fields = record_fields_sorted_by_position(fields_metadata).map do |field|
        metadata = fields_metadata[field]
        "#{field} #{type_to_sql(metadata)}"
      end.join(",\n")

      @declare_sql << "TYPE t_#{name} IS RECORD (\n#{fields});\n"
    end

    def add_record_declaration(name, argument_metadata)
      if argument_metadata[:type_subname]
        add_variable_declaration(name, argument_metadata[:sql_type_name])
      else
        add_type_declaration(name, argument_metadata[:fields])
        add_variable_declaration(name, "t_#{name}")
      end
    end

    def add_cursor_declaration(name, fields, table, order = 'i__')
      # make an array
      fields = [*fields]
      @declare_sql << "CURSOR c_#{name} IS SELECT #{fields.join(', ')} FROM #{table} ORDER BY #{order};\n"
    end

    def add_table_declaration_and_assignment(argument, argument_metadata)
      is_index_by_table = argument_metadata[:data_type] == 'PL/SQL TABLE'
      type = argument_metadata[:sql_type_name]
      add_variable_declaration(argument, type, :value => !is_index_by_table && (type + '()'))

      @assignment_sql << "FOR r_#{argument} IN c_#{argument} LOOP\n"
      @assignment_sql << "l_#{argument}.EXTEND;\n" unless is_index_by_table

      table_name = argument_metadata[:tmp_table_name]

      if argument_metadata[:element][:data_type] == 'PL/SQL RECORD'
        fields = record_fields_sorted_by_position(argument_metadata[:element][:fields])
        add_cursor_declaration(argument, is_index_by_table ? "*" : fields, table_name)
        if is_index_by_table
          fields.each do |field|
            @assignment_sql << "l_#{argument}(r_#{argument}.i__).#{field} := r_#{argument}.#{field};\n"
          end
        else
          @assignment_sql << "l_#{argument}(l_#{argument}.COUNT) := r_#{argument};\n"
        end
      else
        add_cursor_declaration(argument, "*", table_name)
        @assignment_sql << "l_#{argument}(r_#{argument}.i__) := r_#{argument}.element;\n"
      end

      @assignment_sql << "END LOOP;\n"
      @assignment_sql << "DELETE FROM #{table_name};\n"
    end

    ##

    def record_fields_sorted_by_position(fields_metadata)
      fields_metadata.keys.sort_by{|k| fields_metadata[k][:position]}
    end

    def type_to_sql(metadata)
      ProcedureCommon.type_to_sql(metadata)
    end

    def overload_argument_list
      @overload_argument_list ||=
        @skip_self ? @subprogram.argument_list_without_self : @subprogram.argument_list
    end

    def overload_arguments
      @overload_arguments ||=
        @skip_self ? @subprogram.arguments_without_self : @subprogram.arguments
    end

    def argument_list
      @argument_list ||= overload_argument_list[@overload]
    end

    def arguments_metadata
      @arguments ||= overload_arguments[@overload]
    end

    def return_metadata
      @return_metadata ||= @subprogram.return[@overload]
    end

    def out_list
      @out_list ||=
        @skip_self ? @subprogram.out_list_without_self[@overload] : @subprogram.out_list[@overload]
    end

    def schema_name
      @subprogram.schema_name
    end

    def package_name
      @subprogram.package
    end

    def subprogram_name
      @subprogram.procedure
    end

    def full_subprogram_name
      [schema_name, package_name, subprogram_name].compact.join('.')
    end

    def function_return_value
      return_variable_value(:return, return_metadata)
    end

    def out_variable_value(argument)
      return_variable_value(argument, arguments_metadata[argument])
    end

    # declare once temp variable l_i__ that is used as iterator
    def declare_iterator
      unless @declared_i__
        add_variable_declaration(iterator.sub(/^l_/, ''), 'pls_integer')
        @declared_i__ = true
      end
    end

    def iterator
      'l_i__'
    end

    def return_variable_value(argument, argument_metadata)
      case argument_metadata[:data_type]
      when 'PL/SQL RECORD'
        return_value = {}
        argument_metadata[:fields].each do |field, metadata|
          return_value[field] = @cursor[":#{argument}_o#{metadata[:position]}"]
        end
        return_value
      when 'PL/SQL BOOLEAN'
        numeric_value = @cursor[":o_#{argument}"]
        numeric_value.nil? ? nil : numeric_value == 1
      else
        if argument_metadata[:tmp_table_name]
          is_index_by_table = argument_metadata[:data_type] == 'PL/SQL TABLE'
          case argument_metadata[:element][:data_type]
          when 'PL/SQL RECORD'
            if is_index_by_table
              Hash[*@cursor[":#{argument}"].fetch_hash_all.map{|row| [row.delete(:i__), row]}.flatten]
            else
              @cursor[":#{argument}"].fetch_hash_all
            end
          else
            if is_index_by_table
              Hash[*@cursor[":#{argument}"].fetch_all.map{|row| [row[1], row[0]]}.flatten]
            else
              @cursor[":#{argument}"].fetch_all.map{|row| row[0]}
            end
          end
        else
          @cursor[":#{argument}"]
        end
      end
    end

    def record_assignment_sql_values_metadata(argument, argument_metadata, record_value)
      sql = ""
      bind_values = {}
      bind_metadata = {}
      (record_value||{}).each do |key, value|
        field = key.is_a?(Symbol) ? key : key.to_s.downcase.to_sym
        metadata = argument_metadata[:fields][field]
        raise ArgumentError, "Wrong field name #{key.inspect} passed to PL/SQL record argument #{argument.inspect}" unless metadata
        bind_variable = :"#{argument}_f#{metadata[:position]}"
        sql << "l_#{argument}.#{field} := :#{bind_variable};\n"
        bind_values[bind_variable] = value
        bind_metadata[bind_variable] = metadata
      end
      [sql, bind_values, bind_metadata]
    end

    def add_return
      add_return_variable(:return, return_metadata, true)
    end

    def add_out_variables
      out_list.each do |argument|
        add_return_variable(argument, arguments_metadata[argument])
      end
    end

    def add_return_variable(argument, argument_metadata, is_return_value = false)
      case argument_metadata[:data_type]
      when 'PL/SQL RECORD'
        add_record_declaration(argument, argument_metadata) if is_return_value
        argument_metadata[:fields].each do |field, metadata|
          # should use different output bind variable as JDBC does not support
          # if output bind variable appears in several places
          bind_variable = :"#{argument}_o#{metadata[:position]}"
          bind_return_variable(bind_variable, metadata, ":#{bind_variable} := l_#{argument}.#{field};")
        end
        "l_#{argument} := " if is_return_value
      when 'PL/SQL BOOLEAN'
        add_variable_declaration(argument, 'boolean') if is_return_value
        add_variable_declaration(argument, 'number(1)', :output_variable => true)

        # should use different output bind variable as JDBC does not support
        # if output bind variable appears in several places
        bind_variable = :"o_#{argument}"
        bind_return_variable(bind_variable, argument_metadata.merge(:data_type => "NUMBER", :data_precision => 1), <<-SQL)
          IF l_#{argument} IS NULL THEN
            #{bind_variable} := NULL;
          ELSIF l_#{argument} THEN
            #{bind_variable} := 1;
          ELSE
            #{bind_variable} := 0;
          END IF;
          :#{bind_variable} := #{bind_variable};
        SQL
        "l_#{argument} := " if is_return_value
      else
        if argument_metadata[:tmp_table_name]
          add_return_table(argument, argument_metadata, is_return_value)
        elsif is_return_value
          bind_return_variable(argument, argument_metadata)
          ":#{argument} := "
        end
      end
    end

    def bind_return_variable(name, metadata, sql = nil)
      @return[:sql] << sql << "\n" if sql
      @return[:variables] << name
      @return[:metadata][name] = metadata
    end

    def bind_value(name, value, metadata)
      bind_values({name => value}, {name => metadata})
    end

    def bind_values(values, metadata)
      @binds[:values].merge!(values)
      @binds[:metadata].merge!(metadata)
    end

    def add_return_table(argument, argument_metadata, is_return_value = false)
      is_index_by_table = argument_metadata[:data_type] == 'PL/SQL TABLE'
      table_name = argument_metadata[:tmp_table_name]
      declare_iterator
      add_variable_declaration('return', return_metadata[:sql_type_name]) if is_return_value

      if argument_metadata[:element][:data_type] == 'PL/SQL RECORD'
        field_names = record_fields_sorted_by_position(argument_metadata[:element][:fields])
        values_string = field_names.map{|f| "l_#{argument}(#{iterator}).#{f}"}.join(', ')
        return_fields_string = is_index_by_table ? '*' : field_names.join(', ')
      else
        values_string = "l_#{argument}(#{iterator})"
        return_fields_string = '*'
      end

      if is_index_by_table
        return_sql = <<-SQL
          #{iterator} := l_#{argument}.FIRST;
          LOOP
            EXIT WHEN #{iterator} IS NULL;
            INSERT INTO #{table_name} VALUES (#{values_string}, #{iterator});
            #{iterator} := l_#{argument}.NEXT(#{iterator});
          END LOOP;
        SQL
      else
        return_sql = <<-SQL
          IF l_#{argument}.COUNT > 0 THEN
            FOR #{iterator} IN l_#{argument}.FIRST..l_#{argument}.LAST
            LOOP
              INSERT INTO #{table_name} VALUES (#{values_string}, #{iterator});
            END LOOP;
          END IF;
        SQL
      end

      return_sql << <<-SQL
        OPEN :#{argument} FOR SELECT #{return_fields_string} FROM #{table_name} ORDER BY i__;
        DELETE FROM #{table_name};
      SQL

      bind_return_variable(argument, argument_metadata.merge(:data_type => "REF CURSOR"), return_sql)
      "l_#{argument} := " if is_return_value
    end

    def insert_values_into_tmp_table(argument, argument_metadata, values)
      return unless values && !values.empty?
      is_index_by_table = argument_metadata[:data_type] == 'PL/SQL TABLE'
      if is_index_by_table
        raise ArgumentError, "Hash value should be passed for #{argument.inspect} argument" unless values.is_a?(Hash)
      else
        raise ArgumentError, "Array value should be passed for #{argument.inspect} argument" unless values.is_a?(Array)
      end
      tmp_table = @schema.root_schema.send(argument_metadata[:tmp_table_name])
      # insert values without autocommit
      old_autocommit = @schema.connection.autocommit?
      @schema.connection.autocommit = false if old_autocommit
      tmp_table.delete
      case argument_metadata[:element][:data_type]
      when 'PL/SQL RECORD'
        values_with_index = []
        if is_index_by_table
          values.each{|i,v| values_with_index << v.merge(:i__ => i)}
        else
          values.each_with_index{|v,i| values_with_index << v.merge(:i__ => i+1)}
        end
        tmp_table.insert values_with_index
      else
        values_with_index = []
        if is_index_by_table
          values.each{|i,v| values_with_index << [v, i]}
        else
          values.each_with_index{|v,i| values_with_index << [v, i+1]}
        end
        tmp_table.insert_values [:element, :i__], *values_with_index
      end
      @schema.connection.autocommit = true if old_autocommit
    end
  end
end