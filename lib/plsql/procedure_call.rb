require "plsql/procedure_call_helpers"

module PLSQL
  
  class ProcedureCall #:nodoc:
    
    include ProcedureCallHelperProvider

    def initialize(procedure, args = [], options = {})
      @procedure = procedure
      @schema = @procedure.schema
      extend procedure_call_helper(@schema.connection.dialect)
      @dbms_output_stream = @schema.dbms_output_stream
      @skip_self = options[:skip_self]
      @self = options[:self]
      @overload = get_overload_from_arguments_list(args)
      @procedure.ensure_tmp_tables_created(@overload) if @procedure.respond_to?(:ensure_tmp_tables_created)
      construct_sql(args)
    end

    def exec
      # puts "DEBUG: sql = #{@sql.gsub("\n","<br/>\n")}"
      @cursor = @schema.connection.parse(@sql)

      @bind_values.each do |arg, value|
        @cursor.bind_param(":#{arg}", value, @bind_metadata[arg])
      end

      @return_vars.each do |var|
        @cursor.bind_param(":#{var}", nil, @return_vars_metadata[var])
      end

      @cursor.exec

      dbms_output_log

      if block_given?
        yield get_return_value
        nil
      else
        get_return_value
      end
    ensure
      @cursor.close if @cursor
    end

    private

    def record_fields_sorted_by_position(fields_metadata)
      fields_metadata.keys.sort_by{|k| fields_metadata[k][:position]}
    end

    def add_return
      add_return_variable(:return, return_metadata, true)
    end

    def add_out_variables
      out_list.each do |argument|
        add_return_variable(argument, arguments[argument])
      end
    end

    def get_return_value
      # if function with output parameters
      if return_metadata && out_list.size > 0
        result = [function_return_value, {}]
        out_list.each do |k|
          result[1][k] = out_variable_value(k)
        end
        result
      # if function without output parameters
      elsif return_metadata
        function_return_value
      # if procedure with output parameters
      elsif out_list.size > 0
        result = {}
        out_list.each do |k|
          result[k] = out_variable_value(k)
        end
        result
      # if procedure without output parameters
      else
        nil
      end
    end

    def function_return_value
      return_variable_value(:return, return_metadata)
    end

    def out_variable_value(argument)
      return_variable_value(argument, arguments[argument])
    end

    def overload_argument_list
      @overload_argument_list ||=
        @skip_self ? @procedure.argument_list_without_self : @procedure.argument_list
    end

    def overload_arguments
      @overload_arguments ||=
        @skip_self ? @procedure.arguments_without_self : @procedure.arguments
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
      @out_list ||=
        @skip_self ? @procedure.out_list_without_self[@overload] : @procedure.out_list[@overload]
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