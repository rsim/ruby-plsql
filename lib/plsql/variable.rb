module PLSQL

  module VariableClassMethods #:nodoc:
    def find(schema, variable, package, override_schema_name = nil)
      variable_upcase = variable.to_s.upcase
      schema.select_all(
            "SELECT text FROM all_source
            WHERE owner = :owner
              AND name = :object_name
              AND type = 'PACKAGE'
              AND UPPER(text) LIKE :variable_name",
            override_schema_name || schema.schema_name, package, "%#{variable_upcase}%").each do |row|
        if row[0] =~ /^\s*#{variable_upcase}\s+(CONSTANT\s+)?([A-Z0-9_. %]+(\([\w\s,]+\))?)\s*(NOT\s+NULL)?\s*((:=|DEFAULT).*)?;\s*(--.*)?$/i
          return new(schema, variable, package, $2.strip, override_schema_name)
        end
      end
      nil
    end
  end

  class Variable #:nodoc:
    extend VariableClassMethods

    attr_reader :schema_name, :package_name, :variable_name #:nodoc:

    def initialize(schema, variable, package, variable_type, override_schema_name = nil)
      @schema = schema
      @schema_name = override_schema_name || schema.schema_name
      @variable_name = variable.to_s.upcase
      @package_name = package
      @variable_type = variable_type.upcase
      @metadata = metadata(@variable_type)
    end

    def value
      @variable_get_proc ||= VariableProcedure.new(@schema, self, :get, @metadata)
      ProcedureCall.new(@variable_get_proc).exec
    end

    def value=(new_value)
      @variable_set_proc ||= VariableProcedure.new(@schema, self, :set, @metadata)
      ProcedureCall.new(@variable_set_proc, [new_value]).exec
      new_value
    end

    private

    def metadata(type_string)
      case type_string
      when /^(VARCHAR|VARCHAR2|CHAR|NVARCHAR2|NCHAR)(\((\d+)[\s\w]*\))?$/
        {:data_type => $1, :data_length => $3.to_i, :in_out => 'IN/OUT'}
      when /^(CLOB|NCLOB|BLOB)$/,
          /^(NUMBER)(\(.*\))?$/, /^(NATURAL|NATURALN|POSITIVE|POSITIVEN|SIGNTYPE|SIMPLE_INTEGER|PLS_INTEGER|BINARY_INTEGER)$/,
          /^(DATE|TIMESTAMP|TIMESTAMP WITH TIME ZONE|TIMESTAMP WITH LOCAL TIME ZONE)$/,
          /^(XMLTYPE)$/
        {:data_type => $1, :in_out => 'IN/OUT'}
      when /^INTEGER$/
        {:data_type => 'NUMBER', :in_out => 'IN/OUT'}
      when /^BOOLEAN$/
        {:data_type => 'PL/SQL BOOLEAN', :in_out => 'IN/OUT'}
      when /^(\w+\.)?(\w+)\.(\w+)%TYPE$/
        schema = $1 ? plsql.send($1.chop) : plsql
        table = schema.send($2.downcase.to_sym)
        column = table.columns[$3.downcase.to_sym]
        {:data_type => column[:data_type], :data_length => column[:data_length], :sql_type_name => column[:sql_type_name], :in_out => 'IN/OUT'}
      when /^(\w+\.)?(\w+)$/
        schema = $1 ? @schema.root_schema.send($1.chop) : @schema
        begin
          type = schema.send($2.downcase.to_sym)
          raise ArgumentError unless type.is_a?(PLSQL::Type)
          typecode = case type.typecode
          when 'COLLECTION' then 'TABLE'
          else 'OBJECT'
          end
          {:data_type => typecode, :data_length => nil, :sql_type_name => "#{type.schema_name}.#{type.type_name}", :in_out => 'IN/OUT'}
        rescue ArgumentError
          raise ArgumentError, "Package variable data type #{type_string} is not object type defined in schema"
        end
      when /^(\w+\.)?(\w+)%ROWTYPE$/
        schema = $1 ? plsql.send($1.chop) : plsql
        table = schema.send($2.downcase.to_sym)
        record_metadata = {
          :data_type => 'PL/SQL RECORD',
          :in_out => 'IN/OUT',
          :fields => {}
        }
        table.columns.each do |name, col|
          record_metadata[:fields][name] =
            {:data_type => col[:data_type], :data_length => col[:data_length], :sql_type_name => col[:sql_type_name],
            :position => col[:position], :in_out => 'IN/OUT'}
        end
        record_metadata
      else
        raise ArgumentError, "Package variable data type #{type_string} is not supported"
      end
    end

    # wrapper class to simulate Procedure class for ProcedureClass#exec
    class VariableProcedure #:nodoc:
      attr_reader :arguments, :argument_list, :return, :out_list, :schema

      def initialize(schema, variable, operation, metadata)
        @schema = schema
        @variable = variable
        @operation = operation
        @metadata = metadata

        @out_list = [[]]

        case @operation
        when :get
          @argument_list = [[]]
          @arguments = [{}]
          @return = [@metadata]
        when :set
          @argument_list = [[:value]]
          @arguments = [{:value => @metadata}]
          @return = [nil]
        end

      end

      def overloaded?
        false
      end

      def procedure
        nil
      end

      def call_sql(params_string)
        sql = (schema_name = @variable.schema_name) ? "#{schema_name}." : ""
        sql << "#{@variable.package_name}.#{@variable.variable_name}"
        case @operation
        when :get
          # params string contains assignment to return variable
          "#{params_string} #{sql};\n"
        when :set
          "#{sql} := #{params_string};\n"
        end
      end

    end

  end

end
