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
        if row[0] =~ /^\s*#{variable_upcase}\s+(CONSTANT\s+)?([A-Z0-9_. %]+(\([0-9,]+\))?)\s*(NOT\s+NULL)?\s*((:=|DEFAULT).*)?;\s*(--.*)?$/i
          return new(schema, variable, package, $2.strip, override_schema_name)
        end
      end
      nil
    end
  end

  class Variable #:nodoc:
    extend VariableClassMethods

    def initialize(schema, variable, package, variable_type, override_schema_name = nil)
      @schema = schema
      @schema_name = override_schema_name || schema.schema_name
      @variable_name = variable.to_s.upcase
      @package_name = package
      @variable_type = variable_type.upcase
      @metadata = metadata(@variable_type)
    end

    def value
      cursor = @schema.connection.parse(get_value_sql)
      cursor.bind_param(":return", nil, @metadata)
      cursor.exec
      cursor[':return']
    ensure
      cursor.close if defined?(cursor) && cursor
    end

    def value=(new_value)
      cursor = @schema.connection.parse(set_value_sql)
      cursor.bind_param(":value", new_value, @metadata)
      cursor.exec
      new_value
    ensure
      cursor.close if defined?(cursor) && cursor
    end

    private

    def get_value_sql
      sql = "BEGIN\n"
      sql << ":return := "
      sql << "#{@schema_name}." if @schema_name
      sql << "#{@package_name}." if @package_name
      sql << "#{@variable_name};\n"
      sql << "END;"
    end

    def set_value_sql
      sql = "BEGIN\n"
      sql << "#{@schema_name}." if @schema_name
      sql << "#{@package_name}." if @package_name
      sql << "#{@variable_name} := :value ;\n"
      sql << "END;"
    end

    def metadata(type_string)
      case type_string
      when /^(VARCHAR2|CHAR|NVARCHAR2|NCHAR)(\((\d+)\))?$/
        {:data_type => $1, :data_length => $3.to_i, :in_out => 'IN/OUT'}
      when /^(CLOB|NCLOB|BLOB)$/,
          /^(NUMBER)(\(.*\))?$/, /^(PLS_INTEGER|BINARY_INTEGER)$/,
          /^(DATE|TIMESTAMP|TIMESTAMP WITH TIME ZONE|TIMESTAMP WITH LOCAL TIME ZONE)$/
        {:data_type => $1, :in_out => 'IN/OUT'}
      when /^(\w+\.)?(\w+)\.(\w+)%TYPE$/
        schema = $1 ? plsql.send($1.chop) : plsql
        table = schema.send($2.downcase.to_sym)
        column = table.columns[$3.downcase.to_sym]
        {:data_type => column[:data_type], :data_length => column[:data_length], :sql_type_name => column[:sql_type_name], :in_out => 'IN/OUT'}
      when /^(\w+\.)?(\w+)$/
        schema = $1 ? plsql.send($1.chop) : plsql
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
      else
        raise ArgumentError, "Package variable data type #{type_string} is not supported"
      end
    end

  end

end