require "plsql/procedure_helpers"

module PLSQL
  
  module ProcedureCommon
    
    def self.type_to_sql(metadata) #:nodoc:
      case metadata[:data_type]
      when 'NUMBER'
        precision, scale = metadata[:data_precision], metadata[:data_scale]
        "NUMBER#{precision ? "(#{precision}#{scale ? ",#{scale}": ""})" : ""}"
      when 'VARCHAR2', 'CHAR'
        length = case metadata[:char_used]
        when 'C' then "#{metadata[:char_length]} CHAR"
        when 'B' then "#{metadata[:data_length]} BYTE"
        else
          metadata[:data_length]
        end
        "#{metadata[:data_type]}#{length && "(#{length})"}"
      when 'NVARCHAR2', 'NCHAR'
        length = metadata[:char_length]
        "#{metadata[:data_type]}#{length && "(#{length})"}"
      when 'PL/SQL TABLE', 'TABLE', 'VARRAY', 'OBJECT'
        metadata[:sql_type_name]
      else
        metadata[:data_type]
      end
    end
    
  end

  class Procedure #:nodoc:
    
    include ProcedureHelperProvider
    include ProcedureHelper

    attr_reader :arguments, :argument_list, :out_list, :return
    attr_reader :schema, :schema_name, :package, :procedure

    def initialize(schema, procedure, package, override_schema_name, object_id)
      @schema = schema
      @schema_name = override_schema_name || schema.schema_name
      @procedure = procedure.to_s.upcase
      @package = package
      @object_id = object_id
      extend procedure_helper(schema.connection.dialect)
      get_argument_metadata
    end

    def exec(*args, &block)
      call = ProcedureCall.new(self, args)
      call.exec(&block)
    end

  end

end