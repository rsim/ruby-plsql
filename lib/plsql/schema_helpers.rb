module PLSQL
  
  module SchemaHelperProvider
    
    def schema_helper()
      schema_helper = if connection
        case connection.dialect
        when :oracle
          ORASchemaHelper
        when :postgres
          PGSchemaHelper
        end
      end
      schema_helper || ORASchemaHelper
    end
    
  end
  
  module ORASchemaHelper
    
    # Current Oracle schema name
    def schema_name
      return nil unless connection
      @schema_name ||= select_first("SELECT SYS_CONTEXT('userenv','session_user') FROM dual")[0]
    end
  
    def find_database_object(name, override_schema_name = nil)
      object_schema_name = override_schema_name || schema_name
      object_name = name.to_s.upcase
      if row = select_first(
          "SELECT o.object_type, o.object_id, o.status,
          (CASE WHEN o.object_type = 'PACKAGE'
          THEN (SELECT ob.status FROM all_objects ob
          WHERE ob.owner = o.owner AND ob.object_name = o.object_name AND ob.object_type = 'PACKAGE BODY')
          ELSE NULL END) body_status
          FROM all_objects o
          WHERE owner = :owner AND object_name = :object_name
          AND object_type IN ('PROCEDURE','FUNCTION','PACKAGE','TABLE','VIEW','SEQUENCE','TYPE','SYNONYM')",
          object_schema_name, object_name)
        object_type, object_id, status, body_status = row
        raise ArgumentError, "Database object '#{object_schema_name}.#{object_name}' is not in valid status\n#{
        _errors(object_schema_name, object_name, object_type)}" if status == 'INVALID'
        raise ArgumentError, "Package '#{object_schema_name}.#{object_name}' body is not in valid status\n#{
        _errors(object_schema_name, object_name, 'PACKAGE BODY')}" if body_status == 'INVALID'
        case object_type
        when 'PROCEDURE', 'FUNCTION'
          Procedure.new(self, name, nil, override_schema_name, object_id)
        when 'PACKAGE'
          Package.new(self, name, override_schema_name)
        when 'TABLE'
          Table.new(self, name, override_schema_name)
        when 'VIEW'
          View.new(self, name, override_schema_name)
        when 'SEQUENCE'
          Sequence.new(self, name, override_schema_name)
        when 'TYPE'
          Type.new(self, name, override_schema_name)
        when 'SYNONYM'
          target_schema_name, target_object_name = @connection.describe_synonym(object_schema_name, object_name)
          find_database_object(target_object_name, target_schema_name)
        end
      end
    end

    def _errors(object_schema_name, object_name, object_type)
      result = ""
      previous_line = 0
      select_all(
        "SELECT e.line, e.position, e.text error_text, s.text source_text
        FROM all_errors e, all_source s
        WHERE e.owner = :owner AND e.name = :name AND e.type = :type
          AND s.owner = e.owner AND s.name = e.name AND s.type = e.type AND s.line = e.line
        ORDER BY e.sequence",
        object_schema_name, object_name, object_type
      ).each do |line, position, error_text, source_text|
        result << "Error on line #{'%4d' % line}: #{source_text}" if line > previous_line
        result << "     position #{'%4d' % position}: #{error_text}\n"
        previous_line = line
      end
      result unless result.empty?
    end
    
    def find_other_schema(name)
      return nil if @original_schema
      if select_first("SELECT username FROM all_users WHERE username = :username", name.to_s.upcase)
        Schema.new(connection, name, self)
      else
        nil
      end
    end
    
  end
  
  module PGSchemaHelper
    
    # Current Oracle schema name
    def schema_name
      return nil unless connection
      @schema_name ||= select_first("SELECT UPPER(current_schema);")[0]
    end
    
    def find_database_object(name, override_schema_name = nil)
      object_schema_name = override_schema_name || schema_name
      object_name = name.to_s.upcase
      if row = select_first(
          "SELECT objects.* FROM (SELECT 'TABLE' object_type, table_name object_name, table_schema object_schema
          FROM information_schema.tables
          WHERE table_type = 'BASE TABLE'
          UNION ALL
          SELECT 'VIEW' object_type, table_name object_name, table_schema object_schema
          FROM information_schema.views
          UNION ALL
          SELECT 'SEQUENCE' object_type, sequence_name object_name, sequence_schema object_schema
          FROM information_schema.sequences
          UNION ALL
          SELECT 'FUNCTION' object_type, routine_name object_name, routine_schema object_schema
          FROM information_schema.routines) objects
          WHERE UPPER(objects.object_schema) = '#{object_schema_name}' 
          AND UPPER(objects.object_name) = '#{object_name}'")
        object_type, object_id = row
        case object_type
        when 'FUNCTION'
          Procedure.new(self, name, nil, override_schema_name, object_id)
        when 'TABLE'
          Table.new(self, name, override_schema_name)
        when 'VIEW'
          View.new(self, name, override_schema_name)
        when 'SEQUENCE'
          Sequence.new(self, name, override_schema_name)
        when 'TYPE'
          Type.new(self, name, override_schema_name)
        end
      end
    end
    
    def find_other_schema(name)
      return nil if @original_schema
      if select_first("SELECT schema_name FROM information_schema.schemata WHERE schema_name = '#{name}'")
        Schema.new(connection, name, self)
      else
        nil
      end
    end
  end
  
end