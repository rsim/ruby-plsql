module PLSQL
  # Work with table functions
  # See http://www.oracle-base.com/articles/misc/pipelined-table-functions.php for examples
  # or http://asktom.oracle.com/pls/asktom/f?p=100:11:0::::P11_QUESTION_ID:19481671347143
  module PipelinedFunctionClassMethods
    def find(schema, function_name, package_name = nil, override_schema_name = nil)
      if package_name
        find_function_in_package(schema, package_name, function_name, override_schema_name)
      else
        find_function_in_schema(schema, function_name) || find_function_by_synonym(schema, function_name)
      end
    end

    def find_function_in_schema(schema, function_name)
      row = schema.select_first(<<-SQL, schema.schema_name, function_name.to_s.upcase)
        SELECT object_id
        FROM   all_procedures
        WHERE  owner = :owner
        AND    object_name = :object_name
        AND    object_type = 'FUNCTION'
        AND    pipelined = 'YES'
      SQL
      new(schema, function_name, nil, nil, row[0]) if row
    end

    def find_function_by_synonym(schema, function_name)
      row = schema.select_first(<<-SQL, schema.schema_name, function_name.to_s.upcase)
        SELECT p.owner, p.object_name, p.object_id
        FROM   all_synonyms s,
               all_procedures p
        WHERE  s.owner IN (:owner, 'PUBLIC')
        AND    s.synonym_name = :synonym_name
        AND    p.owner        = s.table_owner
        AND    p.object_name  = s.table_name
        AND    p.object_type  = 'FUNCTION'
        AND    p.pipelined    = 'YES'
        ORDER BY DECODE(s.owner, 'PUBLIC', 1, 0)
      SQL
      new(schema, row[1], nil, row[0], row[2]) if row
    end

    def find_function_in_package(schema, package_name, function_name, override_schema_name = nil)
      schema_name = override_schema_name || schema.schema_name
      row = schema.select_first(<<-SQL, schema_name, package_name, function_name.to_s.upcase)
        SELECT o.object_id
        FROM   all_procedures p,
               all_objects o
        WHERE  p.owner       = :owner
        AND    p.object_name = :object_name
        AND    p.procedure_name = :procedure_name
        AND    p.pipelined   = 'YES'
        AND    o.owner       = p.owner
        AND    o.object_name = p.object_name
        AND    o.object_type = 'PACKAGE'
      SQL
      new(schema, function_name, package_name, override_schema_name, row[0]) if row
    end
  end

  # TODO: create subprogram class and replace superclass for PipelinedFunction and Procedure
  class PipelinedFunction < Procedure
    extend PipelinedFunctionClassMethods

    def initialize(*)
      super
      @return = @return[0]
    end

    def exec(*args, &block)
      # use custom call syntax
      call = PipelinedFunctionCall.new(self, args)
      call.exec(&block)
    end

    private :ensure_tmp_tables_created
  end
end