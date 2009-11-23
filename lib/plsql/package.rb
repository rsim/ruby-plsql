module PLSQL

  module PackageClassMethods #:nodoc:
    def find(schema, package)
      if schema.select_first("
            SELECT object_name FROM all_objects
            WHERE owner = :owner
              AND object_name = :package
              AND object_type = 'PACKAGE'",
            schema.schema_name, package.to_s.upcase)
        new(schema, package)
      # search for synonym
      elsif (row = schema.select_first("
            SELECT o.owner, o.object_name
            FROM all_synonyms s, all_objects o
            WHERE s.owner IN (:owner, 'PUBLIC')
              AND s.synonym_name = :synonym_name
              AND o.owner = s.table_owner
              AND o.object_name = s.table_name
              AND o.object_type = 'PACKAGE'
            ORDER BY DECODE(s.owner, 'PUBLIC', 1, 0)",
            schema.schema_name, package.to_s.upcase))
        new(schema, row[1], row[0])
      else
        nil
      end
    end
  end

  class Package #:nodoc:
    extend PackageClassMethods

    def initialize(schema, package, override_schema_name = nil)
      @schema = schema
      @override_schema_name = override_schema_name
      @package = package.to_s.upcase
      @procedures = {}
    end

    private
    
    def method_missing(method, *args, &block)
      if procedure = @procedures[method]
        procedure.exec(*args, &block)
      elsif procedure = Procedure.find(@schema, method, @package, @override_schema_name)
        @procedures[method] = procedure
        procedure.exec(*args, &block)
      else
        raise ArgumentError, "No PL/SQL procedure found"
      end
    end
    
  end

end
