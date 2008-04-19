module PLSQL

  module PackageClassMethods
    def find(schema, package)
      if schema.select_first("
            SELECT object_name FROM all_objects
            WHERE owner = :owner
              AND object_name = :package
              AND object_type = 'PACKAGE'",
            schema.schema_name, package.to_s.upcase)
        new(schema, package)
      else
        nil
      end
    end
  end

  class Package
    extend PackageClassMethods

    def initialize(schema, package)
      @schema = schema
      @package = package.to_s.upcase
      @procedures = {}
    end

    private
    
    def method_missing(method, *args)
      if procedure = @procedures[method]
        procedure.exec(*args)
      elsif procedure = Procedure.find(@schema, method, @package)
        @procedures[method] = procedure
        procedure.exec(*args)
      else
        raise ArgumentError, "No PL/SQL procedure found"
      end
    end
    
  end

end
