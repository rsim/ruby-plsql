module PLSQL

  module PackageClassMethods #:nodoc:
    def find(schema, package)
      if schema.select_first(
            "SELECT object_name FROM all_objects
            WHERE owner = :owner
              AND object_name = :package
              AND object_type = 'PACKAGE'",
            schema.schema_name, package.to_s.upcase)
        new(schema, package)
      # search for synonym
      elsif (row = schema.select_first(
            "SELECT o.owner, o.object_name
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
      @package_objects = {}
    end

    def procedure_defined?(name)
      PLSQL::Procedure.find(@schema, name, @package) ? true : false
    end

    private
    
    def method_missing(method, *args, &block)
      if assignment = (method.to_s[-1,1] == '=')
        method = method.to_s.chop.to_sym
      end
      object = (@package_objects[method] ||=
        Procedure.find(@schema, method, @package, @override_schema_name) ||
        Variable.find(@schema, method, @package, @override_schema_name))
      case object
      when Procedure
        raise ArgumentError, "Cannot assign value to package procedure '#{method.to_s.upcase}'" if assignment
        object.exec(*args, &block)
      when Variable
        if assignment
          raise ArgumentError, "Just one value can be assigned to package variable '#{method.to_s.upcase}'" unless args.size == 1 && block == nil
          object.value = args[0]
        else
          raise ArgumentError, "Cannot pass arguments when getting package variable '#{method.to_s.upcase}' value" unless args.size == 0 && block == nil
          object.value
        end
      else
        raise ArgumentError, "No PL/SQL procedure or variable '#{method.to_s.upcase}' found"
      end
    end

  end

end
