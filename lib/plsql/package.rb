module PLSQL

  module PackageClassMethods #:nodoc:
    def find(schema, package)
      package_name = package.to_s.upcase
      find_in_schema(schema, package_name) || find_by_synonym(schema, package_name)
    end

    def find_in_schema(schema, package_name)
      row = schema.select_first(<<-SQL, schema.schema_name, package_name)
        SELECT object_name
        FROM   all_objects
        WHERE  owner = :owner
        AND    object_name = :package
        AND    object_type = 'PACKAGE'
      SQL
      new(schema, package_name) if row
    end

    def find_by_synonym(schema, package_name)
      row = schema.select_first(<<-SQL, schema.schema_name, package_name)
        SELECT o.object_name, o.owner
        FROM   all_synonyms s,
               all_objects o
        WHERE  s.owner IN (:owner, 'PUBLIC')
        AND    s.synonym_name = :synonym_name
        AND    o.owner = s.table_owner
        AND    o.object_name = s.table_name
        AND    o.object_type = 'PACKAGE'
        ORDER BY DECODE(s.owner, 'PUBLIC', 1, 0)
      SQL
      new(schema, row[0], row[1]) if row
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

    def [](object_name)
      object_name = object_name.to_s
      @package_objects[object_name] ||= [Procedure, Variable, PipelinedFunction].inject(nil) do |res, object_type|
        res || object_type.find(@schema, object_name, @package, @override_schema_name)
      end
    end

    private

    def method_missing(method, *args, &block)
      method = method.to_s
      method.chop! if (assignment = method[/=$/])

      case (object = self[method])
        when Procedure, PipelinedFunction
          if assignment
            raise ArgumentError, "Cannot assign value to package procedure '#{method.upcase}'"
          end
          object.exec(*args, &block)
        when Variable
          if assignment
            unless args.size == 1 && block.nil?
              raise ArgumentError, "Just one value can be assigned " \
                                   "to package variable '#{method.upcase}'"
            end
            object.value = args[0]
          else
            unless args.size == 0 && block.nil?
              raise ArgumentError, "Cannot pass arguments when getting " \
                                   "package variable '#{method.upcase}' value"
            end
            object.value
          end
        else
          raise ArgumentError, "No PL/SQL procedure or variable '#{method.upcase}' found"
      end
    end

  end

end
