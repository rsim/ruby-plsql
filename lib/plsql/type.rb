module PLSQL

  module TypeClassMethods #:nodoc:
    def find(schema, type)
      if schema.select_first(
            "SELECT type_name FROM all_types
            WHERE owner = :owner
              AND type_name = :table_name",
            schema.schema_name, type.to_s.upcase)
        new(schema, type)
      # search for synonym
      elsif (row = schema.select_first(
            "SELECT t.owner, t.type_name
            FROM all_synonyms s, all_types t
            WHERE s.owner = :owner
              AND s.synonym_name = :synonym_name
              AND t.owner = s.table_owner
              AND t.type_name = s.table_name
            UNION ALL
            SELECT t.owner, t.type_name
            FROM all_synonyms s, all_types t
            WHERE s.owner = 'PUBLIC'
              AND s.synonym_name = :synonym_name
              AND t.owner = s.table_owner
              AND t.type_name = s.table_name",
            schema.schema_name, type.to_s.upcase, type.to_s.upcase))
        new(schema, row[1], row[0])
      else
        nil
      end
    end
  end

  class Type
    extend TypeClassMethods

    attr_reader :typecode, :attributes, :schema_name, :type_name, :type_object_id #:nodoc:

    def initialize(schema, type, override_schema_name = nil) #:nodoc:
      @schema = schema
      @schema_name = override_schema_name || schema.schema_name
      @type_name = type.to_s.upcase
      @attributes = {}
      @type_procedures = {}

      @typecode, @type_object_id = @schema.select_first(
        "SELECT t.typecode, o.object_id FROM all_types t, all_objects o
        WHERE t.owner = :owner
        AND t.type_name = :type_name
        AND o.owner = t.owner
        AND o.object_name = t.type_name
        AND o.object_type = 'TYPE'",
        @schema_name, @type_name)

      @schema.select_all(
        "SELECT attr_name, attr_no,
              attr_type_name, length, precision, scale,
              attr_type_owner, attr_type_mod,
              (SELECT t.typecode FROM all_types t
              WHERE t.owner = attr_type_owner
              AND t.type_name = attr_type_name) typecode
        FROM all_type_attrs
        WHERE owner = :owner
        AND type_name = :type_name
        ORDER BY attr_no",
        @schema_name, @type_name
      ) do |r|
        attr_name, position,
              data_type, data_length, data_precision, data_scale,
              data_type_owner, _, typecode = r
        @attributes[attr_name.downcase.to_sym] = {
          :position => position && position.to_i,
          :data_type => data_type_owner && (typecode == 'COLLECTION' ? 'TABLE' : 'OBJECT' ) || data_type,
          :data_length => data_type_owner ? nil : data_length && data_length.to_i,
          :data_precision => data_precision && data_precision.to_i,
          :data_scale => data_scale && data_scale.to_i,
          :type_owner => data_type_owner,
          :type_name => data_type_owner && data_type,
          :sql_type_name => data_type_owner && "#{data_type_owner}.#{data_type}"
        }
      end
    end

    # is type collection?
    def collection?
      @is_collection ||= @typecode == 'COLLECTION'
    end

    # list of object type attribute names
    def attribute_names
      @attribute_names ||= @attributes.keys.sort_by{|k| @attributes[k][:position]}
    end

    # create new PL/SQL object instance
    def new(*args, &block)
      procedure = find_procedure(:new)
      # in case of collections pass array of elements as one argument for constructor
      if collection? && !(args.size == 1 && args[0].is_a?(Array))
        args = [args]
      end
      result = procedure.exec_with_options(args, {:skip_self => true}, &block)
      # TODO: collection constructor should return Array of ObhjectInstance objects
      if collection?
        result
      else
        # TODO: what to do if block is passed to constructor?
        ObjectInstance.create(self, result)
      end
    end

    def method_missing(method, *args, &block) #:nodoc:
      if procedure = find_procedure(method)
        procedure.exec_with_options(args, {}, &block)
      else
        raise ArgumentError, "No PL/SQL procedure '#{method.to_s.upcase}' found for type '#{@type_name}'"
      end
    end

    def find_procedure(new_or_procedure) #:nodoc:
      @type_procedures[new_or_procedure] ||= begin
        procedure_name = new_or_procedure == :new ? @type_name : new_or_procedure
        # find defined procedure for type
        if @schema.select_first(
              "SELECT procedure_name FROM all_procedures
              WHERE owner = :owner
                AND object_name = :object_name
                AND procedure_name = :procedure_name",
              @schema_name, @type_name, procedure_name.to_s.upcase)
          TypeProcedure.new(@schema, self, procedure_name)
        # call default constructor
        elsif new_or_procedure == :new
          TypeProcedure.new(@schema, self, :new)
        end
      end
    end

    # wrapper class to simulate Procedure class for ProcedureClass#exec
    class TypeProcedure #:nodoc:
      include ProcedureCommon

      def initialize(schema, type, procedure)
        @schema = schema
        @type = type
        @schema_name = @type.schema_name
        @type_name = @type.type_name
        @object_id = @type.type_object_id

        # if default constructor
        if @default_constructor = (procedure == :new)
          @procedure = @type.collection? ? nil : @type_name
          set_default_constructor_arguments
        # if defined type procedure
        else
          @procedure = procedure.to_s.upcase
          get_argument_metadata
          # add also definition for default constructor in case of custom constructor
          set_default_constructor_arguments if @procedure == @type_name
        end

        # constructors do not need type prefix in call
        @package = @procedure == @type_name ? nil : @type_name
      end

      # will be called for collection constructor
      def call_sql(params_string)
        "#{params_string};\n"
      end

      attr_reader :arguments, :argument_list, :out_list
      def arguments_without_self
        @arguments_without_self ||= begin
          hash = {}
          @arguments.each do |ov, args|
            hash[ov] = args.reject{|key, value| key == :self}
          end
          hash
        end
      end

      def argument_list_without_self
        @argument_list_without_self ||= begin
          hash = {}
          @argument_list.each do |ov, arg_list|
            hash[ov] = arg_list.select{|arg| arg != :self}
          end
          hash
        end
      end

      def out_list_without_self
        @out_list_without_self ||= begin
          hash = {}
          @out_list.each do |ov, out_list|
            hash[ov] = out_list.select{|arg| arg != :self}
          end
          hash
        end
      end

      def exec_with_options(args, options={}, &block)
        call = ProcedureCall.new(self, args, options)
        result = call.exec(&block)
        # if procedure was called then modified object is returned in SELF output parameter
        if result.is_a?(Hash) && result[:self]
          object = result.delete(:self)
          result.empty? ? object : [object, result]
        else
          result
        end
      end

      private

      def set_default_constructor_arguments
        @arguments ||= {}
        @argument_list ||= {}
        @out_list ||= {}
        @return ||= {}
        # either this will be the only overload or it will be additional
        overload = @arguments.keys.size
        # if type is collection then expect array of objects as argument
        if @type.collection?
          @arguments[overload] = {
            :value => {
              :position => 1,
              :data_type => 'TABLE',
              :in_out => 'IN',
              :type_owner => @schema_name,
              :type_name => @type_name,
              :sql_type_name => "#{@schema_name}.#{@type_name}"
            }
          }
        # otherwise if type is object type then expect object attributes as argument list
        else
          @arguments[overload] = @type.attributes
        end
        attributes = @arguments[overload]
        @argument_list[overload] = attributes.keys.sort {|k1, k2| attributes[k1][:position] <=> attributes[k2][:position]}
        # returns object or collection
        @return[overload] = {
          :position => 0,
          :data_type => @type.collection? ? 'TABLE' : 'OBJECT',
          :in_out => 'OUT',
          :type_owner => @schema_name,
          :type_name => @type_name,
          :sql_type_name => "#{@schema_name}.#{@type_name}"
        }
        @out_list[overload] = []
        @overloaded = overload > 0
      end

    end

  end

  class ObjectInstance < Hash #:nodoc:
    attr_accessor :plsql_type

    def self.create(type, attributes)
      object = self.new.merge!(attributes)
      object.plsql_type = type
      object
    end

    def method_missing(method, *args, &block)
      if procedure = @plsql_type.find_procedure(method)
        procedure.exec_with_options(args, :self => self, &block)
      else
        raise ArgumentError, "No PL/SQL procedure '#{method.to_s.upcase}' found for type '#{@plsql_type.type_name}' object"
      end
    end

  end

end
