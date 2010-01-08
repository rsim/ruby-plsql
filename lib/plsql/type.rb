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
              data_type_owner, data_type_mod, typecode = r
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

    # list of object type attribute names
    def attribute_names
      @attribute_names ||= @attributes.keys.sort_by{|k| @attributes[k][:position]}
    end

    # create new PL/SQL object instance
    def new(*args, &block)
      procedure = find_procedure(:new)
      call = ProcedureCall.new(procedure, args)
      call.exec(&block)
    end

    def find_procedure(new_or_procedure)
      procedure = new_or_procedure == :new ? @type_name : new_or_procedure
      # find defined procedure for type
      if @schema.select_first(
            "SELECT procedure_name FROM all_procedures
            WHERE owner = :owner
              AND object_name = :object_name
              AND procedure_name = :procedure_name",
            @schema_name, @type_name, procedure.to_s.upcase)
        TypeProcedure.new(@schema, self, procedure)
      # call default constructor
      elsif new_or_procedure == :new
        TypeProcedure.new(@schema, self, :new)
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
          @procedure = @type_name
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

      def set_default_constructor_arguments
        attributes = @type.attributes
        @arguments ||= {}
        @argument_list ||= {}
        @out_list ||= {}
        @return ||= {}
        # either this will be the only overload or it will be additional
        overload = @arguments.keys.size
        @arguments[overload] = attributes
        @argument_list[overload] = attributes.keys.sort {|k1, k2| attributes[k1][:position] <=> attributes[k2][:position]}
        @out_list[overload] = []
        # returns object or collection
        @return[overload] = {
          :position => 0,
          :data_type => @type.typecode == 'COLLECTION' ? 'TABLE' : 'OBJECT',
          :in_out => 'OUT',
          :data_length => nil,
          :data_precision => nil,
          :data_scale => nil,
          :type_owner => @schema_name,
          :type_name => @type_name,
          :sql_type_name => "#{@schema_name}.#{@type_name}"
        }
        @overloaded = overload > 0
      end

    end


  end

end
