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

    attr_reader :typecode, :attributes, :schema_name, :type_name #:nodoc:

    def initialize(schema, type, override_schema_name = nil) #:nodoc:
      @schema = schema
      @schema_name = override_schema_name || schema.schema_name
      @type_name = type.to_s.upcase
      @attributes = {}

      @typecode = @schema.connection.select_first(
        "SELECT typecode FROM all_types
        WHERE owner = :owner
        AND type_name = :type_name",
        @schema_name, @type_name)[0]

      @schema.connection.select_all(
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

    def attribute_names
      @attribute_names ||= @attributes.keys.sort_by{|k| @attributes[k][:position]}
    end

  end

end
