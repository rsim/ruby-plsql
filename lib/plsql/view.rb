module PLSQL
  module ViewClassMethods #:nodoc:
    def find(schema, view)
      if schema.select_first(
        "SELECT view_name FROM all_views
        WHERE owner = :owner
          AND view_name = :view_name",
            schema.schema_name, view.to_s.upcase)
        new(schema, view)
      # search for synonym
      elsif (row = schema.select_first(
        "SELECT v.owner, v.view_name
        FROM all_synonyms s, all_views v
        WHERE s.owner = :owner
          AND s.synonym_name = :synonym_name
          AND v.owner = s.table_owner
          AND v.view_name = s.table_name
        UNION ALL
        SELECT v.owner, v.view_name
        FROM all_synonyms s, all_views v
        WHERE s.owner = 'PUBLIC'
          AND s.synonym_name = :synonym_name
          AND v.owner = s.table_owner
          AND v.view_name = s.table_name",
            schema.schema_name, view.to_s.upcase, view.to_s.upcase))
        new(schema, row[1], row[0])
      else
        nil
      end
    end
  end

  class View < Table
    extend ViewClassMethods

    alias :view_name :table_name #:nodoc:
  end
end
