module PLSQL
  module SequenceClassMethods #:nodoc:
    def find(schema, sequence)
      if schema.select_first(
        "SELECT sequence_name FROM all_sequences
        WHERE sequence_owner = :owner
          AND sequence_name = :sequence_name",
            schema.schema_name, sequence.to_s.upcase)
        new(schema, sequence)
      # search for synonym
      elsif (row = schema.select_first(
        "SELECT t.sequence_owner, t.sequence_name
        FROM all_synonyms s, all_sequences t
        WHERE s.owner IN (:owner, 'PUBLIC')
          AND s.synonym_name = :synonym_name
          AND t.sequence_owner = s.table_owner
          AND t.sequence_name = s.table_name
        ORDER BY DECODE(s.owner, 'PUBLIC', 1, 0)",
            schema.schema_name, sequence.to_s.upcase))
        new(schema, row[1], row[0])
      else
        nil
      end
    end
  end

  class Sequence
    extend SequenceClassMethods

    def initialize(schema, sequence, override_schema_name = nil) #:nodoc:
      @schema = schema
      @schema_name = override_schema_name || schema.schema_name
      @sequence_name = sequence.to_s.upcase
    end

    # Get NEXTVAL of sequence
    def nextval
      @schema.select_one "SELECT \"#{@schema_name}\".\"#{@sequence_name}\".NEXTVAL FROM dual"
    end

    # Get CURRVAL of sequence (can be called just after nextval)
    def currval
      @schema.select_one "SELECT \"#{@schema_name}\".\"#{@sequence_name}\".CURRVAL FROM dual"
    end
  end
end
