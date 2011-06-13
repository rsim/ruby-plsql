module PLSQL
  
  module SequenceHelperProvider
    
    def sequence_helper(dialect)
      case dialect
      when :oracle
        ORASequenceHelper
      when :postgres
        PGSequenceHelper
      end
    end
    
  end
  
  module SequenceHelper
    
    module ClassMethods
    
      def find(schema, sequence)
        case schema.connection.dialect
        when :oracle
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
        when :postgres
          if schema.select_first(
              "SELECT sequence_name FROM information_schema.sequences
              WHERE UPPER(sequence_schema) = '#{schema.schema_name}'
              AND UPPER(sequence_name) = '#{sequence.to_s.upcase}'")
            new(schema, sequence)
          end
        end
      end
      
    end
    
    def self.included(host_class)
      host_class.extend(ClassMethods)
    end
    
  end
  
  module ORASequenceHelper #:nodoc:
    
    # Get NEXTVAL of sequence
    def nextval
      @schema.select_one "SELECT \"#{@schema_name}\".\"#{@sequence_name}\".NEXTVAL FROM dual"
    end

    # Get CURRVAL of sequence (can be called just after nextval)
    def currval
      @schema.select_one "SELECT \"#{@schema_name}\".\"#{@sequence_name}\".CURRVAL FROM dual"
    end
    
  end
  
  module PGSequenceHelper #:nodoc:
    
    def nextval
      @schema.select_one "SELECT nextval('#{@schema_name}.#{@sequence_name}')";
    end
    
    def currval
      @schema.select_one "SELECT currval('#{@schema_name}.#{@sequence_name}')"
    end
  
  end
  
end