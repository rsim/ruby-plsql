require "plsql/sequence_helpers"

module PLSQL

  class Sequence
    
    include SequenceHelperProvider
    include SequenceHelper

    def initialize(schema, sequence, override_schema_name = nil) #:nodoc:
      @schema = schema
      @schema_name = override_schema_name || schema.schema_name
      @sequence_name = sequence.to_s.upcase
      extend sequence_helper(schema)
    end
    
  end

end
