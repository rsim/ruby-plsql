module PLSQL
  class OCIConnection < Connection
    
    def logoff
      raw_connection.logoff
    end

    def commit
      raw_connection.commit
    end

    def rollback
      raw_connection.rollback
    end
    
    def autocommit?
      raw_connection.autocommit?
    end

    def autocommit=(value)
      raw_connection.autocommit = value
    end

    def select_first(sql, *bindvars)
      cursor = raw_connection.exec(sql, *bindvars)
      result = cursor.fetch
      if result
        result.map { |val| ora_value_to_ruby_value(val) }
      else
        nil
      end
    ensure
      cursor.close rescue nil
    end

    def select_all(sql, *bindvars, &block)
      cursor = raw_connection.exec(sql, *bindvars)
      results = []
      row_count = 0
      while row = cursor.fetch
        row_with_typecast = row.map {|val| ora_value_to_ruby_value(val) }
        if block_given?
          yield(row_with_typecast)
          row_count += 1
        else
          results << row_with_typecast
        end
      end
      block_given? ? row_count : results
    ensure
      cursor.close rescue nil
    end

    def exec(sql, *bindvars)
      raw_connection.exec(sql, *bindvars)
      true
    end

    class Cursor
      attr_accessor :raw_cursor
      
      def initialize(raw_cur)
        @raw_cursor = raw_cur
      end

      def bind_param(key, value, type=nil, length=nil, in_out='IN')
        raw_cursor.bind_param(key, value, type, length)
      end
      
      def exec(*bindvars)
        raw_cursor.exec(*bindvars)
      end

      def [](key)
        raw_cursor[key]
      end

      def close
        raw_cursor.close
      end
    end

    def parse(sql)
      Cursor.new(raw_connection.parse(sql))
    end
    

    def plsql_to_ruby_data_type(data_type, data_length)
      case data_type
      when "VARCHAR2"
        [String, data_length || 4000]
      when "CLOB"
        [OCI8::CLOB, nil]
      when "NUMBER"
        [OraNumber, nil]
      when "DATE"
        [DateTime, nil]
      when "TIMESTAMP"
        [Time, nil]
      # CLOB
      # BLOB
      else
        [String, 4000]
      end
    end

    def ruby_value_to_ora_value(val, type)
      if type == OraNumber
        val.nil? || val.is_a?(Fixnum) ? val : val.to_f
      elsif type == DateTime
        val ? val.to_datetime : nil
      elsif type == OCI8::CLOB
        # ruby-oci8 cannot create CLOB from ''
        val = nil if val == ''
        OCI8::CLOB.new(raw_oci_connection, val)
      else
        val
      end
    end

    def ora_value_to_ruby_value(val)
      case val
      when Float, OraNumber
        ora_number_to_ruby_number(val)
      when DateTime, OraDate
        ora_date_to_ruby_date(val)
      when OCI8::CLOB
        val.rewind
        val.read
      else
        val
      end
    end


    private
    
    def raw_oci_connection
      if raw_connection.is_a? OCI8
        raw_connection
      # ActiveRecord Oracle enhanced adapter puts OCI8EnhancedAutoRecover wrapper around OCI8
      # in this case we need to pass original OCI8 connection
      else
        raw_connection.instance_variable_get(:@connection)
      end
    end
    
    def ora_number_to_ruby_number(num)
      num.to_i == num.to_f ? num.to_i : num.to_f
    end
    
    def ora_date_to_ruby_date(val)
      case val
      when DateTime
        Time.parse(val.strftime("%c")) rescue val
      when OraDate
        val.to_time rescue val.to_datetime
      else
        val
      end
    end

  end
  
end