begin
  require "oci8"
rescue LoadError
  # OCI8 driver is unavailable.
  error_message = "ERROR: ruby-plsql could not load ruby-oci8 library. "+
                  "Please install ruby-oci8 gem."
  STDERR.puts error_message
  raise LoadError
end

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
      def initialize(sql, conn)
        @connection = conn
        @raw_cursor = @connection.raw_connection.parse(sql)
      end

      def bind_param(arg, value, metadata)
        type, length = @connection.plsql_to_ruby_data_type(metadata[:data_type], metadata[:data_length])
        ora_value = @connection.ruby_value_to_ora_value(value, type)
        @raw_cursor.bind_param(arg, ora_value, type, length)
      end
      
      def exec(*bindvars)
        @raw_cursor.exec(*bindvars)
      end

      def [](key)
        @connection.ora_value_to_ruby_value(@raw_cursor[key])
      end

      def close
        @raw_cursor.close
      end

    end

    def parse(sql)
      Cursor.new(sql, self)
    end

    def plsql_to_ruby_data_type(data_type, data_length)
      case data_type
      when "VARCHAR2"
        [String, data_length || 32767]
      when "CLOB"
        [OCI8::CLOB, nil]
      when "BLOB"
        [OCI8::BLOB, nil]
      when "NUMBER"
        [OraNumber, nil]
      when "DATE"
        [DateTime, nil]
      when "TIMESTAMP"
        [Time, nil]
      # CLOB
      # BLOB
      else
        [String, 32767]
      end
    end

    def ruby_value_to_ora_value(val, type)
      if type == OraNumber
        # pass parameters as OraNumber to avoid rounding errors
        case val
        when Bignum
          OraNumber.new(val.to_s)
        when BigDecimal
          OraNumber.new(val.to_s('F'))
        else
          val
        end
      elsif type == DateTime
        case val
        when Time
          ::DateTime.civil(val.year, val.month, val.day, val.hour, val.min, val.sec, Rational(val.utc_offset, 86400))
        when DateTime
          val
        when Date
          ::DateTime.civil(val.year, val.month, val.day, 0, 0, 0, 0)
        else
          val
        end
      elsif type == OCI8::CLOB
        # ruby-oci8 cannot create CLOB from ''
        val = nil if val == ''
        OCI8::CLOB.new(raw_oci_connection, val)
      elsif type == OCI8::BLOB
        # ruby-oci8 cannot create BLOB from ''
        val = nil if val == ''
        OCI8::BLOB.new(raw_oci_connection, val)
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
      when OCI8::LOB
        if val.available?
          val.rewind
          val.read
        else
          nil
        end
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
      # return BigDecimal instead of Float to avoid rounding errors
      # num.to_i == num.to_f ? num.to_i : num.to_f
      num == (num_to_i = num.to_i) ? num_to_i : BigDecimal.new(num.to_s)
    end
    
    def ora_date_to_ruby_date(val)
      case val
      when DateTime
        # similar implementation as in oracle_enhanced adapter
        begin
          Time.send(plsql.default_timezone, val.year, val.month, val.day, val.hour, val.min, val.sec)
        rescue
          offset = plsql.default_timezone.to_sym == :local ? plsql.local_timezone_offset : 0
          DateTime.civil(val.year, val.month, val.day, val.hour, val.min, val.sec, offset)
        end
      when OraDate
        # similar implementation as in oracle_enhanced adapter
        begin
          Time.send(plsql.default_timezone, val.year, val.month, val.day, val.hour, val.minute, val.second)
        rescue
          offset = plsql.default_timezone.to_sym == :local ? plsql.local_timezone_offset : 0
          DateTime.civil(val.year, val.month, val.day, val.hour, val.minute, val.second, offset)
        end
      else
        val
      end
    end

  end
  
end