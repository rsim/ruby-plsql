module PLSQL
  class JDBCConnection < Connection
    def logoff
      raw_connection.close
      true
    rescue
      false
    end

    def commit
      raw_connection.commit
    end

    def rollback
      raw_connection.rollback
    end

    def autocommit?
      raw_connection.getAutoCommit
    end

    def autocommit=(value)
      raw_connection.setAutoCommit(value)
    end

    def select_first(sql, *bindvars)
      stmt = prepare_statement(sql, *bindvars)
      rset = stmt.executeQuery
      metadata = rset.getMetaData
      column_count = metadata.getColumnCount
      if rset.next
        (1..column_count).map do |i|
          get_ruby_value_from_result_set(rset,i,metadata.getColumnTypeName(i))
        end
      else
        nil
      end
    ensure
      rset.close rescue nil
      stmt.close rescue nil
    end

    def select_all(sql, *bindvars, &block)
      stmt = prepare_statement(sql, *bindvars)
      results = []
      row_count = 0
      rset = stmt.executeQuery
      metadata = rset.getMetaData
      column_count = metadata.getColumnCount
      while rset.next
        row_with_typecast = (1..column_count).map do |i|
          get_ruby_value_from_result_set(rset,i,metadata.getColumnTypeName(i))
        end
        if block_given?
          yield(row_with_typecast)
          row_count += 1
        else
          results << row_with_typecast
        end
      end
      block_given? ? row_count : results
    ensure
      rset.close rescue nil
      stmt.close rescue nil
    end
    
    def exec(sql, *bindvars)
      cs = prepare_call(sql, *bindvars)
      cs.execute
      true
    ensure
      cs.close rescue nil
    end

    class Cursor

      def initialize(sql, conn)
        @sql = sql
        @connection = conn
        @params = sql.scan(/\:\w+/)
        @out_types = {}
        @out_index = {}
        @statement = @connection.prepare_call(sql)
      end

      def bind_param(key, value, type=nil, length=nil, in_out='IN')
        @connection.set_bind_variable(@statement, key, value, type, length)
        if in_out =~ /OUT/
          @out_types[key] = type || value.class
          @out_index[key] = bind_param_index(key)
          @statement.registerOutParameter(@out_index[key],@connection.get_java_sql_type(value,type))
        end
      end
      
      def exec
        @statement.execute
      end

      def [](key)
        @connection.get_bind_variable(@statement, @out_index[key], @out_types[key])
      end

      def close
        @statement.close
      end
      
      private
      
      def bind_param_index(key)
        return key if key.kind_of? Integer
        key = ":#{key.to_s}" unless key.to_s =~ /^:/
        @params.index(key)+1
      end
    end

    def parse(sql)
      Cursor.new(sql, self)
    end

    def prepare_statement(sql, *bindvars)
      stmt = raw_connection.prepareStatement(sql)
      bindvars.each_with_index do |bv, i|
        set_bind_variable(stmt, i+1, bv)
      end
      stmt
    end

    def prepare_call(sql, *bindvars)
      stmt = raw_connection.prepareCall(sql)
      bindvars.each_with_index do |bv, i|
        set_bind_variable(stmt, i+1, bv)
      end
      stmt
    end

    def get_java_sql_type(value, type)
      case type ? type.to_s : value.class.to_s
      when 'Fixnum', 'Bignum', 'Integer'
        java.sql.Types::INTEGER
      when 'Float'
        java.sql.Types::FLOAT
      when 'BigDecimal'
        java.sql.Types::NUMERIC
      when 'String'
        java.sql.Types::VARCHAR
      when 'Date'
        java.sql.Types::DATE
      when 'Time'
        java.sql.Types::DATE
      when 'DateTime'
        java.sql.Types::DATE
      else
        java.sql.Types::VARCHAR
      end
    end

    def set_bind_variable(stmt, i, value, type=nil, length=nil)
      key = i.kind_of?(Integer) ? nil : i.to_s.gsub(':','')
      case !value.nil? && type ? type.to_s : value.class.to_s
      when 'Fixnum', 'Bignum', 'Integer'
        stmt.send("setInt#{key && "AtName"}", key || i, value)
      when 'Float'
        stmt.send("setFloat#{key && "AtName"}", key || i, value)
      when 'BigDecimal'
        stmt.send("setBigDecimal#{key && "AtName"}", key || i, java.math.BigDecimal.new(value.to_s))
      when 'String'
        stmt.send("setString#{key && "AtName"}", key || i, value)
      when 'Date'
        stmt.send("setDate#{key && "AtName"}", key || i, java.sql.Date.new(Time.parse(value.to_s).to_i*1000))
      when 'Time'
        stmt.send("setTime#{key && "AtName"}", key || i, java.sql.Time.new(value.to_i*1000))
      when 'DateTime'
        stmt.send("setTime#{key && "AtName"}", key || i, java.sql.Time.new(Time.parse(value.strftime("%c")).to_i*1000))
      when 'NilClass'
        stmt.send("setNull#{key && "AtName"}", key || i, get_java_sql_type(value, type))
      end
    end
    
    def get_bind_variable(stmt, i, type)
      case type.to_s
      when 'Fixnum', 'Bignum', 'Integer'
        stmt.getInt(i)
      when 'Float'
        stmt.getFloat(i)
      when 'BigDecimal'
        bd = stmt.getBigDecimal(i)
        bd && BigDecimal.new(bd.to_s)
      when 'String'
        stmt.getString(i)
      when 'Date','Time','DateTime'
        ts = stmt.getTimestamp(i)
        # ts && Time.parse(Time.at(ts.getTime/1000).iso8601)
        ts && Time.local(1900+ts.year, ts.month+1, ts.date, ts.hours, ts.minutes, ts.seconds)
      end
    end

    def get_ruby_value_from_result_set(rset, i, type_name)
      case type_name
      when "CHAR", "VARCHAR2"
        rset.getString(i)
      when "NUMBER"
        d = rset.getBigDecimal(i)
        if d.nil?
          nil
        elsif d.scale == 0
          d.longValue
        else
          d.doubleValue
        end
      when "DATE", "TIMESTAMP"
        Time.at(rset.getTimestamp(i).getTime/1000)
      else
        nil
      end
    end
    
    def plsql_to_ruby_data_type(data_type, data_length)
      case data_type
      when "VARCHAR2"
        [String, data_length || 4000]
      when "NUMBER"
        [BigDecimal, nil]
      when "DATE"
        [Time, nil]
      when "TIMESTAMP"
        [Time, nil]
      # CLOB
      # BLOB
      else
        [String, 4000]
      end
    end

    def ruby_value_to_ora_value(val, type)
      if type == BigDecimal
        val.nil? || val.is_a?(Fixnum) ? val : val.to_f
      elsif type == Time
        date_to_time(val)
      else
        val
      end
    end

    def ora_value_to_ruby_value(val)
      case val
      when Float, BigDecimal
        ora_number_to_ruby_number(val)
      # when OraDate
      #   ora_date_to_ruby_date(val)
      else
        val
      end
    end

    private
    
    def ora_number_to_ruby_number(num)
      num.to_i == num.to_f ? num.to_i : num.to_f
    end
    
    # def ora_date_to_ruby_date(val)
    #   val.to_time
    # end

    def date_to_time(val)
      case val
      when Time
        val
      when DateTime
        Time.parse(val.strftime("%c"))
      when Date
        Time.parse(val.strftime("%c"))
      end
    end

  end
  
end