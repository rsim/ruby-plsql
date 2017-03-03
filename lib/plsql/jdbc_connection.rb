begin
  require "java"
  require "jruby"

  # ojdbc6.jar or ojdbc5.jar file should be in JRUBY_HOME/lib or should be in ENV['PATH'] or load path

  java_version = java.lang.System.getProperty("java.version")
  ojdbc_jars = if java_version =~ /^1.5/
    %w(ojdbc5.jar)
  elsif java_version =~ /^1.6/
    %w(ojdbc6.jar)
  elsif java_version >= "1.7"
    # Oracle 11g client ojdbc6.jar is also compatible with Java 1.7
    # Oracle 12c client provides new ojdbc7.jar
    %w(ojdbc7.jar ojdbc6.jar)
  else
    []
  end

  if ENV_JAVA["java.class.path"] !~ Regexp.new(ojdbc_jars.join("|"))
    # On Unix environment variable should be PATH, on Windows it is sometimes Path
    env_path = (ENV["PATH"] || ENV["Path"] || "").split(File::PATH_SEPARATOR)
    # Look for JDBC driver at first in lib subdirectory (application specific JDBC file version)
    # then in Ruby load path and finally in environment PATH
    ["./lib"].concat($LOAD_PATH).concat(env_path).detect do |dir|
      # check any compatible JDBC driver in the priority order
      ojdbc_jars.any? do |ojdbc_jar|
        if File.exists?(file_path = File.join(dir, ojdbc_jar))
          require file_path
          true
        end
      end
    end
  end

  java.sql.DriverManager.registerDriver Java::oracle.jdbc.OracleDriver.new

  # set tns_admin property from TNS_ADMIN environment variable
  if !java.lang.System.get_property("oracle.net.tns_admin") && ENV["TNS_ADMIN"]
    java.lang.System.set_property("oracle.net.tns_admin", ENV["TNS_ADMIN"])
  end

rescue LoadError, NameError
  # JDBC driver is unavailable.
  raise LoadError, "ERROR: ruby-plsql could not load Oracle JDBC driver. Please install #{ojdbc_jars.empty? ? "Oracle JDBC" : ojdbc_jars.join(' or ') } library."
end

module PLSQL
  class JDBCConnection < Connection  #:nodoc:

    def self.create_raw(params)
      database = params[:database]
      url = if ENV['TNS_ADMIN'] && database && !params[:host] && !params[:url]
        "jdbc:oracle:thin:@#{database}"
      else
        database = ":#{database}" unless database.match(/^(\:|\/)/)
        params[:url] || "jdbc:oracle:thin:@#{params[:host] || 'localhost'}:#{params[:port] || 1521}#{database}"
      end
      new(java.sql.DriverManager.getConnection(url, params[:username], params[:password]))
    end

    def set_time_zone(time_zone=nil)
      raw_connection.setSessionTimeZone(time_zone) if time_zone
    end

    def logoff
      super
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

    def prefetch_rows=(value)
      raw_connection.setDefaultRowPrefetch(value)
    end

    def exec(sql, *bindvars)
      cs = prepare_call(sql, *bindvars)
      cs.execute
      true
    ensure
      cs.close rescue nil
    end

    class CallableStatement #:nodoc:

      def initialize(conn, sql)
        @sql = sql
        @connection = conn
        @params = sql.scan(/\:\w+/)
        @out_types = {}
        @out_index = {}
        @statement = @connection.prepare_call(sql)
      end

      def bind_param(arg, value, metadata)
        type, length = @connection.plsql_to_ruby_data_type(metadata)
        ora_value = @connection.ruby_value_to_ora_value(value, type, metadata)
        @connection.set_bind_variable(@statement, arg, ora_value, type, length, metadata)
        if metadata[:in_out] =~ /OUT/
          @out_types[arg] = type || ora_value.class
          @out_index[arg] = bind_param_index(arg)
          if ['TABLE','VARRAY','OBJECT','XMLTYPE'].include?(metadata[:data_type])
            @statement.registerOutParameter(@out_index[arg], @connection.get_java_sql_type(ora_value,type),
              metadata[:sql_type_name])
          else
            @statement.registerOutParameter(@out_index[arg],@connection.get_java_sql_type(ora_value,type))
          end
        end
      end

      def exec
        @statement.execute
      end

      def [](key)
        @connection.ora_value_to_ruby_value(@connection.get_bind_variable(@statement, @out_index[key], @out_types[key]))
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

    class Cursor #:nodoc:
      include Connection::CursorCommon

      attr_reader :result_set
      attr_accessor :statement

      def initialize(conn, result_set)
        @connection = conn
        @result_set = result_set
        @metadata = @result_set.getMetaData
        @column_count = @metadata.getColumnCount
        @column_type_names = [nil] # column numbering starts at 1
        (1..@column_count).each do |i|
          @column_type_names << {:type_name => @metadata.getColumnTypeName(i), :sql_type => @metadata.getColumnType(i)}
        end
      end

      def self.new_from_query(conn, sql, bindvars=[], options={})
        stmt = conn.prepare_statement(sql, *bindvars)
        if prefetch_rows = options[:prefetch_rows]
          stmt.setRowPrefetch(prefetch_rows)
        end
        cursor = Cursor.new(conn, stmt.executeQuery)
        cursor.statement = stmt
        cursor
      rescue
        # in case of any error close statement
        stmt.close rescue nil
        raise
      end

      def fetch
        if @result_set.next
          (1..@column_count).map do |i|
            @connection.get_ruby_value_from_result_set(@result_set, i, @column_type_names[i])
          end
        else
          nil
        end
      end

      def fields
        @fields ||= (1..@column_count).map do |i|
          @metadata.getColumnName(i).downcase.to_sym
        end
      end

      def close
        @result_set.close
        @statement.close if @statement
      end
    end

    def parse(sql)
      CallableStatement.new(self, sql)
    end

    def cursor_from_query(sql, bindvars=[], options={})
      Cursor.new_from_query(self, sql, bindvars, options)
    end

    def prepare_statement(sql, *bindvars)
      stmt = raw_connection.prepareStatement(sql)
      bindvars.each_with_index do |bv, i|
        set_bind_variable(stmt, i+1, ruby_value_to_ora_value(bv))
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

    RUBY_CLASS_TO_SQL_TYPE = {
      Fixnum => java.sql.Types::INTEGER,
      Bignum => java.sql.Types::INTEGER,
      Integer => java.sql.Types::INTEGER,
      Float => java.sql.Types::FLOAT,
      BigDecimal => java.sql.Types::NUMERIC,
      String => java.sql.Types::VARCHAR,
      Java::OracleSql::CLOB => Java::oracle.jdbc.OracleTypes::CLOB,
      Java::OracleSql::BLOB => Java::oracle.jdbc.OracleTypes::BLOB,
      Date => java.sql.Types::DATE,
      Time => java.sql.Types::TIMESTAMP,
      DateTime => java.sql.Types::DATE,
      Java::OracleSql::ARRAY => Java::oracle.jdbc.OracleTypes::ARRAY,
      Array => Java::oracle.jdbc.OracleTypes::ARRAY,
      Java::OracleSql::STRUCT => Java::oracle.jdbc.OracleTypes::STRUCT,
      Hash => Java::oracle.jdbc.OracleTypes::STRUCT,
      java.sql.ResultSet => Java::oracle.jdbc.OracleTypes::CURSOR,
    }

    SQL_TYPE_TO_RUBY_CLASS = {
      java.sql.Types::CHAR => String,
      java.sql.Types::NCHAR => String,
      java.sql.Types::VARCHAR => String,
      java.sql.Types::NVARCHAR => String,
      java.sql.Types::LONGVARCHAR => String,
      java.sql.Types::NUMERIC => BigDecimal,
      java.sql.Types::INTEGER => Integer,
      java.sql.Types::DATE => Time,
      java.sql.Types::TIMESTAMP => Time,
      Java::oracle.jdbc.OracleTypes::TIMESTAMPTZ => Time,
      Java::oracle.jdbc.OracleTypes::TIMESTAMPLTZ => Time,
      java.sql.Types::BLOB => String,
      java.sql.Types::CLOB => String,
      java.sql.Types::ARRAY => Java::OracleSql::ARRAY,
      java.sql.Types::STRUCT => Java::OracleSql::STRUCT,
      Java::oracle.jdbc.OracleTypes::CURSOR => java.sql.ResultSet
    }

    def get_java_sql_type(value, type)
      RUBY_CLASS_TO_SQL_TYPE[type || value.class] || java.sql.Types::VARCHAR
    end

    def set_bind_variable(stmt, i, value, type=nil, length=nil, metadata={})
      key = i.kind_of?(Integer) ? nil : i.to_s.gsub(':','')
      type_symbol = (!value.nil? && type ? type : value.class).to_s.to_sym
      case type_symbol
      when :Fixnum, :Bignum, :Integer
        stmt.send("setInt#{key && "AtName"}", key || i, value)
      when :Float
        stmt.send("setFloat#{key && "AtName"}", key || i, value)
      when :BigDecimal, :'Java::JavaMath::BigDecimal'
        stmt.send("setBigDecimal#{key && "AtName"}", key || i, value)
      when :String
        stmt.send("setString#{key && "AtName"}", key || i, value)
      when :'Java::OracleSql::CLOB'
        stmt.send("setClob#{key && "AtName"}", key || i, value)
      when :'Java::OracleSql::BLOB'
        stmt.send("setBlob#{key && "AtName"}", key || i, value)
      when :Date, :DateTime, :'Java::OracleSql::DATE'
        stmt.send("setDATE#{key && "AtName"}", key || i, value)
      when :Time, :'Java::JavaSql::Timestamp'
        stmt.send("setTimestamp#{key && "AtName"}", key || i, value)
      when :NilClass
        if ['TABLE', 'VARRAY', 'OBJECT','XMLTYPE'].include?(metadata[:data_type])
          stmt.send("setNull#{key && "AtName"}", key || i, get_java_sql_type(value, type),
            metadata[:sql_type_name])
        elsif metadata[:data_type] == 'REF CURSOR'
          # TODO: cannot bind NULL value to cursor parameter, getting error
          # java.sql.SQLException: Unsupported feature: sqlType=-10
          # Currently do nothing and assume that NULL values will not be passed to IN parameters
          # If cursor is IN/OUT or OUT parameter then it should work
        else
          stmt.send("setNull#{key && "AtName"}", key || i, get_java_sql_type(value, type))
        end
      when :'Java::OracleSql::ARRAY'
        stmt.send("setARRAY#{key && "AtName"}", key || i, value)
      when :'Java::OracleSql::STRUCT'
        stmt.send("setSTRUCT#{key && "AtName"}", key || i, value)
      when :'Java::JavaSql::ResultSet'
        # TODO: cannot find how to pass cursor parameter from JDBC
        # setCursor is giving exception java.sql.SQLException: Unsupported feature
        stmt.send("setCursor#{key && "AtName"}", key || i, value)
      else
        raise ArgumentError, "Don't know how to bind variable with type #{type_symbol}"
      end
    end

    def get_bind_variable(stmt, i, type)
      case type.to_s.to_sym
      when :Fixnum, :Bignum, :Integer
        stmt.getObject(i)
      when :Float
        stmt.getFloat(i)
      when :BigDecimal
        bd = stmt.getBigDecimal(i)
        bd && BigDecimal.new(bd.to_s)
      when :String
        stmt.getString(i)
      when :'Java::OracleSql::CLOB'
        stmt.getClob(i)
      when :'Java::OracleSql::BLOB'
        stmt.getBlob(i)
      when :Date, :DateTime
        stmt.getDATE(i)
      when :Time
        stmt.getTimestamp(i)
      when :'Java::OracleSql::ARRAY'
        stmt.getArray(i)
      when :'Java::OracleSql::STRUCT'
        stmt.getSTRUCT(i)
      when :'Java::JavaSql::ResultSet'
        stmt.getCursor(i)
      end
    end

    def get_ruby_value_from_result_set(rset, i, metadata)
      ruby_type = SQL_TYPE_TO_RUBY_CLASS[metadata[:sql_type]]
      ora_value = get_bind_variable(rset, i, ruby_type)
      result_new = ora_value_to_ruby_value(ora_value)
    end

    def result_set_to_ruby_data_type(column_type, column_type_name)

    end

    def plsql_to_ruby_data_type(metadata)
      data_type, data_length = metadata[:data_type], metadata[:data_length]
      case data_type
      when "VARCHAR", "VARCHAR2", "CHAR", "NVARCHAR2", "NCHAR"
        [String, data_length || 32767]
      when "CLOB", "NCLOB"
        [Java::OracleSql::CLOB, nil]
      when "BLOB"
        [Java::OracleSql::BLOB, nil]
      when "NUMBER"
        [BigDecimal, nil]
      when "NATURAL", "NATURALN", "POSITIVE", "POSITIVEN", "SIGNTYPE", "SIMPLE_INTEGER", "PLS_INTEGER", "BINARY_INTEGER"
        [Integer, nil]
      when "DATE"
        [DateTime, nil]
      when "TIMESTAMP", "TIMESTAMP WITH TIME ZONE", "TIMESTAMP WITH LOCAL TIME ZONE"
        [Time, nil]
      when "TABLE", "VARRAY"
        [Java::OracleSql::ARRAY, nil]
      when "OBJECT"
        [Java::OracleSql::STRUCT, nil]
      when "REF CURSOR"
        [java.sql.ResultSet, nil]
      else
        [String, 32767]
      end
    end

    def ruby_value_to_ora_value(value, type=nil, metadata={})
      type ||= value.class
      case type.to_s.to_sym
      when :Integer
        value
      when :String
        value.to_s
      when :BigDecimal
        case value
        when TrueClass
          java_bigdecimal(1)
        when FalseClass
          java_bigdecimal(0)
        else
          java_bigdecimal(value)
        end
      when :Date, :DateTime
        case value
        when DateTime
          java_date(Time.send(plsql.default_timezone, value.year, value.month, value.day, value.hour, value.min, value.sec))
        when Date
          java_date(Time.send(plsql.default_timezone, value.year, value.month, value.day, 0, 0, 0))
        else
          java_date(value)
        end
      when :Time
        java_timestamp(value)
      when :'Java::OracleSql::CLOB'
        if value
          clob = Java::OracleSql::CLOB.createTemporary(raw_connection, false, Java::OracleSql::CLOB::DURATION_SESSION)
          clob.setString(1, value)
          clob
        else
          nil
        end
      when :'Java::OracleSql::BLOB'
        if value
          blob = Java::OracleSql::BLOB.createTemporary(raw_connection, false, Java::OracleSql::BLOB::DURATION_SESSION)
          blob.setBytes(1, value.to_java_bytes)
          blob
        else
          nil
        end
      when :'Java::OracleSql::ARRAY'
        if value
          raise ArgumentError, "You should pass Array value for collection type parameter" unless value.is_a?(Array)
          descriptor = Java::OracleSql::ArrayDescriptor.createDescriptor(metadata[:sql_type_name], raw_connection)
          elem_type = descriptor.getBaseType
          elem_type_name = descriptor.getBaseName
          elem_list = value.map do |elem|
            case elem_type
            when Java::oracle.jdbc.OracleTypes::ARRAY
              ruby_value_to_ora_value(elem, Java::OracleSql::ARRAY, :sql_type_name => elem_type_name)
            when Java::oracle.jdbc.OracleTypes::STRUCT
              ruby_value_to_ora_value(elem, Java::OracleSql::STRUCT, :sql_type_name => elem_type_name)
            else
              ruby_value_to_ora_value(elem)
            end
          end
          Java::OracleSql::ARRAY.new(descriptor, raw_connection, elem_list.to_java)
        end
      when :'Java::OracleSql::STRUCT'
        if value
          raise ArgumentError, "You should pass Hash value for object type parameter" unless value.is_a?(Hash)
          descriptor = Java::OracleSql::StructDescriptor.createDescriptor(metadata[:sql_type_name], raw_connection)
          struct_metadata = descriptor.getMetaData
          struct_fields = (1..descriptor.getLength).inject({}) do |hash, i|
            hash[struct_metadata.getColumnName(i).downcase.to_sym] =
              {:type => struct_metadata.getColumnType(i), :type_name => struct_metadata.getColumnTypeName(i)}
            hash
          end
          object_attrs = java.util.HashMap.new
          value.each do |key, attr_value|
            raise ArgumentError, "Wrong object type field passed to PL/SQL procedure" unless (field = struct_fields[key])
            case field[:type]
            when Java::oracle.jdbc.OracleTypes::ARRAY
              # nested collection
              object_attrs.put(key.to_s.upcase, ruby_value_to_ora_value(attr_value, Java::OracleSql::ARRAY, :sql_type_name => field[:type_name]))
            when Java::oracle.jdbc.OracleTypes::STRUCT
              # nested object type
              object_attrs.put(key.to_s.upcase, ruby_value_to_ora_value(attr_value, Java::OracleSql::STRUCT, :sql_type_name => field[:type_name]))
            else
              object_attrs.put(key.to_s.upcase, ruby_value_to_ora_value(attr_value))
            end
          end
          Java::OracleSql::STRUCT.new(descriptor, raw_connection, object_attrs)
        end
      when :'Java::JavaSql::ResultSet'
        if value
          value.result_set
        end
      else
        value
      end
    end

    def ora_value_to_ruby_value(value)
      case value
      when Float, BigDecimal
        ora_number_to_ruby_number(value)
      when Java::JavaMath::BigDecimal
        value && ora_number_to_ruby_number(BigDecimal.new(value.to_s))
      when Java::OracleSql::DATE
        if value
          d = value.dateValue
          t = value.timeValue
          Time.send(plsql.default_timezone, d.year + 1900, d.month + 1, d.date, t.hours, t.minutes, t.seconds)
        end
      when Java::JavaSql::Timestamp
        if value
          Time.send(plsql.default_timezone, value.year + 1900, value.month + 1, value.date, value.hours, value.minutes, value.seconds,
            value.nanos / 1000)
        end
      when Java::OracleSql::CLOB
        if value.isEmptyLob
          nil
        else
          value.getSubString(1, value.length)
        end
      when Java::OracleSql::BLOB
        if value.isEmptyLob
          nil
        else
          String.from_java_bytes(value.getBytes(1, value.length))
        end
      when Java::OracleSql::ARRAY
        value.getArray.map{|e| ora_value_to_ruby_value(e)}
      when Java::OracleSql::STRUCT
        descriptor = value.getDescriptor
        struct_metadata = descriptor.getMetaData
        field_names = (1..descriptor.getLength).map {|i| struct_metadata.getColumnName(i).downcase.to_sym}
        field_values = value.getAttributes.map{|e| ora_value_to_ruby_value(e)}
        ArrayHelpers::to_hash(field_names, field_values)
      when Java::java.sql.ResultSet
        Cursor.new(self, value)
      else
        value
      end
    end

    def database_version
      @database_version ||= if md = raw_connection.getMetaData
        major = md.getDatabaseMajorVersion
        minor = md.getDatabaseMinorVersion
        if md.getDatabaseProductVersion =~ /#{major}\.#{minor}\.(\d+)\.(\d+)/
          update = $1.to_i
          patch = $2.to_i
        else
          update = patch = 0
        end
        [major, minor, update, patch]
      end
    end

    private

    def java_date(value)
      value && Java::oracle.sql.DATE.new(value.strftime("%Y-%m-%d %H:%M:%S"))
    end

    def java_timestamp(value)
      value && Java::java.sql.Timestamp.new(value.year-1900, value.month-1, value.day, value.hour, value.min, value.sec, value.usec * 1000)
    end

    def java_bigdecimal(value)
      value && java.math.BigDecimal.new(value.to_s)
    end

    def ora_number_to_ruby_number(num)
      # return BigDecimal instead of Float to avoid rounding errors
      num == (num_to_i = num.to_i) ? num_to_i : (num.is_a?(BigDecimal) ? num : BigDecimal.new(num.to_s))
    end

  end

end
