module PLSQL

  module OraConnectionHelper

    # Returns session ID
    def session_id
      @session_id ||= select_first("SELECT TO_NUMBER(USERENV('SESSIONID')) FROM dual")[0]
    end

    # Set time zone (default taken from TZ environment variable)
    def set_time_zone(time_zone=nil)
      time_zone ||= ENV['TZ']
      exec("ALTER SESSION SET time_zone = '#{time_zone}'") if time_zone
    end

    # Returns session time zone
    def time_zone
      select_first("SELECT SESSIONTIMEZONE FROM dual")[0]
    end

    # Drop all ruby temporary tables that are used for calling packages with table parameter types defined in packages
    def drop_all_ruby_temporary_tables
      select_all("SELECT table_name FROM user_tables WHERE temporary='Y' AND table_name LIKE :table_name",
        PLSQL::Connection::RUBY_TEMP_TABLE_PREFIX.upcase+'%').each do |row|
        exec "TRUNCATE TABLE #{row[0]}"
        exec "DROP TABLE #{row[0]}"
      end
    end

    # Drop ruby temporary tables created in current session that are used for calling packages with table parameter types defined in packages
    def drop_session_ruby_temporary_tables
      select_all("SELECT table_name FROM user_tables WHERE temporary='Y' AND table_name LIKE :table_name",
        PLSQL::Connection::RUBY_TEMP_TABLE_PREFIX.upcase+"#{session_id}_%").each do |row|
        exec "TRUNCATE TABLE #{row[0]}"
        exec "DROP TABLE #{row[0]}"
      end
    end

  end
  
  module PGConnectionHelper
    
    # Returns session ID
    def session_id
      @session_id ||= select_first("SELECT pg_backend_pid()")[0]
    end

    # Set time zone (default taken from TZ environment variable)
    def set_time_zone(time_zone=nil)
      time_zone ||= ENV['TZ']
      exec("SET TIME ZONE '#{time_zone}'") if time_zone
    end

    # Returns session time zone
    def time_zone
      select_first("SHOW TIMEZONE")[0]
    end

    # Drop all ruby temporary tables that are used for calling packages with table parameter types defined in packages
    def drop_all_ruby_temporary_tables
      select_all("SELECT table_name FROM information_schema.tables WHERE table_type = 'LOCAL TEMPORARY' AND table_name ILIKE '" +
          PLSQL::Connection::RUBY_TEMP_TABLE_PREFIX.upcase + "%'").each do |row|
        exec "TRUNCATE TABLE #{row[0]}"
        exec "DROP TABLE #{row[0]}"
      end
    end

    # Drop ruby temporary tables created in current session that are used for calling packages with table parameter types defined in packages
    def drop_session_ruby_temporary_tables
      select_all("SELECT table_name FROM information_schema.tables WHERE table_type = 'LOCAL TEMPORARY' AND table_name ILIKE '" +
          PLSQL::Connection::RUBY_TEMP_TABLE_PREFIX.upcase + "#{session_id}_%'").each do |row|
        exec "TRUNCATE TABLE #{row[0]}"
        exec "DROP TABLE #{row[0]}"
      end
    end
    
  end
  
  module JDBCConnectionHelper
    
    def logoff
      super
      raw_connection.close
      true
    rescue
      false
    end
    
    def exec(sql, *bindvars)
      cs = prepare_call(sql, *bindvars)
      cs.execute
      true
    ensure
      cs.close rescue nil
    end
    
    def parse(sql, params = {})
      create_callable_stmt(self, sql, params)
    end

    def cursor_from_query(sql, bindvars=[], options={})
      Cursor.new_from_query(self, sql, bindvars, options)
    end

    def prepare_statement(sql, *bindvars)
      stmt = raw_connection.prepareStatement(sql)
      bindvars.each_with_index do |bv, i|
        set_bind_variable(stmt, i+1, ruby_value_to_db_value(bv))
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
        if prefetch_rows = (options[:prefetch_rows] || conn.prefetch_rows)
          case conn.dialect
          when :oracle
            stmt.setRowPrefetch(prefetch_rows)
          when :postgres
            stmt.setFetchSize(prefetch_rows)
          end
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
    
    def database_version
      @database_version ||= if md = raw_connection.getMetaData
        major = md.getDatabaseMajorVersion
        minor = md.getDatabaseMinorVersion
        if md.getDatabaseProductVersion =~ /#{major}\.#{minor}\.(\d+)(\.(\d+))?/
          update = $1.to_i
          patch = $3.to_i if $2
        else
          update = patch = 0
        end
        [major, minor, update, patch].compact
      end
    end
    
  end
  
end