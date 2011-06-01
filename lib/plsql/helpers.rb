module PLSQL #:nodoc:
  
  module ArrayHelpers #:nodoc:

    def self.to_hash(keys, values) #:nodoc:
      (0...keys.size).inject({}) { |hash, i| hash[keys[i]] = values[i]; hash }
    end

  end
  
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
      select_all("SELECT table_name FROM information_schema.tables WHERE table_type = 'LOCAL TEMPORARY' AND table_name ILIKE $1",
        PLSQL::Connection::RUBY_TEMP_TABLE_PREFIX.upcase + '%').each do |row|
        exec "TRUNCATE TABLE #{row[0]}"
        exec "DROP TABLE #{row[0]}"
      end
    end

    # Drop ruby temporary tables created in current session that are used for calling packages with table parameter types defined in packages
    def drop_session_ruby_temporary_tables
      select_all("SELECT table_name FROM information_schema.tables WHERE table_type = 'LOCAL TEMPORARY' AND table_name ILIKE $1",
        PLSQL::Connection::RUBY_TEMP_TABLE_PREFIX.upcase + "#{session_id}_%").each do |row|
        exec "TRUNCATE TABLE #{row[0]}"
        exec "DROP TABLE #{row[0]}"
      end
    end
    
  end
  
end