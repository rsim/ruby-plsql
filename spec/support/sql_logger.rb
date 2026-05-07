# Always write SQL executed during the spec run to debug.log.
# Logger args mirror activerecord-oracle_enhanced-adapter's spec logger
# (shift_age, shift_size) so debug.log behaves the same way.
require "logger"

PLSQL_DEBUG_LOGGER = Logger.new("debug.log", 0, 100 * 1024 * 1024)
PLSQL_DEBUG_LOGGER.formatter = ->(_sev, time, _prog, msg) {
  "#{time.iso8601(6)}  #{msg}\n"
}

module PLSQLSQLLogger
  def exec(sql, *bindvars)
    PLSQL_DEBUG_LOGGER.info("EXEC   #{sql.strip}#{bindvars.empty? ? '' : "  BINDS=#{bindvars.inspect}"}")
    super
  end

  def cursor_from_query(sql, bindvars = [], options = {})
    PLSQL_DEBUG_LOGGER.info("QUERY  #{sql.strip}#{bindvars.empty? ? '' : "  BINDS=#{bindvars.inspect}"}")
    super
  end

  def parse(sql)
    PLSQL_DEBUG_LOGGER.info("PARSE  #{sql.strip}")
    super
  end
end

PLSQL::OCIConnection.prepend(PLSQLSQLLogger)  if defined?(PLSQL::OCIConnection)
PLSQL::JDBCConnection.prepend(PLSQLSQLLogger) if defined?(PLSQL::JDBCConnection)

if defined?(ActiveRecord::Base)
  ActiveRecord::Base.logger = PLSQL_DEBUG_LOGGER
end
