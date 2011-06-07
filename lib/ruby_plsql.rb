require "time"
require "date"
require "bigdecimal"

%w(connection sql_statements schema procedure procedure_call package variable table view sequence type version helpers).each do |file|
  require "plsql/#{file}"
end

unless defined?(JRuby)
  require "plsql/oci_connection"
  require "plsql/pg_connection"
else
  require "plsql/jdbc_ora_connection"
end
