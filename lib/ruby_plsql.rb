require "time"
require "date"
require "bigdecimal"

%w(connection sql_statements schema procedure procedure_call package variable table view sequence type version helpers).each do |file|
  require "plsql/#{file}"
end

unless defined?(JRUBY_VERSION)
  require "plsql/oci_connection"
else
  require "plsql/jdbc_connection"
end
