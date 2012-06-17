require "time"
require "date"
require "bigdecimal"

%w(connection sql_statements schema
   procedure subprogram_call procedure_call
   pipelined_function pipelined_function_call
   package variable
   table view sequence type
   version helpers).each do |file|
  require "plsql/#{file}"
end

if defined?(JRUBY_VERSION)
  require "plsql/jdbc_connection"
else
  require "plsql/oci_connection"
end
