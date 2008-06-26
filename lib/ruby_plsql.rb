$:.unshift File.dirname(__FILE__)

module RubyPlsql #:nodoc:
end

unless defined?(JRUBY_VERSION)
  begin
    require "oci8"
    require "oradate_patch"
  rescue LoadError
      puts <<-EOS
    To use ruby_plsql you must install ruby-oci8 library.
      EOS
  end
else
  begin
    require "java"
    ojdbc_jar = "ojdbc14.jar"
    if ojdbc_jar_path = ENV["PATH"].split(":").find{|d| File.exists?(File.join(d,ojdbc_jar))}
      require File.join(ojdbc_jar_path,ojdbc_jar)
    else
      require ojdbc_jar
    end
    import java.sql.Statement
    import java.sql.Connection
    import java.sql.SQLException
    import java.sql.Types
    import java.sql.DriverManager
    DriverManager.registerDriver Java::oracle.jdbc.driver.OracleDriver.new
  rescue LoadError
      puts <<-EOS
    To use ruby_plsql you must have Oracle JDBC driver installed.
      EOS
  end
end

require "time"
require "date"
require "bigdecimal"

%w(connection schema procedure package).each do |file|
  require File.dirname(__FILE__) + "/plsql/#{file}"
end
