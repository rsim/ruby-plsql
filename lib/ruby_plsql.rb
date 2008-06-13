$:.unshift File.dirname(__FILE__)

module RubyPlsql #:nodoc:
end

unless defined?(JRUBY_VERSION)
  begin
    require "oci8"

  rescue LoadError
      puts <<-EOS
    To use ruby_plsql you must install ruby-oci8 library.
      EOS
  end
else
  begin
    require "java"
    require "#{ENV['SQLPATH']}/ojdbc14.jar"
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

%w(connection schema procedure package).each do |file|
  require File.dirname(__FILE__) + "/plsql/#{file}"
end
