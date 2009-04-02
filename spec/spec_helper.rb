require "rubygems"
gem "rspec"
require "spec"

gem "activerecord"
require "activerecord"
gem "activerecord-oracle_enhanced-adapter"

require File.expand_path(File.dirname(__FILE__) + "/../lib/ruby_plsql")

def get_connection
  unless defined?(JRUBY_VERSION)
    begin
      OCI8.new("hr","hr","xe")
    # if connection fails then sleep 5 seconds and retry
    rescue OCIError
      sleep 5
      OCI8.new("hr","hr","xe")
    end
  else
    begin
      java.sql.DriverManager.getConnection("jdbc:oracle:thin:@ubuntu810:1521:XE","hr","hr")
    # if connection fails then sleep 5 seconds and retry
    rescue NativeException
      sleep 5
      java.sql.DriverManager.getConnection("jdbc:oracle:thin:@ubuntu810:1521:XE","hr","hr")
    end
  end
end

CONNECTION_PARAMS = {
  :adapter => "oracle_enhanced",
  :database => "xe",
  :host => "ubuntu810",
  :username => "hr",
  :password => "hr"
}
