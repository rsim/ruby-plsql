begin
  require 'spec'
rescue LoadError
  require 'rubygems'
  gem 'rspec'
  require 'spec'
end

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
    DriverManager.getConnection("jdbc:oracle:thin:@ubuntu710:1521:XE","hr","hr")
  end
end