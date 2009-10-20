require "rubygems"
gem "rspec"
require "spec"

gem "activerecord"
require "activerecord"
gem "activerecord-oracle_enhanced-adapter"

$:.unshift(File.dirname(__FILE__) + '/../lib')

require "ruby_plsql"

DATABASE_NAME = ENV['DATABASE_NAME'] || 'orcl'
DATABASE_HOST = ENV['DATABASE_HOST'] || 'localhost'
DATABASE_PORT = ENV['DATABASE_PORT'] || 1521
DATABASE_USER = ENV['DATABASE_USER'] || 'hr'
DATABASE_PASSWORD = ENV['DATABASE_PASSWORD'] || 'hr'

def get_connection
  unless defined?(JRUBY_VERSION)
    begin
      OCI8.new(DATABASE_USER,DATABASE_PASSWORD,DATABASE_NAME)
    # if connection fails then sleep 5 seconds and retry
    rescue OCIError
      sleep 5
      OCI8.new(DATABASE_USER,DATABASE_PASSWORD,DATABASE_NAME)
    end
  else
    begin
      java.sql.DriverManager.getConnection("jdbc:oracle:thin:@#{DATABASE_HOST}:#{DATABASE_PORT}:#{DATABASE_NAME}",
        DATABASE_USER,DATABASE_PASSWORD)
    # if connection fails then sleep 5 seconds and retry
    rescue NativeException
      sleep 5
      java.sql.DriverManager.getConnection("jdbc:oracle:thin:@#{DATABASE_HOST}:#{DATABASE_PORT}:#{DATABASE_NAME}",
        DATABASE_USER,DATABASE_PASSWORD)
    end
  end
end

CONNECTION_PARAMS = {
  :adapter => "oracle_enhanced",
  :database => DATABASE_NAME,
  :host => DATABASE_HOST,
  :port => DATABASE_PORT,
  :username => DATABASE_USER,
  :password => DATABASE_PASSWORD
}
