require "rubygems"
gem "rspec"
require "spec"

gem "activerecord"
require "active_record"
gem "activerecord-oracle_enhanced-adapter"

if !defined?(RUBY_ENGINE) || RUBY_ENGINE == 'ruby'
  # gem "ruby-oci8", "=2.0.2"
  gem "ruby-oci8", ">=2.0.3"
end

$:.unshift(File.dirname(__FILE__) + '/../lib')

require "ruby_plsql"

DATABASE_NAME = ENV['DATABASE_NAME'] || 'orcl'
DATABASE_HOST = ENV['DATABASE_HOST'] || 'localhost'
DATABASE_PORT = ENV['DATABASE_PORT'] || 1521
DATABASE_USERS_AND_PASSWORDS = [
  [ENV['DATABASE_USER'] || 'hr', ENV['DATABASE_PASSWORD'] || 'hr'],
  [ENV['DATABASE_USER2'] || 'arunit', ENV['DATABASE_PASSWORD2'] || 'arunit']
]

def get_connection(user_number = 0)
  database_user, database_password = DATABASE_USERS_AND_PASSWORDS[user_number]
  unless defined?(JRUBY_VERSION)
    begin
      OCI8.new(database_user, database_password, DATABASE_NAME)
    # if connection fails then sleep 5 seconds and retry
    rescue OCIError
      sleep 5
      OCI8.new(database_user, database_password, DATABASE_NAME)
    end
  else
    begin
      java.sql.DriverManager.getConnection("jdbc:oracle:thin:@#{DATABASE_HOST}:#{DATABASE_PORT}:#{DATABASE_NAME}",
        database_user, database_password)
    # if connection fails then sleep 5 seconds and retry
    rescue NativeException
      sleep 5
      java.sql.DriverManager.getConnection("jdbc:oracle:thin:@#{DATABASE_HOST}:#{DATABASE_PORT}:#{DATABASE_NAME}",
        database_user, database_password)
    end
  end
end

CONNECTION_PARAMS = {
  :adapter => "oracle_enhanced",
  :database => DATABASE_NAME,
  :host => DATABASE_HOST,
  :port => DATABASE_PORT,
  :username => DATABASE_USERS_AND_PASSWORDS[0][0],
  :password => DATABASE_USERS_AND_PASSWORDS[0][1]
}
