require "rubygems"
require "bundler"
Bundler.setup(:default, :development)
require 'simplecov'

SimpleCov.configure do
  load_profile 'root_filter'
  load_profile 'test_frameworks'
end

ENV["COVERAGE"] && SimpleCov.start do
  add_filter "/.rvm/"
end

$LOAD_PATH.unshift(File.join(File.dirname(__FILE__), '..', 'lib'))
require 'rspec'

unless ENV['NO_ACTIVERECORD']
  require 'active_record'
else
  puts 'Without ActiveRecord'
end

require 'ruby-plsql'

DATABASE_NAME = ENV['DATABASE_NAME'] || 'orcl'
DATABASE_SERVICE_NAME = (defined?(JRUBY_VERSION) ? "/" : "") +
                        (ENV['DATABASE_SERVICE_NAME'] || DATABASE_NAME)
DATABASE_HOST = ENV['DATABASE_HOST'] || 'localhost'
DATABASE_PORT = (ENV['DATABASE_PORT'] || 1521).to_i
DATABASE_USERS_AND_PASSWORDS = [
  [ENV['DATABASE_USER'] || 'hr', ENV['DATABASE_PASSWORD'] || 'hr'],
  [ENV['DATABASE_USER2'] || 'arunit', ENV['DATABASE_PASSWORD2'] || 'arunit']
]
# specify which database version is used (will be verified in one test)
DATABASE_VERSION = ENV['DATABASE_VERSION'] || '10.2.0.4'

def get_connection(user_number = 0)
  database_user, database_password = DATABASE_USERS_AND_PASSWORDS[user_number]
  unless defined?(JRUBY_VERSION)
    try_to_connect(OCIError) do
      OCI8.new(database_user, database_password, DATABASE_NAME)
    end
  else
    try_to_connect(NativeException) do
      java.sql.DriverManager.getConnection("jdbc:oracle:thin:@#{DATABASE_HOST}:#{DATABASE_PORT}#{DATABASE_SERVICE_NAME}",
        database_user, database_password)
    end
  end
end

def try_to_connect(exception)
  begin
    yield
  # if connection fails then sleep 5 seconds and retry
  rescue exception
    sleep 5
    yield
  end
end

CONNECTION_PARAMS = {
  :adapter => "oracle_enhanced",
  :database => DATABASE_SERVICE_NAME,
  :host => DATABASE_HOST,
  :port => DATABASE_PORT,
  :username => DATABASE_USERS_AND_PASSWORDS[0][0],
  :password => DATABASE_USERS_AND_PASSWORDS[0][1]
}

class Hash
  def except(*blacklist)
    self.reject {|key, value| blacklist.include?(key) }
  end unless method_defined?(:except)

  def only(*whitelist)
    self.reject {|key, value| !whitelist.include?(key) }
  end unless method_defined?(:only)
end

# set default time zone in TZ environment variable
# which will be used to set session time zone
ENV['TZ'] ||= 'Europe/Riga'
# ENV['TZ'] ||= 'UTC'
