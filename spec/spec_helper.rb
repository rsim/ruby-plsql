require "rubygems"

# Set up gems listed in the Gemfile.
gemfile = File.expand_path('../../Gemfile', __FILE__)
begin
  ENV['BUNDLE_GEMFILE'] = gemfile
  require 'bundler'
  Bundler.setup
rescue Bundler::GemNotFound => e
  STDERR.puts e.message
  STDERR.puts "Try running `bundle install`."
  exit!
end if File.exist?(gemfile)

$:.unshift(File.dirname(__FILE__) + '/../lib')

require 'spec'

unless ENV['NO_ACTIVERECORD']
  require 'active_record'
else
  puts 'Without ActiveRecord'
end

require 'ruby-plsql'

# Oracle-specific database connection parameters.
ORA_DATABASE_NAME = ENV['ORA_DATABASE_NAME'] || 'orcl'
ORA_DATABASE_PORT = ENV['ORA_DATABASE_PORT'] || 1521
ORA_DATABASE_VERSION = ENV['ORA_DATABASE_VERSION'] || '10.2.0.4'

# Postgres-specific database connection parameters.
PG_DATABASE_NAME = ENV['PG_DATABASE_NAME'] || 'postgres'
PG_DATABASE_PORT = ENV['PG_DATABASE_PORT'] || 5432
PG_DATABASE_VERSION = ENV["PG_DATABASE_VERSION"] || '9.0.3'

# Generic database connection parameters.
DATABASE_HOST = ENV['DATABASE_HOST'] || 'localhost'
DATABASE_USERS_AND_PASSWORDS = [
  [ENV['DATABASE_USER'] || 'hr', ENV['DATABASE_PASSWORD'] || 'hr'],
  [ENV['DATABASE_USER2'] || 'arunit', ENV['DATABASE_PASSWORD2'] || 'arunit']
]


def get_connection(params = {})
  params.reverse_merge!(:user_number => 0, :dialect => :oracle)
  database_user, database_password = DATABASE_USERS_AND_PASSWORDS[params[:user_number]]
  unless defined?(JRuby)
    case params[:dialect]
    when :oracle
      connection_args = "//#{DATABASE_HOST}:#{ORA_DATABASE_PORT}/#{ORA_DATABASE_NAME}"
      begin
        OCI8.new(database_user, database_password, connection_args)
        # if connection fails then sleep 5 seconds and retry
      rescue OCIError
        sleep 5
        OCI8.new(database_user, database_password, connection_args)
      end
    when :postgres
      connection_args = {:user => database_user, :password => database_password,
        :host => DATABASE_HOST, :port => PG_DATABASE_PORT, :dbname => PG_DATABASE_NAME}
      begin
        PGconn.open(connection_args)
        # if connection fails then sleep 5 seconds and retry
      rescue PGError
        sleep 5
        PGconn.open(connection_args)
      end
    end
  else
    connection_args = case params[:dialect]
    when :oracle
      "jdbc:oracle:thin:@#{DATABASE_HOST}:#{ORA_DATABASE_PORT}:#{ORA_DATABASE_NAME}"
    when :postgres
      "jdbc:postgresql://#{DATABASE_HOST}:#{PG_DATABASE_PORT}/#{PG_DATABASE_NAME}"
    end
    connection_args = 
      begin
      java.sql.DriverManager.getConnection(connection_args, database_user, database_password)
      # if connection fails then sleep 5 seconds and retry
    rescue NativeException
      sleep 5
      java.sql.DriverManager.getConnection(connection_args, database_user, database_password)
    end
  end
end

ORA_CONNECTION_PARAMS = {
  :adapter => "oracle_enhanced",
  :database => ORA_DATABASE_NAME,
  :host => DATABASE_HOST,
  :port => ORA_DATABASE_PORT,
  :username => DATABASE_USERS_AND_PASSWORDS[0][0],
  :password => DATABASE_USERS_AND_PASSWORDS[0][1]
}

PG_CONNECTION_PARAMS = {
  :database => PG_DATABASE_NAME,
  :host => DATABASE_HOST,
  :port => PG_DATABASE_PORT,
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