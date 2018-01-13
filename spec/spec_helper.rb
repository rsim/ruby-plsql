require "rubygems"
require "bundler"
Bundler.setup(:default, :development)
require "simplecov"

SimpleCov.configure do
  load_profile "root_filter"
  load_profile "test_frameworks"
end

ENV["COVERAGE"] && SimpleCov.start do
  add_filter "/.rvm/"
end

$LOAD_PATH.unshift(File.join(File.dirname(__FILE__), "..", "lib"))
require "rspec"

unless ENV["NO_ACTIVERECORD"]
  require "active_record"
else
  puts "Without ActiveRecord"
end

require "ruby-plsql"

# Requires supporting ruby files with custom matchers and macros, etc,
# in spec/support/ and its subdirectories.
Dir[File.join(File.dirname(__FILE__), "support/**/*.rb")].each { |f| require f }

if ENV["USE_VM_DATABASE"] == "Y"
  DATABASE_NAME = "XE"
else
  DATABASE_NAME = ENV["DATABASE_NAME"] || "orcl"
end

DATABASE_SERVICE_NAME = (defined?(JRUBY_VERSION) ? "/" : "") +
                        (ENV["DATABASE_SERVICE_NAME"] || DATABASE_NAME)
DATABASE_HOST = ENV["DATABASE_HOST"] || "localhost"
DATABASE_PORT = (ENV["DATABASE_PORT"] || 1521).to_i
DATABASE_USERS_AND_PASSWORDS = [
  [ENV["DATABASE_USER"] || "hr", ENV["DATABASE_PASSWORD"] || "hr"],
  [ENV["DATABASE_USER2"] || "arunit", ENV["DATABASE_PASSWORD2"] || "arunit"]
]
# specify which database version is used (will be verified in one test)
DATABASE_VERSION = ENV["DATABASE_VERSION"] || "10.2.0.4"

if ENV["USE_VM_DATABASE"] == "Y"
  RSpec.configure do |config|
    config.before(:suite) do
      TestDb.build

      # Set Verbose off to hide warning: already initialized constant DATABASE_VERSION
      original_verbosity = $VERBOSE
      $VERBOSE           = nil
      DATABASE_VERSION   = TestDb.database_version
      $VERBOSE           = original_verbosity
    end
  end
end

def oracle_error_class
  unless defined?(JRUBY_VERSION)
    OCIError
  else
    java.sql.SQLException
  end
end

def get_eazy_connect_url(svc_separator = "")
  "#{DATABASE_HOST}:#{DATABASE_PORT}#{svc_separator}#{DATABASE_SERVICE_NAME}"
end

def get_connection_url
  unless defined?(JRUBY_VERSION)
    (ENV["DATABASE_USE_TNS"] == "NO") ? get_eazy_connect_url("/") : DATABASE_NAME
  else
    "jdbc:oracle:thin:@#{get_eazy_connect_url}"
  end
end

def get_connection(user_number = 0)
  database_user, database_password = DATABASE_USERS_AND_PASSWORDS[user_number]
  unless defined?(JRUBY_VERSION)
    try_to_connect(OCIError) do
      OCI8.new(database_user, database_password, get_connection_url)
    end
  else
    try_to_connect(NativeException) do
      java.sql.DriverManager.getConnection(get_connection_url, database_user, database_password)
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
  adapter: "oracle_enhanced",
  database: DATABASE_SERVICE_NAME,
  host: DATABASE_HOST,
  port: DATABASE_PORT,
  username: DATABASE_USERS_AND_PASSWORDS[0][0],
  password: DATABASE_USERS_AND_PASSWORDS[0][1]
}

class Hash
  def except(*blacklist)
    self.reject { |key, value| blacklist.include?(key) }
  end unless method_defined?(:except)

  def only(*whitelist)
    self.reject { |key, value| !whitelist.include?(key) }
  end unless method_defined?(:only)
end
