begin
  require 'spec'
rescue LoadError
  require 'rubygems'
  gem 'rspec'
  require 'spec'
end

require File.expand_path(File.dirname(__FILE__) + "/../lib/ruby_plsql")
