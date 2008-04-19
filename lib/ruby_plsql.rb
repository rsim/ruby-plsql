$:.unshift File.dirname(__FILE__)

module RubyPlsql #:nodoc:
end

begin
  require "oci8"

  %w(schema procedure package).each do |file|
    require File.dirname(__FILE__) + "/plsql/#{file}"
  end
rescue LoadError
    puts <<-EOS
  To use ruby_plsql you must install ruby-oci8 library.
    EOS
end
