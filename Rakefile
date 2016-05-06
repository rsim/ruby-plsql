require 'rubygems'
require 'bundler'
begin
  Bundler.setup(:default, :development)
rescue Bundler::BundlerError => e
  $stderr.puts e.message
  $stderr.puts "Run `bundle install` to install missing gems"
  exit e.status_code
end

require 'rake'

begin
  require 'juwelier'
  Juwelier::Tasks.new do |gem|
    gem.name = "ruby-plsql"
    gem.summary = "Ruby API for calling Oracle PL/SQL procedures."
    gem.description = <<-EOS
  ruby-plsql gem provides simple Ruby API for calling Oracle PL/SQL procedures.
  It could be used both for accessing Oracle PL/SQL API procedures in legacy applications
  as well as it could be used to create PL/SQL unit tests using Ruby testing libraries.
  EOS
    gem.email = "raimonds.simanovskis@gmail.com"
    gem.homepage = "http://github.com/rsim/ruby-plsql"
    gem.authors = ["Raimonds Simanovskis"]
    gem.extra_rdoc_files = ['README.md']
  end
  Juwelier::RubygemsDotOrgTasks.new
rescue LoadError
  # juwelier not installed
end

require 'rspec/core/rake_task'
RSpec::Core::RakeTask.new(:spec)

desc "Code coverage detail"
task :simplecov do
  ENV['COVERAGE'] = "true"
  Rake::Task['spec'].execute
end

task :default => :spec

require 'rdoc/task'
Rake::RDocTask.new do |rdoc|
  version = File.exist?('VERSION') ? File.read('VERSION') : ""

  rdoc.rdoc_dir = 'doc'
  rdoc.title = "ruby-plsql #{version}"
  rdoc.rdoc_files.include('README*')
  rdoc.rdoc_files.include('lib/**/*.rb')
end
