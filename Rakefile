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

require 'jeweler'
Jeweler::Tasks.new do |gem|
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
  gem.add_development_dependency "rspec", "~> 1.3.0"
  gem.extra_rdoc_files = ['README.rdoc']
end
Jeweler::RubygemsDotOrgTasks.new

require 'spec/rake/spectask'
Spec::Rake::SpecTask.new(:spec) do |spec|
  spec.libs << 'lib' << 'spec'
  spec.spec_files = FileList['spec/**/*_spec.rb']
end

Spec::Rake::SpecTask.new(:rcov) do |spec|
  spec.libs << 'lib' << 'spec'
  spec.pattern = 'spec/**/*_spec.rb'
  spec.rcov = true
  spec.rcov_opts = ['--exclude', '/Library,spec/']
end

task :default => :spec

require 'rake/rdoctask'
Rake::RDocTask.new do |rdoc|
  version = File.exist?('VERSION') ? File.read('VERSION') : ""

  rdoc.rdoc_dir = 'doc'
  rdoc.title = "ruby-plsql #{version}"
  rdoc.rdoc_files.include('README*')
  rdoc.rdoc_files.include('lib/**/*.rb')
end
