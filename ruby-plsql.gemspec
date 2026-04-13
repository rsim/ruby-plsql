# -*- encoding: utf-8 -*-

Gem::Specification.new do |s|
  s.name = "ruby-plsql"
  s.version = File.read(File.expand_path("VERSION", __dir__)).chomp

  s.authors = ["Raimonds Simanovskis"]
  s.email = "raimonds.simanovskis@gmail.com"
  s.summary = "Ruby API for calling Oracle PL/SQL procedures."
  s.description = <<~DESC.strip
    ruby-plsql gem provides simple Ruby API for calling Oracle PL/SQL procedures.
    It could be used both for accessing Oracle PL/SQL API procedures in legacy applications
    as well as it could be used to create PL/SQL unit tests using Ruby testing libraries.
  DESC
  s.homepage = "https://github.com/rsim/ruby-plsql"
  s.license = "MIT"

  s.require_paths = ["lib"]
  s.files = Dir["lib/**/*.rb", "VERSION", "License.txt", "README.md", "History.txt"]
  s.extra_rdoc_files = ["README.md"]

  s.add_development_dependency "rake", ">= 10.0"
  s.add_development_dependency "rspec", "~> 3.1"
  s.add_development_dependency "rspec_junit_formatter"
  s.add_development_dependency "simplecov"
  if RUBY_PLATFORM =~ /java/
    s.platform = Gem::Platform.new("java")
  else
    s.add_runtime_dependency "ruby-oci8", "~> 2.1"
  end
end
