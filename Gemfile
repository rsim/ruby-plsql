source 'http://rubygems.org'

gem 'jeweler'
gem 'rspec', "~> 1.3.0"

unless ENV['NO_ACTIVERECORD']
  # avoid loading activerecord 3.0 beta
  gem 'activerecord', '=2.3.8'
  gem 'activerecord-oracle_enhanced-adapter', '=1.3.1'
end

if !defined?(RUBY_ENGINE) || RUBY_ENGINE == 'ruby'
  gem 'ruby-oci8', '>=2.0.4'
end
