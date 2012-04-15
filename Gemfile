source 'http://rubygems.org'

group :development do
  gem 'jeweler', '~> 1.8.3'
  gem 'rspec', '~> 2.9'

  unless ENV['NO_ACTIVERECORD']
    gem 'activerecord', '~> 3.2.3'
    gem 'activerecord-oracle_enhanced-adapter', '~> 1.4.1'
  end

  if !defined?(RUBY_ENGINE) || RUBY_ENGINE == 'ruby'
    gem 'ruby-oci8', '~> 2.1.0'
  end
end
