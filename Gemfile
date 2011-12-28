source 'http://rubygems.org'

group :development do
  gem 'jeweler', '~> 1.5.1'
  gem 'rspec', '~> 1.3.0'

  unless ENV['NO_ACTIVERECORD']
    gem 'activerecord'
    gem 'activerecord-oracle_enhanced-adapter'
  end

  if !defined?(RUBY_ENGINE) || RUBY_ENGINE == 'ruby'
    gem 'ruby-oci8', '~> 2.1.0'
  end
end
