source 'http://rubygems.org'

group :development do
  gem 'jeweler', '~> 2.0.1'
  gem 'rspec', '~> 3.1'

  unless ENV['NO_ACTIVERECORD']
    gem 'activerecord', '>= 3.2.3', '< 4.3.0'
    gem 'activerecord-oracle_enhanced-adapter', '>= 1.4.1', '< 1.6.0'
    gem 'simplecov', '>= 0'
  end

  platforms :ruby do
    gem 'ruby-oci8', '~> 2.1.2'
  end
end
