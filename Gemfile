source "http://rubygems.org"

git_source(:github) { |repo| "https://github.com/#{repo}.git" }

group :development do
  gem "juwelier", "~> 2.0"
  gem "rspec_junit_formatter"
  gem "rubocop", "0.81", require: false
  gem "rubocop-performance", require: false
  gem "rubocop-rails", require: false
end

group :test, :development do
  gem "rake", ">= 10.0"
  gem "rspec", "~> 3.1"

  unless ENV["NO_ACTIVERECORD"]
    gem "activerecord", "~> 5.0"
    gem "activerecord-oracle_enhanced-adapter", "~> 1.7"
    gem "simplecov", ">= 0"
  end

  platforms :ruby, :mswin, :mingw do
    if RUBY_VERSION >= "3.2"
      gem "ruby-oci8", github: "kubo/ruby-oci8", branch: "master"
    else
      gem "ruby-oci8", "~> 2.1"
    end
  end
end
