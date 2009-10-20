require 'rubygems'
gem 'hoe', '>= 2.3.0'
require 'hoe'
require 'fileutils'

Hoe.plugin :newgem
Hoe.plugin :website

require File.dirname(__FILE__) + '/lib/plsql/version'

# do not generate dot graphics for RDoc
ENV['NODOT'] = 'true'

# Generate all the Rake tasks
# Run 'rake -T' to see list of generated tasks (from gem root directory)
$hoe = Hoe.spec('ruby-plsql') do
  developer 'Raimonds Simanovskis', 'raimonds.simanovskis@gmail.com'
  self.version           = PLSQL::VERSION
  self.changes           = paragraphs_of("History.txt", 0..1).join("\n\n")
  self.rubyforge_name    = name
  self.summary           = "ruby-plsql gem provides simple Ruby API for calling Oracle PL/SQL procedures."
  self.extra_rdoc_files  = ['README.rdoc']
  self.clean_globs |= %w[**/.DS_Store tmp *.log]
end

require 'newgem/tasks' # load /tasks/*.rake
Dir['tasks/**/*.rake'].each { |t| load t }

# want other tests/tasks run by default? Add them to the list
remove_task :default
task :default => [:spec]
