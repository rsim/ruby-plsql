= ruby-plsql

* http://rubyforge.org/projects/ruby-plsql/

== DESCRIPTION:

ruby-plsql gem provides simple Ruby API for calling Oracle PL/SQL procedures.
ruby-plsql support both Ruby 1.8 MRI, Ruby 1.9.1 YARV and JRuby runtime environments.
This gem requires ruby-oci8 library version 1.x (if MRI is used) or 2.x (if Ruby 1.9.1 is used) or Oracle JDBC driver (ojdbc14.jar) (if JRuby is used) for connection to Oracle database.

See http://blog.rayapps.com for more information.

Look ar RSpec tests under spec directory for usage examples.

== FEATURES/PROBLEMS:

* Currently just NUMBER, VARCHAR2, DATE, TIMESTAMP, CLOB, BLOB argument types are supported for PL/SQL procedures

== SYNOPSIS:

Usage examples:

require "ruby_plsql"

plsql.connection = OCI8.new("hr","hr","xe")

plsql.test_uppercase('xxx')              # => "XXX"
plsql.test_uppercase(:p_string => 'xxx') # => "XXX"
plsql.test_copy("abc", nil, nil)         # => { :p_to => "abc", :p_to_double => "abcabc" }
plsql.test_copy(:p_from => "abc", :p_to => nil, :p_to_double => nil)
                                         # => { :p_to => "abc", :p_to_double => "abcabc" }
plsql.hr.test_uppercase('xxx')           # => "XXX"
plsql.test_package.test_uppercase('xxx') # => 'XXX'

plsql.logoff


If using with Rails then include in initializer file:

plsql.activerecord_class = ActiveRecord::Base

and then you do not need to specify plsql.connection (this is also safer when ActiveRecord reestablishes connection to database).

== REQUIREMENTS:

Ruby 1.8 MRI
* Requires ruby-oci8 library to connect to Oracle (please use version 1.0.3 or later)
Ruby 1.9.1
* Requires ruby-oci8 library to connect to Oracle (please use version 2.0 or later)
JRuby
* Requires Oracle JDBC driver (ojdbc14.jar should be somewhere in PATH) to connect to Oracle

== INSTALL:

* sudo gem install ruby-plsql

== LICENSE:

(The MIT License)

Copyright (c) 2009 Raimonds Simanovskis

Permission is hereby granted, free of charge, to any person obtaining
a copy of this software and associated documentation files (the
'Software'), to deal in the Software without restriction, including
without limitation the rights to use, copy, modify, merge, publish,
distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so, subject to
the following conditions:

The above copyright notice and this permission notice shall be
included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED 'AS IS', WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.