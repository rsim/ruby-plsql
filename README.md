[![Build Status](https://travis-ci.org/rsim/ruby-plsql.svg?branch=master)](https://travis-ci.org/rsim/ruby-plsql)

ruby-plsql
==========

Ruby API for calling Oracle PL/SQL procedures.

DESCRIPTION
-----------

ruby-plsql gem provides simple Ruby API for calling Oracle PL/SQL procedures. It could be used both for accessing Oracle PL/SQL API procedures in legacy applications as well as it could be used to create PL/SQL unit tests using Ruby testing libraries.

NUMBER, BINARY_INTEGER, PLS_INTEGER, NATURAL, NATURALN, POSITIVE, POSITIVEN, SIGNTYPE, SIMPLE_INTEGER, VARCHAR, VARCHAR2, NVARCHAR2, CHAR, NCHAR, DATE, TIMESTAMP, CLOB, BLOB, BOOLEAN, PL/SQL RECORD, TABLE, VARRAY, OBJECT and CURSOR types are supported for input and output parameters and return values of PL/SQL procedures and functions.

ruby-plsql supports Ruby 1.8.7, 1.9.3, 2.1, 2.2, 2.3, 2.4 (ruby-oci8 2.2.3+ is needed for Ruby 2.4) and JRuby 1.6.7, 1.7.16, 9.0, 9.1 implementations.

USAGE
-----

### Calling PL/SQL functions and procedures:

```ruby
require "rubygems"
require "ruby-plsql"

plsql.connection = OCI8.new("hr","hr","xe")

plsql.test_uppercase('xxx')               # => "XXX"
plsql.test_uppercase(:p_string => 'xxx')  # => "XXX"
plsql.test_copy("abc", nil, nil)          # => { :p_to => "abc", :p_to_double => "abcabc" }
plsql.test_copy(:p_from => "abc", :p_to => nil, :p_to_double => nil)
                                          # => { :p_to => "abc", :p_to_double => "abcabc" }
plsql.hr.test_uppercase('xxx')            # => "XXX"
plsql.test_package.test_uppercase('xxx')  # => 'XXX'

# PL/SQL records or object type parameters should be passed as Hash
p_employee = { :employee_id => 1, :first_name => 'First', :last_name => 'Last', :hire_date => Time.local(2000,01,31) }
plsql.test_full_name(p_employee)

# TABLE or VARRAY parameters should be passed as Array
plsql.test_sum([1,2,3,4])

# Nested objects or arrays are also supported
p_employee = { :employee_id => 1, :first_name => 'First', :last_name => 'Last', :hire_date => Time.local(2000,01,31),
  :address => {:street => 'Street', :city => 'City', :country => 'Country'},
  :phones => [{:type => 'mobile', :phone_number => '123456'}, {:type => 'fixed', :phone_number => '654321'}]}
plsql.test_store_employee(p_employee)

# Returned cursor can be fetched
plsql.test_cursor do |cursor|
  cursor.fetch                            # => one row from cursor
  cursor.fetch_all                        # => all rows from cursor
end

plsql.connection.autocommit = false
plsql.commit
plsql.rollback

plsql.logoff
```

Look at RSpec tests under spec directory for more usage examples.

Note: named arguments in procedures calls should be in lower case.

### Table operations:

ruby-plsql also provides simple API for select/insert/update/delete table operations (with Sequel-like syntax). This could be useful if ruby-plsql is used without ActiveRecord (e.g. for writing PL/SQL unit tests):

```ruby
# insert record in table
employee = { :employee_id => 1, :first_name => 'First', :last_name => 'Last', :hire_date => Time.local(2000,01,31) }
plsql.employees.insert employee           # INSERT INTO employees VALUES (1, 'First', 'Last', ...)

# insert many records
employees = [employee1, employee2, ... ]  # array of many Hashes
plsql.employees.insert employees

# insert many records as list of values
plsql.employees.insert_values [:employee_id, :first_name, :last_name],
  [1, 'First 1', 'Last 1'],
  [2, 'First 2', 'Last 2']

# select one record
plsql.employees.first                     # SELECT * FROM employees
                                          # fetch first row => {:employee_id => ..., :first_name => '...', ...}
plsql.employees.first(:employee_id => 1)  # SELECT * FROM employees WHERE employee_id = 1
plsql.employees.first("WHERE employee_id = 1")
plsql.employees.first("WHERE employee_id = :employee_id", 1)

# select many records
plsql.employees.all                       # => [{...}, {...}, ...]
plsql.employees.all(:order_by => :employee_id)
plsql.employees.all("WHERE employee_id > :employee_id", 5)

# count records
plsql.employees.count                     # SELECT COUNT(*) FROM employees
plsql.employees.count("WHERE employee_id > :employee_id", 5)

# update records
plsql.employees.update(:first_name => 'Second', :where => {:employee_id => 1})
                                          # UPDATE employees SET first_name = 'Second' WHERE employee_id = 1

# delete records
plsql.employees.delete(:employee_id => 1) # DELETE FROM employees WHERE employee_id = 1

# select from sequences
plsql.employees_seq.nextval               # SELECT employees_seq.NEXTVAL FROM dual
plsql.employees_seq.currval               # SELECT employees_seq.CURRVAL FROM dual
```

### Usage with Rails:

If using with Rails then include in initializer file:

```ruby
plsql.activerecord_class = ActiveRecord::Base
```

and then you do not need to specify plsql.connection (this is also safer when ActiveRecord reestablishes connection to database).


### Cheat Sheet:

You may have a look at this [Cheat Sheet](http://cheatography.com/jgebal/cheat-sheets/ruby-plsql-cheat-sheet/) for instructions on how to use ruby-plsql

INSTALLATION
------------

Install as gem with

    gem install ruby-plsql

or include gem in Gemfile if using bundler.

In addition install either ruby-oci8 (for MRI/YARV) or copy Oracle JDBC driver to $JRUBY_HOME/lib (for JRuby).

If you are using MRI 1.8, 1.9 or 2.x Ruby implementation then you need to install ruby-oci8 gem (version 2.0.x or 2.1.x)
as well as Oracle client, e.g. [Oracle Instant Client](http://www.oracle.com/technetwork/database/features/instant-client/index-097480.html).

If you are using JRuby then you need to download latest [Oracle JDBC driver](http://www.oracle.com/technetwork/database/enterprise-edition/jdbc-112010-090769.html) - either ojdbc7.jar for Java 8 and 7, ojdbc6.jar for Java 6, 7, 8 or ojdbc5.jar for Java 5. You can refer [the support matrix](http://www.oracle.com/technetwork/database/enterprise-edition/jdbc-faq-090281.html#01_03) for details.

And copy this file to one of these locations. JDBC driver will be searched in this order:

* in `JRUBY_HOME/lib` directory
* in `./lib` directory of Rails application
* or include path to JDBC driver jar file in Java `CLASSPATH`
* in some directory which is in `PATH`

If you put multiple versions of JDBC driver in the same directory the higher version one will be used.

Make sure to setup the following Oracle-specific environment variables properly

* [NLS_LANG](http://www.orafaq.com/wiki/NLS_LANG) - preferred value `NLS_LANG=AMERICAN_AMERICA.AL32UTF8`
* [ORA_SDTZ](http://docs.oracle.com/cd/E18283_01/server.112/e10729/ch4datetime.htm#CBBEEAFB) The setting should point a machine timezone like: `ORA_SDTZ=Europe/Riga`, otherwise Oracle by default uses a Fixed-offset timezone (like `03:00`) that is not daylight saving (DST) aware, which will lead to wrong translations of the timestamp values between Ruby code (DTS-aware) and Oracle session (non-DST-aware).
* [ORACLE_HOME](http://www.orafaq.com/wiki/ORACLE_HOME)

You may either alter your environment settings or set the values in file `spec/support/custom_config.rb`. Sample file `custom_config.rb.sample` shows how to do that.


Make sure you use correct version of Oracle client for database you're connecting to. Otherwise you may encounter TimeZone errors like [this](http://stackoverflow.com/questions/7678485/oracle-ora-01805-on-oracle-11g-database)


TESTS
-----

Review `spec/spec_helper.rb` to see default schema/user names and database names (use environment variables to override defaults)

##### Prepare database

* With local [Vagrant](https://www.vagrantup.com) based Oracle XE database.

    Download Oracle XE database ```oracle-xe-11.2.0-1.0.x86_64.rpm.zip``` from [Oracle Home page](http://www.oracle.com/technetwork/database/database-technologies/express-edition/downloads/index.html) and put it into project home directory.

    From project home directory run ```vagrant up``` command to build fully functioning **Centos 6.6** virtual machine with installed Oracle XE database.

* Within other Oracle Database create Oracle database schema for test purposes.

        SQL> CREATE USER hr IDENTIFIED BY hr;
        SQL> GRANT unlimited tablespace, create session, create table, create sequence, create procedure, create type, create view, create synonym TO hr;

        SQL> CREATE USER arunit IDENTIFIED BY arunit;
        SQL> GRANT create session TO arunit;

##### Prepare dependencies

* Install bundler with

        gem install bundler

* Install necessary gems with

        bundle install

##### Run tests

* Run tests with local Vagrant based Oracle XE database

        USE_VM_DATABASE=Y rake spec

* Run tests with other Oracle database

        rake spec

LINKS
-----

* Source code: http://github.com/rsim/ruby-plsql
* Bug reports / Feature requests: http://github.com/rsim/ruby-plsql/issues
* Discuss at oracle_enhanced adapter group: http://groups.google.com/group/oracle-enhanced

CONTRIBUTORS
------------

* Raimonds Simanovskis
* Edgars Beigarts
* Oleh Mykytyuk
* Wiehann Matthysen
* Dayle Larson
* Yasuo Honda
* Yavor Nikolov

LICENSE
-------

(The MIT License)

Copyright (c) 2008-2014 Raimonds Simanovskis

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
