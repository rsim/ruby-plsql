# encoding: utf-8

require File.dirname(__FILE__) + '/../spec_helper'

describe "Connection" do

  before(:all) do
    @raw_conn = get_connection
  end

  after(:all) do
    unless defined?(JRUBY_VERSION)
      @raw_conn.logoff rescue nil
    else
      @raw_conn.close rescue nil
    end
  end

  before(:each) do
    @conn = PLSQL::Connection.create( @raw_conn )
  end

  it "should create connection" do
    @conn.raw_connection.should == @raw_conn
  end

  unless defined?(JRUBY_VERSION)
    it "should be oci connection" do
      @conn.should be_oci
      @conn.raw_driver.should == :oci
    end
  else
    it "should be jdbc connection" do
      @conn.should be_jdbc
      @conn.raw_driver.should == :jdbc
    end
  end
  
  it "should logoff connection" do
    @conn.logoff.should be_true
  end
  
  # Ruby 1.8 and 1.9
  if !defined?(RUBY_ENGINE) || RUBY_ENGINE == 'ruby'
    describe "OCI data type conversions" do
      it "should translate PL/SQL VARCHAR2 to Ruby String" do
        @conn.plsql_to_ruby_data_type(:data_type => "VARCHAR2", :data_length => 100).should == [String, 100]
        @conn.plsql_to_ruby_data_type(:data_type => "VARCHAR2", :data_length => nil).should == [String, 32767]
      end

      it "should translate PL/SQL CLOB to Ruby String" do
        @conn.plsql_to_ruby_data_type(:data_type => "CLOB", :data_length => 100_000).should == [OCI8::CLOB, nil]
        @conn.plsql_to_ruby_data_type(:data_type => "CLOB", :data_length => nil).should == [OCI8::CLOB, nil]
      end

      it "should translate PL/SQL NUMBER to Ruby OraNumber" do
        @conn.plsql_to_ruby_data_type(:data_type => "NUMBER", :data_length => 15).should == [OraNumber, nil]
      end

      it "should translate PL/SQL DATE to Ruby DateTime" do
        @conn.plsql_to_ruby_data_type(:data_type => "DATE", :data_length => nil).should == [DateTime, nil]
      end

      it "should translate PL/SQL TIMESTAMP to Ruby Time" do
        @conn.plsql_to_ruby_data_type(:data_type => "TIMESTAMP", :data_length => nil).should == [Time, nil]
      end

      it "should not translate Ruby Fixnum when OraNumber type specified" do
        @conn.ruby_value_to_ora_value(100, OraNumber).should eql(100)
      end

      it "should translate Ruby Bignum value to OraNumber when OraNumber type specified" do
        ora_number = @conn.ruby_value_to_ora_value(12345678901234567890, OraNumber)
        ora_number.class.should == OraNumber
        ora_number.to_s.should == "12345678901234567890"
        # OraNumber has more numeric comparison methods in ruby-oci8 2.0
        ora_number.should == OraNumber.new("12345678901234567890") if OCI8::VERSION >= '2.0.0'
      end

      it "should translate Ruby String value to OCI8::CLOB when OCI8::CLOB type specified" do
        large_text = "x" * 100_000
        ora_value = @conn.ruby_value_to_ora_value(large_text, OCI8::CLOB)
        ora_value.class.should == OCI8::CLOB
        ora_value.size.should == 100_000
        ora_value.rewind
        ora_value.read.should == large_text
      end

      it "should translate Oracle OraNumber integer value to Fixnum" do
        @conn.ora_value_to_ruby_value(OraNumber.new(100)).should eql(100)
      end

      it "should translate Oracle OraNumber float value to BigDecimal" do
        @conn.ora_value_to_ruby_value(OraNumber.new(100.11)).should eql(BigDecimal("100.11"))
      end

      # ruby-oci8 2.0 returns DATE as Time or DateTime
      if OCI8::VERSION < '2.0.0'
        it "should translate Oracle OraDate value to Time" do
          now = OraDate.now
          @conn.ora_value_to_ruby_value(now).should eql(now.to_time)
        end
      end

      it "should translate Oracle CLOB value to String" do
        large_text = "x" * 100_000
        clob = OCI8::CLOB.new(@raw_conn, large_text)
        @conn.ora_value_to_ruby_value(clob).should == large_text
      end
      
    end

  elsif RUBY_ENGINE == 'jruby'
    
    describe "JDBC data type conversions" do
      it "should translate PL/SQL VARCHAR2 to Ruby String" do
        @conn.plsql_to_ruby_data_type(:data_type => "VARCHAR2", :data_length => 100).should == [String, 100]
        @conn.plsql_to_ruby_data_type(:data_type => "VARCHAR2", :data_length => nil).should == [String, 32767]
      end

      it "should translate PL/SQL NUMBER to Ruby BigDecimal" do
        @conn.plsql_to_ruby_data_type(:data_type => "NUMBER", :data_length => 15).should == [BigDecimal, nil]
      end
      
      it "should translate PL/SQL DATE to Ruby DateTime" do
        @conn.plsql_to_ruby_data_type(:data_type => "DATE", :data_length => nil).should == [DateTime, nil]
      end
      
      it "should translate PL/SQL TIMESTAMP to Ruby Time" do
        @conn.plsql_to_ruby_data_type(:data_type => "TIMESTAMP", :data_length => nil).should == [Time, nil]
      end
      
      it "should not translate Ruby Fixnum when BigDecimal type specified" do
        @conn.ruby_value_to_ora_value(100, BigDecimal).should == java.math.BigDecimal.new(100)
      end
      
      it "should translate Ruby Bignum value to BigDecimal when BigDecimal type specified" do
        big_decimal = @conn.ruby_value_to_ora_value(12345678901234567890, BigDecimal)
        big_decimal.should == java.math.BigDecimal.new("12345678901234567890")
      end
      
      # it "should translate Ruby OraDate value to DateTime when DateTime type specified" do
      #   now = OraDate.now
      #   @conn.ruby_value_to_ora_value(now, DateTime).should eql(now.to_datetime)
      # end
      
      it "should translate Ruby String value to Java::OracleSql::CLOB when Java::OracleSql::CLOB type specified" do
        large_text = "x" * 100_000
        ora_value = @conn.ruby_value_to_ora_value(large_text, Java::OracleSql::CLOB)
        ora_value.class.should == Java::OracleSql::CLOB
        ora_value.length.should == 100_000
        ora_value.getSubString(1, ora_value.length) == large_text
        ora_value.freeTemporary
      end

      it "should translate Ruby nil value to empty Java::OracleSql::CLOB when Java::OracleSql::CLOB type specified" do
        ora_value = @conn.ruby_value_to_ora_value(nil, Java::OracleSql::CLOB)
        ora_value.class.should == Java::OracleSql::CLOB
        ora_value.isEmptyLob.should be_true
      end

      it "should translate Oracle BigDecimal integer value to Fixnum" do
        @conn.ora_value_to_ruby_value(BigDecimal("100")).should eql(100)
      end
      
      it "should translate Oracle BigDecimal float value to BigDecimal" do
        @conn.ora_value_to_ruby_value(BigDecimal("100.11")).should eql(BigDecimal("100.11"))
      end
      
      # it "should translate Oracle OraDate value to Time" do
      #   now = OraDate.now
      #   @conn.ora_value_to_ruby_value(now).should eql(now.to_time)
      # end

      it "should translate Oracle CLOB value to String" do
        large_text = "āčē" * 100_000
        clob = @conn.ruby_value_to_ora_value(large_text, Java::OracleSql::CLOB)
        @conn.ora_value_to_ruby_value(clob).should == large_text
        clob.freeTemporary
      end

      it "should translate empty Oracle CLOB value to nil" do
        clob = @conn.ruby_value_to_ora_value(nil, Java::OracleSql::CLOB)
        @conn.ora_value_to_ruby_value(clob).should be_nil
      end

    end
    
  end  

  describe "SQL SELECT statements" do

    it "should execute SQL statement and return first result" do
      @now = Time.local(2008,05,31,23,22,11)
      @conn.select_first("SELECT 'abc',123,123.456,
        TO_DATE('#{@now.strftime("%Y-%m-%d %H:%M:%S")}','YYYY-MM-DD HH24:MI:SS')
        FROM dual").should == ["abc",123,123.456,@now]
    end

    it "should execute SQL statement and return first result as hash" do
      @now = Time.local(2008,05,31,23,22,11)
      @conn.select_hash_first("SELECT 'abc' a, 123 b, 123.456 c,
        TO_DATE('#{@now.strftime("%Y-%m-%d %H:%M:%S")}', 'YYYY-MM-DD HH24:MI:SS') d
        FROM dual").should == {:a => "abc", :b => 123, :c => 123.456, :d => @now}
    end

    it "should execute SQL statement with bind parameters and return first result" do
      @today = Date.parse("2008-05-31")
      @now = Time.local(2008,05,31,23,22,11)
      @conn.select_first("SELECT :1,:2,:3,:4,:5 FROM dual",
        'abc',123,123.456,@now,@today).should == ["abc",123,123.456,@now,Time.parse(@today.to_s)]
    end
    
    it "should execute SQL statement with NULL values and return first result" do
      @now = Time.local(2008,05,31,23,22,11)
      @conn.select_first("SELECT NULL,123,123.456,
        TO_DATE('#{@now.strftime("%Y-%m-%d %H:%M:%S")}','YYYY-MM-DD HH24:MI:SS')
        FROM dual").should == [nil,123,123.456,@now]
    end

    if defined?(JRUBY_VERSION)

      it "should execute SQL statement with NULL values as bind parameters and return first result" do
        @today = Date.parse("2008-05-31")
        @now = Time.local(2008,05,31,23,22,11)
        @conn.select_first("SELECT :1,:2,:3,:4,:5 FROM dual",
          nil,123,123.456,@now,@today).should == [nil,123,123.456,@now,Time.parse(@today.to_s)]
      end
    
    end

    it "should execute SQL statement and return all results" do
      @now = Time.local(2008,05,31,23,22,11)
      @conn.select_all("SELECT 'abc',123,123.456,
          TO_DATE('#{@now.strftime("%Y-%m-%d %H:%M:%S")}','YYYY-MM-DD HH24:MI:SS')
          FROM dual
          UNION ALL SELECT 'abc',123,123.456,
          TO_DATE('#{@now.strftime("%Y-%m-%d %H:%M:%S")}','YYYY-MM-DD HH24:MI:SS')
          FROM dual").should == [["abc",123,123.456,@now],["abc",123,123.456,@now]]
    end

    it "should execute SQL statement and return all results as hash" do
      @now = Time.local(2008,05,31,23,22,11)
      @conn.select_hash_all("SELECT 'abc' a, 123 b, 123.456 c,
          TO_DATE('#{@now.strftime("%Y-%m-%d %H:%M:%S")}','YYYY-MM-DD HH24:MI:SS') d
          FROM dual
          UNION ALL SELECT 'def' a, 123 b, 123.456 c,
          TO_DATE('#{@now.strftime("%Y-%m-%d %H:%M:%S")}','YYYY-MM-DD HH24:MI:SS') d
          FROM dual").should == [{:a=>"abc",:b=>123,:c=>123.456,:d=>@now},{:a=>"def",:b=>123,:c=>123.456,:d=>@now}]
    end

    it "should execute SQL statement with bind parameters and return all results" do
      @now = Time.local(2008,05,31,23,22,11)
      @conn.select_all("SELECT :1,:2,:3,:4 FROM dual UNION ALL SELECT :1,:2,:3,:4 FROM dual",
        'abc',123,123.456,@now,'abc',123,123.456,@now).should == [["abc",123,123.456,@now],["abc",123,123.456,@now]]
    end
    
    it "should execute SQL statement and yield all results in block" do
      @now = Time.local(2008,05,31,23,22,11)
      @conn.select_all("SELECT 'abc',123,123.456,
          TO_DATE('#{@now.strftime("%Y-%m-%d %H:%M:%S")}','YYYY-MM-DD HH24:MI:SS')
          FROM dual
          UNION ALL SELECT 'abc',123,123.456,
          TO_DATE('#{@now.strftime("%Y-%m-%d %H:%M:%S")}','YYYY-MM-DD HH24:MI:SS')
          FROM dual") do |r|
        r.should == ["abc",123,123.456,@now]
      end.should == 2
    end
    
    it "should execute SQL statement with bind parameters and yield all results in block" do
      @now = Time.local(2008,05,31,23,22,11)
      @conn.select_all("SELECT :1,:2,:3,:4 FROM dual UNION ALL SELECT :1,:2,:3,:4 FROM dual",
        'abc',123,123.456,@now,'abc',123,123.456,@now) do |r|
        r.should == ["abc",123,123.456,@now]
      end.should == 2
    end

  end
  
  describe "PL/SQL procedures" do
    before(:each) do
      @random = rand(1000)
      @now = Time.local(2008,05,31,23,22,11)
      sql = <<-EOS
        CREATE OR REPLACE FUNCTION test_add_random (p_number NUMBER, p_varchar IN OUT VARCHAR2, p_date IN OUT DATE)
          RETURN NUMBER
        IS
        BEGIN
          RETURN p_number + #{@random};
        END test_add_random;
      EOS
      @conn.exec(sql).should be_true
    end

    after(:each) do
      @conn.exec "DROP FUNCTION test_add_random"
    end

    it "should parse PL/SQL procedure call and bind parameters and exec and get bind parameter value" do
      sql = <<-EOS
        BEGIN
          :result := test_add_random (:p_number, :p_varchar, :p_date);
        END;
      EOS
      cursor = @conn.parse(sql)
      cursor.bind_param(":result", nil, :data_type => 'NUMBER', :in_out => 'OUT')
      cursor.bind_param(":p_number", 100, :data_type => 'NUMBER', :in_out => 'IN')
      cursor.bind_param(":p_varchar", "abc", :data_type => 'VARCHAR2', :in_out => 'IN/OUT')
      cursor.bind_param(":p_date", @now, :data_type => 'DATE', :in_out => 'IN/OUT')
      cursor.exec
      cursor[":result"].should == @random + 100
      cursor[":p_varchar"].should == "abc"
      cursor[":p_date"].should == @now
      cursor.close.should be_nil
    end
  
  end
  
  describe "commit and rollback" do
    before(:each) do
      sql = "CREATE TABLE test_commit (dummy VARCHAR2(100))"
      @conn.exec(sql).should be_true
      @conn.autocommit = false
      @conn.should_not be_autocommit
    end
    after(:each) do
      sql = "DROP TABLE test_commit"
      @conn.exec(sql).should be_true      
    end
    
    it "should do commit" do
      @conn.exec("INSERT INTO test_commit VALUES ('test')")
      @conn.commit
      @conn.select_first("SELECT COUNT(*) FROM test_commit")[0].should == 1
    end

    it "should do rollback" do
      @conn.exec("INSERT INTO test_commit VALUES ('test')")
      @conn.rollback
      @conn.select_first("SELECT COUNT(*) FROM test_commit")[0].should == 0
    end

    it "should do commit and rollback should not undo commited transaction" do
      @conn.exec("INSERT INTO test_commit VALUES ('test')")
      @conn.commit
      @conn.rollback
      @conn.select_first("SELECT COUNT(*) FROM test_commit")[0].should == 1
    end

  end

end
