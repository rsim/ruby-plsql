# encoding: utf-8

require 'spec_helper'

describe "Connection" do

  before(:all) do
    @raw_conn = get_connection
    @conn = PLSQL::Connection.create( @raw_conn )
  end

  after(:all) do
    unless defined?(JRuby)
      @raw_conn.logoff rescue nil
    else
      @raw_conn.close rescue nil
    end
  end

  describe "create and destroy" do
    before(:all) do
      @raw_conn1 = get_connection
    end

    before(:each) do
      @conn1 = PLSQL::Connection.create( @raw_conn1 )
    end

    it "should create connection" do
      expect(@conn1.raw_connection).to eq @raw_conn1
    end

    unless defined?(JRuby)
      it "should be oci connection" do
        expect(@conn1).to be_oci
        expect(@conn1.raw_driver).to eq :oci
      end
    else
      it "should be jdbc connection" do
        expect(@conn1).to be_jdbc
        expect(@conn1.raw_driver).to eq :jdbc
      end
    end

    it "should logoff connection" do
      expect(@conn1.logoff).to be true
    end

  end

  # Ruby 1.8 and 1.9
  unless defined?(JRuby)
    describe "OCI data type conversions" do
      it "should translate PL/SQL VARCHAR to Ruby String" do
        expect(@conn.plsql_to_ruby_data_type(:data_type => "VARCHAR", :data_length => 100)).to eq [String, 100]
        expect(@conn.plsql_to_ruby_data_type(:data_type => "VARCHAR", :data_length => nil)).to eq [String, 32767]
      end

      it "should translate PL/SQL VARCHAR2 to Ruby String" do
        expect(@conn.plsql_to_ruby_data_type(:data_type => "VARCHAR2", :data_length => 100)).to eq [String, 100]
        expect(@conn.plsql_to_ruby_data_type(:data_type => "VARCHAR2", :data_length => nil)).to eq [String, 32767]
      end

      it "should translate PL/SQL CLOB to Ruby String" do
        expect(@conn.plsql_to_ruby_data_type(:data_type => "CLOB", :data_length => 100_000)).to eq [OCI8::CLOB, nil]
        expect(@conn.plsql_to_ruby_data_type(:data_type => "CLOB", :data_length => nil)).to eq [OCI8::CLOB, nil]
      end

      it "should translate PL/SQL NUMBER to Ruby OraNumber" do
        expect(@conn.plsql_to_ruby_data_type(:data_type => "NUMBER", :data_length => 15)).to eq [OraNumber, nil]
      end

      it "should translate PL/SQL DATE to Ruby DateTime" do
        expect(@conn.plsql_to_ruby_data_type(:data_type => "DATE", :data_length => nil)).to eq [DateTime, nil]
      end

      it "should translate PL/SQL TIMESTAMP to Ruby Time" do
        expect(@conn.plsql_to_ruby_data_type(:data_type => "TIMESTAMP", :data_length => nil)).to eq [Time, nil]
      end

      it "should not translate small Ruby Integer when OraNumber type specified" do
        expect(@conn.ruby_value_to_ora_value(100, OraNumber)).to eql(100)
      end

      it "should not translate big Ruby Integer when OraNumber type specified" do
        ora_number = @conn.ruby_value_to_ora_value(12345678901234567890, OraNumber)
        expect(ora_number).to be_an Integer
        expect(ora_number.to_s).to eq "12345678901234567890"
        # OraNumber has more numeric comparison methods in ruby-oci8 2.0
        expect(ora_number).to eq OraNumber.new("12345678901234567890") if OCI8::VERSION >= '2.0.0'
      end

      it "should translate Ruby String value to OCI8::CLOB when OCI8::CLOB type specified" do
        large_text = "x" * 100_000
        ora_value = @conn.ruby_value_to_ora_value(large_text, OCI8::CLOB)
        expect(ora_value.class).to eq OCI8::CLOB
        expect(ora_value.size).to eq 100_000
        ora_value.rewind
        expect(ora_value.read).to eq large_text
      end

      it "should translate Oracle OraNumber integer value to Integer" do
        expect(@conn.ora_value_to_ruby_value(OraNumber.new(100))).to eql(100)
      end

      it "should translate Oracle OraNumber float value to BigDecimal" do
        expect(@conn.ora_value_to_ruby_value(OraNumber.new(100.11))).to eql(BigDecimal("100.11"))
      end

      # ruby-oci8 2.0 returns DATE as Time or DateTime
      if OCI8::VERSION < '2.0.0'
        it "should translate Oracle OraDate value to Time" do
          now = OraDate.now
          expect(@conn.ora_value_to_ruby_value(now)).to eql(now.to_time)
        end
      end

      it "should translate Oracle CLOB value to String" do
        large_text = "x" * 100_000
        clob = OCI8::CLOB.new(@raw_conn, large_text)
        expect(@conn.ora_value_to_ruby_value(clob)).to eq large_text
      end

    end

  # JRuby
  else

    describe "JDBC data type conversions" do
      it "should translate PL/SQL VARCHAR to Ruby String" do
        expect(@conn.plsql_to_ruby_data_type(:data_type => "VARCHAR", :data_length => 100)).to eq [String, 100]
        expect(@conn.plsql_to_ruby_data_type(:data_type => "VARCHAR", :data_length => nil)).to eq [String, 32767]
      end
      it "should translate PL/SQL VARCHAR2 to Ruby String" do
        expect(@conn.plsql_to_ruby_data_type(:data_type => "VARCHAR2", :data_length => 100)).to eq [String, 100]
        expect(@conn.plsql_to_ruby_data_type(:data_type => "VARCHAR2", :data_length => nil)).to eq [String, 32767]
      end

      it "should translate PL/SQL NUMBER to Ruby BigDecimal" do
        expect(@conn.plsql_to_ruby_data_type(:data_type => "NUMBER", :data_length => 15)).to eq [BigDecimal, nil]
      end

      it "should translate PL/SQL DATE to Ruby DateTime" do
        expect(@conn.plsql_to_ruby_data_type(:data_type => "DATE", :data_length => nil)).to eq [DateTime, nil]
      end

      it "should translate PL/SQL TIMESTAMP to Ruby Time" do
        expect(@conn.plsql_to_ruby_data_type(:data_type => "TIMESTAMP", :data_length => nil)).to eq [Time, nil]
      end

      it "should not translate Ruby Integer when BigDecimal type specified" do
        expect(@conn.ruby_value_to_ora_value(100, BigDecimal)).to eq java.math.BigDecimal.new(100)
      end

      it "should translate Ruby String to string value" do
        expect(@conn.ruby_value_to_ora_value(1.1, String)).to eq '1.1'
      end

      it "should translate Ruby Integer value to BigDecimal when BigDecimal type specified" do
        big_decimal = @conn.ruby_value_to_ora_value(12345678901234567890, BigDecimal)
        expect(big_decimal).to eq java.math.BigDecimal.new("12345678901234567890")
      end

      it "should translate Ruby String value to Java::OracleSql::CLOB when Java::OracleSql::CLOB type specified" do
        large_text = "x" * 100_000
        ora_value = @conn.ruby_value_to_ora_value(large_text, Java::OracleSql::CLOB)
        expect(ora_value.class).to eq Java::OracleSql::CLOB
        expect(ora_value.length).to eq 100_000
        expect(ora_value.getSubString(1, ora_value.length)).to eq large_text
        ora_value.freeTemporary
      end

      it "should translate Ruby nil value to nil when Java::OracleSql::CLOB type specified" do
        ora_value = @conn.ruby_value_to_ora_value(nil, Java::OracleSql::CLOB)
        expect(ora_value).to be_nil
      end

      it "should translate Oracle BigDecimal integer value to Integer" do
        expect(@conn.ora_value_to_ruby_value(BigDecimal("100"))).to eql(100)
      end

      it "should translate Oracle BigDecimal float value to BigDecimal" do
        expect(@conn.ora_value_to_ruby_value(BigDecimal("100.11"))).to eql(BigDecimal("100.11"))
      end

      it "should translate Oracle CLOB value to String" do
        large_text = "āčē" * 100_000
        clob = @conn.ruby_value_to_ora_value(large_text, Java::OracleSql::CLOB)
        expect(@conn.ora_value_to_ruby_value(clob)).to eq large_text
        clob.freeTemporary
      end

      it "should translate empty Oracle CLOB value to nil" do
        clob = @conn.ruby_value_to_ora_value(nil, Java::OracleSql::CLOB)
        expect(@conn.ora_value_to_ruby_value(clob)).to be_nil
      end

    end

  end

  describe "SQL SELECT statements" do

    it "should execute SQL statement and return first result" do
      @now = Time.local(2008,05,31,23,22,11)
      expect(@conn.select_first("SELECT 'abc',123,123.456,
        TO_DATE('#{@now.strftime("%Y-%m-%d %H:%M:%S")}','YYYY-MM-DD HH24:MI:SS')
        FROM dual")).to eq ["abc",123,123.456,@now]
    end

    it "should execute SQL statement and return first result as hash" do
      @now = Time.local(2008,05,31,23,22,11)
      expect(@conn.select_hash_first("SELECT 'abc' a, 123 b, 123.456 c,
        TO_DATE('#{@now.strftime("%Y-%m-%d %H:%M:%S")}', 'YYYY-MM-DD HH24:MI:SS') d
        FROM dual")).to eq({:a => "abc", :b => 123, :c => 123.456, :d => @now})
    end

    it "should execute SQL statement with bind parameters and return first result" do
      @today = Date.parse("2008-05-31")
      @now = Time.local(2008,05,31,23,22,11)
      expect(@conn.select_first("SELECT :1,:2,:3,:4,:5 FROM dual",
        'abc',123,123.456,@now,@today)).to eq ["abc",123,123.456,@now,Time.parse(@today.to_s)]
    end

    it "should execute SQL statement with NULL values and return first result" do
      @now = Time.local(2008,05,31,23,22,11)
      expect(@conn.select_first("SELECT NULL,123,123.456,
        TO_DATE('#{@now.strftime("%Y-%m-%d %H:%M:%S")}','YYYY-MM-DD HH24:MI:SS')
        FROM dual")).to eq [nil,123,123.456,@now]
    end

    if defined?(JRuby)

      it "should execute SQL statement with NULL values as bind parameters and return first result" do
        @today = Date.parse("2008-05-31")
        @now = Time.local(2008,05,31,23,22,11)
        expect(@conn.select_first("SELECT :1,:2,:3,:4,:5 FROM dual",
          nil,123,123.456,@now,@today)).to eq [nil,123,123.456,@now,Time.parse(@today.to_s)]
      end

    end

    it "should execute SQL statement and return all results" do
      @now = Time.local(2008,05,31,23,22,11)
      expect(@conn.select_all("SELECT 'abc',123,123.456,
          TO_DATE('#{@now.strftime("%Y-%m-%d %H:%M:%S")}','YYYY-MM-DD HH24:MI:SS')
          FROM dual
          UNION ALL SELECT 'abc',123,123.456,
          TO_DATE('#{@now.strftime("%Y-%m-%d %H:%M:%S")}','YYYY-MM-DD HH24:MI:SS')
          FROM dual")).to eq [["abc",123,123.456,@now],["abc",123,123.456,@now]]
    end

    it "should execute SQL statement and return all results as hash" do
      @now = Time.local(2008,05,31,23,22,11)
      expect(@conn.select_hash_all("SELECT 'abc' a, 123 b, 123.456 c,
          TO_DATE('#{@now.strftime("%Y-%m-%d %H:%M:%S")}','YYYY-MM-DD HH24:MI:SS') d
          FROM dual
          UNION ALL SELECT 'def' a, 123 b, 123.456 c,
          TO_DATE('#{@now.strftime("%Y-%m-%d %H:%M:%S")}','YYYY-MM-DD HH24:MI:SS') d
          FROM dual")).to eq [{:a=>"abc",:b=>123,:c=>123.456,:d=>@now},{:a=>"def",:b=>123,:c=>123.456,:d=>@now}]
    end

    it "should execute SQL statement with bind parameters and return all results" do
      @now = Time.local(2008,05,31,23,22,11)
      expect(@conn.select_all("SELECT :1,:2,:3,:4 FROM dual UNION ALL SELECT :1,:2,:3,:4 FROM dual",
        'abc',123,123.456,@now,'abc',123,123.456,@now)).to eq [["abc",123,123.456,@now],["abc",123,123.456,@now]]
    end

    it "should execute SQL statement and yield all results in block" do
      @now = Time.local(2008,05,31,23,22,11)
      expect(@conn.select_all("SELECT 'abc',123,123.456,
          TO_DATE('#{@now.strftime("%Y-%m-%d %H:%M:%S")}','YYYY-MM-DD HH24:MI:SS')
          FROM dual
          UNION ALL SELECT 'abc',123,123.456,
          TO_DATE('#{@now.strftime("%Y-%m-%d %H:%M:%S")}','YYYY-MM-DD HH24:MI:SS')
          FROM dual") do |r|
        expect(r).to eq ["abc",123,123.456,@now]
      end).to eq 2
    end

    it "should execute SQL statement with bind parameters and yield all results in block" do
      @now = Time.local(2008,05,31,23,22,11)
      expect(@conn.select_all("SELECT :1,:2,:3,:4 FROM dual UNION ALL SELECT :1,:2,:3,:4 FROM dual",
        'abc',123,123.456,@now,'abc',123,123.456,@now) do |r|
        expect(r).to eq ["abc",123,123.456,@now]
      end).to eq 2
    end

  end

  describe "PL/SQL procedures" do
    before(:all) do
      @random = rand(1000)
      @now = Time.local(2008,05,31,23,22,11)
      sql = <<-SQL
        CREATE OR REPLACE FUNCTION test_add_random (p_number NUMBER, p_varchar IN OUT VARCHAR2, p_date IN OUT DATE)
          RETURN NUMBER
        IS
        BEGIN
          RETURN p_number + #{@random};
        END test_add_random;
      SQL
      expect(@conn.exec(sql)).to be true
    end

    after(:all) do
      @conn.exec "DROP FUNCTION test_add_random"
    end

    it "should parse PL/SQL procedure call and bind parameters and exec and get bind parameter value" do
      sql = <<-SQL
        BEGIN
          :result := test_add_random (:p_number, :p_varchar, :p_date);
        END;
      SQL
      cursor = @conn.parse(sql)
      cursor.bind_param(":result", nil, :data_type => 'NUMBER', :in_out => 'OUT')
      cursor.bind_param(":p_number", 100, :data_type => 'NUMBER', :in_out => 'IN')
      cursor.bind_param(":p_varchar", "abc", :data_type => 'VARCHAR2', :in_out => 'IN/OUT')
      cursor.bind_param(":p_date", @now, :data_type => 'DATE', :in_out => 'IN/OUT')
      cursor.exec
      expect(cursor[":result"]).to eq @random + 100
      expect(cursor[":p_varchar"]).to eq "abc"
      expect(cursor[":p_date"]).to eq @now
      expect(cursor.close).to be_nil
    end

  end

  describe "commit and rollback" do
    before(:all) do
      expect(@conn.exec("CREATE TABLE test_commit (dummy VARCHAR2(100))")).to be true
      @conn.autocommit = false
      expect(@conn).not_to be_autocommit
    end

    after(:all) do
      @conn.exec "DROP TABLE test_commit"
    end

    after(:each) do
      @conn.exec "DELETE FROM test_commit"
      @conn.commit
    end

    it "should do commit" do
      @conn.exec("INSERT INTO test_commit VALUES ('test')")
      @conn.commit
      expect(@conn.select_first("SELECT COUNT(*) FROM test_commit")[0]).to eq 1
    end

    it "should do rollback" do
      @conn.exec("INSERT INTO test_commit VALUES ('test')")
      @conn.rollback
      expect(@conn.select_first("SELECT COUNT(*) FROM test_commit")[0]).to eq 0
    end

    it "should do commit and rollback should not undo commited transaction" do
      @conn.exec("INSERT INTO test_commit VALUES ('test')")
      @conn.commit
      @conn.rollback
      expect(@conn.select_first("SELECT COUNT(*) FROM test_commit")[0]).to eq 1
    end

  end

  describe "prefetch rows" do
    after(:each) do
      @conn.prefetch_rows = 1 # set back to default
    end

    it "should set prefetch rows for connection" do
      sql = "SELECT 1 FROM dual UNION ALL SELECT 1/0 FROM dual"
      @conn.prefetch_rows = 2
      expect {
        @conn.cursor_from_query(sql)
      }.to raise_error(/divisor is equal to zero/)
      @conn.prefetch_rows = 1
      expect {
        @conn.cursor_from_query(sql)
      }.not_to raise_error
    end

    it "should fetch just one row when using select_first" do
      sql = "SELECT 1 FROM dual UNION ALL SELECT 1/0 FROM dual"
      @conn.prefetch_rows = 2
      expect {
        @conn.select_first(sql)
      }.not_to raise_error
    end

  end

  describe "describe synonym" do
    before(:all) do
      @conn.exec "CREATE SYNONYM hr.synonym_for_dual FOR sys.dual"
    end

    after(:all) do
      @conn.exec "DROP SYNONYM hr.synonym_for_dual"
    end

    it "should describe local synonym" do
      expect(@conn.describe_synonym('HR','SYNONYM_FOR_DUAL')).to eq ['SYS', 'DUAL']
      expect(@conn.describe_synonym('hr','synonym_for_dual')).to eq ['SYS', 'DUAL']
      expect(@conn.describe_synonym(:hr,:synonym_for_dual)).to eq ['SYS', 'DUAL']
    end

    it "should return nil on non-existing synonym" do
      expect(@conn.describe_synonym('HR','SYNONYM_FOR_XXX')).to be_nil
      expect(@conn.describe_synonym('hr','synonym_for_xxx')).to be_nil
      expect(@conn.describe_synonym(:hr,:synonym_for_xxx)).to be_nil
    end

    it "should describe public synonym" do
      expect(@conn.describe_synonym('PUBLIC','DUAL')).to eq ['SYS', 'DUAL']
      expect(@conn.describe_synonym('PUBLIC','dual')).to eq ['SYS', 'DUAL']
      expect(@conn.describe_synonym('PUBLIC',:dual)).to eq ['SYS', 'DUAL']
    end

  end

  describe "session information" do
    it "should get database version" do
      # using Oracle version 10.2.0.4 for unit tests
      expect(@conn.database_version).to eq DATABASE_VERSION.split('.').map{|n| n.to_i}
    end

    it "should get session ID" do
      expect(@conn.session_id).to eq @conn.select_first("SELECT USERENV('SESSIONID') FROM dual")[0].to_i
    end
  end

  describe "drop ruby temporary tables" do
    after(:all) do
      @conn.drop_all_ruby_temporary_tables
    end

    it "should drop all ruby temporary tables" do
      tmp_table = "ruby_111_222_333"
      @conn.exec "CREATE GLOBAL TEMPORARY TABLE #{tmp_table} (dummy CHAR(1))"
      expect { @conn.select_first("SELECT * FROM #{tmp_table}") }.not_to raise_error
      @conn.drop_all_ruby_temporary_tables
      expect { @conn.select_first("SELECT * FROM #{tmp_table}") }.to raise_error(/table or view does not exist/)
    end

    it "should drop current session ruby temporary tables" do
      tmp_table = "ruby_#{@conn.session_id}_222_333"
      @conn.exec "CREATE GLOBAL TEMPORARY TABLE #{tmp_table} (dummy CHAR(1))"
      expect { @conn.select_first("SELECT * FROM #{tmp_table}") }.not_to raise_error
      @conn.drop_session_ruby_temporary_tables
      expect { @conn.select_first("SELECT * FROM #{tmp_table}") }.to raise_error(/table or view does not exist/)
    end

    it "should not drop other session ruby temporary tables" do
      tmp_table = "ruby_#{@conn.session_id+1}_222_333"
      @conn.exec "CREATE GLOBAL TEMPORARY TABLE #{tmp_table} (dummy CHAR(1))"
      expect { @conn.select_first("SELECT * FROM #{tmp_table}") }.not_to raise_error
      @conn.drop_session_ruby_temporary_tables
      expect { @conn.select_first("SELECT * FROM #{tmp_table}") }.not_to raise_error
    end

  end

  describe "logoff" do
    before(:each) do
      # restore connection before each test
      reconnect_connection
    end

    after(:all) do
      @conn.exec "DROP TABLE test_dummy_table" rescue nil
    end

    def reconnect_connection
      @raw_conn = get_connection
      @conn = PLSQL::Connection.create( @raw_conn )
    end

    it "should drop current session ruby temporary tables" do
      tmp_table = "ruby_#{@conn.session_id}_222_333"
      @conn.exec "CREATE GLOBAL TEMPORARY TABLE #{tmp_table} (dummy CHAR(1))"
      expect { @conn.select_first("SELECT * FROM #{tmp_table}") }.not_to raise_error
      @conn.logoff
      reconnect_connection
      expect { @conn.select_first("SELECT * FROM #{tmp_table}") }.to raise_error(/table or view does not exist/)
    end

    it "should rollback any uncommited transactions" do
      tmp_table = "ruby_#{@conn.session_id}_222_333"
      old_autocommit = @conn.autocommit?
      @conn.autocommit = false
      @conn.exec "CREATE GLOBAL TEMPORARY TABLE #{tmp_table} (dummy CHAR(1))"
      @conn.exec "CREATE TABLE test_dummy_table (dummy CHAR(1))"
      @conn.exec "INSERT INTO test_dummy_table VALUES ('1')"
      # logoff will drop ruby temporary tables, it should do rollback before drop table
      @conn.logoff
      reconnect_connection
      expect(@conn.select_first("SELECT * FROM test_dummy_table")).to eq nil
      @conn.autocommit = old_autocommit
    end

  end

end
