class TestDb

  DATABASE_USERS = %w{hr arunit}

  def self.build
    db = self.new
    db.cleanup_database_users
    db.create_user_tablespace
    db.setup_database_users
    db.connection.logoff
  end

  def self.database_version
     db = self.new
     db.database_version
  end

  def connection
    unless defined?(@connection)
      begin
        Timeout::timeout(5) {
          if defined?(JRUBY_VERSION)
            @connection = java.sql.DriverManager.get_connection(
              'jdbc:oracle:thin:@127.0.0.1:1521/XE',
              'system',
              'oracle'
            );
          else
            @connection = OCI8.new(
              'system',
              'oracle',
              '(DESCRIPTION=(ADDRESS=(PROTOCOL=tcp)(HOST=127.0.0.1)(PORT=1521))(CONNECT_DATA=(SERVICE_NAME=XE)))'
            )
          end
        }
      rescue Timeout::Error
        raise "Cannot establish connection with Oracle database as SYSTEM user. Seams you need to start local Oracle database"
      end
    end
    @connection
  end

  def create_user_tablespace
    return unless connection
    execute_statement(<<-STATEMENT
      DECLARE
        v_exists number;
      BEGIN
        SELECT count(1)
          INTO v_exists
          FROM dba_tablespaces
          WHERE tablespace_name = 'TBS_USERS';

          IF v_exists = 0 THEN
            EXECUTE IMMEDIATE 'ALTER SYSTEM SET DB_CREATE_FILE_DEST = ''/u01/app/oracle/oradata/XE''';
            EXECUTE IMMEDIATE 'CREATE TABLESPACE TBS_USERS DATAFILE ''tbs_users.dat'' SIZE 10M REUSE AUTOEXTEND ON NEXT 10M MAXSIZE 200M';
          END IF;
      END;
      STATEMENT
      )
  end

  def database_users
    DATABASE_USERS.inject([]){|array, user| array << [user.upcase, user]}
  end

  def cleanup_database_users
    return unless connection
    database_users.each do | db, _ |
      execute_statement(<<-STATEMENT
        DECLARE
           v_count INTEGER := 0;
           l_cnt   INTEGER;
        BEGIN

          SELECT COUNT (1)
            INTO v_count
            FROM dba_users
            WHERE username = '#{db}';

          IF v_count != 0 THEN
            FOR x IN (SELECT *
                        FROM v$session
                        WHERE username = '#{db}')
            LOOP
              EXECUTE IMMEDIATE 'ALTER SYSTEM DISCONNECT SESSION ''' || x.sid || ',' || x.serial# || ''' IMMEDIATE';
            END LOOP;

            EXECUTE IMMEDIATE ('DROP USER #{db} CASCADE');
          END IF;
        END;
        STATEMENT
      )
    end
  end

  def setup_database_users
    return unless connection
    database_users.each do | db, passwd |
      execute_statement(<<-STATEMENT
        DECLARE
           v_count INTEGER := 0;
        BEGIN

          SELECT COUNT (1)
            INTO v_count
            FROM dba_users
            WHERE username = '#{db}';

          IF v_count = 0 THEN
            EXECUTE IMMEDIATE ('CREATE USER #{db} IDENTIFIED BY #{passwd} DEFAULT TABLESPACE TBS_USERS QUOTA 10m ON TBS_USERS');
            EXECUTE IMMEDIATE ('GRANT create session, create table, create sequence, create procedure, create type, create view, create synonym TO #{db}');
          END IF;
        END;
        STATEMENT
      )
    end
  end

  def database_version
    query = 'SELECT version FROM V$INSTANCE'

    if defined?(JRUBY_VERSION)
      statement = connection.create_statement
      resource  = statement.execute_query(query)

      resource.next
      value = resource.get_string('VERSION')

      resource.close
      statement.close
    else
      cursor = execute_statement(query)
      value = cursor.fetch()[0]
      cursor.close
    end

    value.match(/(.*)\.\d$/)[1]
  end

  def execute_statement(statement)
    if defined?(JRUBY_VERSION)
      statement = connection.prepare_call(statement)
      statement.execute
      statement.close
    else
      connection.exec(statement)
    end
  end
end
