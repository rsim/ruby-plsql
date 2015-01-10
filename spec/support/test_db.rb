class TestDb

  DATABASES = %w{hr arunit}

  def self.build
    db = self.new
    db.drop_databases(DATABASES)
    db.create_databases(DATABASES)
    db.connection.logoff
  end

  def self.database_version
     db = self.new
     db.database_version
  end

  def self.drop
    db = self.new
    db.drop_databases(DATABASES)
    db.connection.logoff
  end

  def connection
    unless defined?(@connection)
      begin
        Timeout::timeout(5) {
          @connection = OCI8.new(
            'system',
            'oracle',
            '(DESCRIPTION=(ADDRESS=(PROTOCOL=tcp)(HOST=127.0.0.1)(PORT=1521))(CONNECT_DATA=(SERVICE_NAME=XE)))'
          )
        }
      rescue Timeout::Error
        raise "Cannot establish connection with Oracle database as SYSTEM user. Seams you need to start local Oracle database"
      end
    end
    @connection
  end

  def drop_databases(databases=[])
    return unless connection
    databases.each do |db|
      connection.exec(<<-STATEMENT
        DECLARE
           v_count INTEGER := 0;
           l_cnt   INTEGER;
        BEGIN

          SELECT COUNT (1)
            INTO v_count
            FROM dba_users
            WHERE username = UPPER('#{db}');

          IF v_count != 0 THEN
            FOR x IN (SELECT *
                        FROM v$session
                        WHERE username = UPPER('#{db}'))
            LOOP
              EXECUTE IMMEDIATE 'alter system kill session ''' || x.sid || ',' || x.serial# || ''' IMMEDIATE';
            END LOOP;

            EXECUTE IMMEDIATE ('DROP USER #{db} CASCADE');
          END IF;
        END;
        STATEMENT
      )
    end
  end

  def create_databases(databases=[])
    return unless connection
    databases.each do |db|
      connection.exec(<<-STATEMENT
        DECLARE
           v_count INTEGER := 0;
        BEGIN

          SELECT COUNT (1)
            INTO v_count
            FROM dba_users
            WHERE username = UPPER ('#{db}');

          IF v_count = 0 THEN
            EXECUTE IMMEDIATE ('CREATE USER #{db} IDENTIFIED BY #{db}');
            EXECUTE IMMEDIATE ('GRANT unlimited tablespace, create session, create table, create sequence, create procedure, create type, create view, create synonym TO #{db}');
            EXECUTE IMMEDIATE ('ALTER USER #{db} QUOTA 50m ON SYSTEM');
          END IF;
        END;
        STATEMENT
      )
    end
  end

  def database_version
    return unless connection
    cursor = connection.exec('SELECT version FROM V$INSTANCE')
    cursor.fetch()[0].match(/(.*)\.\d$/)[1]
  ensure
    cursor.close
  end

end
