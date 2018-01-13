module PLSQL
  module SQLStatements
    # Select first row as array or values (without column names)
    def select_first(sql, *bindvars)
      @connection.select_first(sql, *bindvars)
    end

    # Select all rows as array or values (without column names)
    def select_all(sql, *bindvars, &block)
      @connection.select_all(sql, *bindvars, &block)
    end

    # Select one value (use if only one row with one value is selected)
    def select_one(sql, *bindvars)
      (row = @connection.select_first(sql, *bindvars)) && row[0]
    end

    # Select :first or :all values. Examples:
    #
    #   plsql.select :first, "SELECT * FROM employees WHERE employee_id = :1", 1
    #   plsql.select :all, "SELECT * FROM employees ORDER BY employee_id"
    def select(*args)
      case args[0]
      when nil
        raise ArgumentError, "Not enough arguments"
      when :first
        args.shift
        @connection.select_hash_first(*args)
      when :all
        args.shift
        @connection.select_hash_all(*args)
      else
        @connection.select_hash_all(*args)
      end
    end

    # Execute SQL statement. Example:
    #
    #   plsql.execute "DROP TABLE employees"
    def execute(*args)
      @connection.exec(*args)
    end

    # Execute COMMIT in current database session.
    # Use beforehand
    #
    #   plsql.connection.autocommit = false
    #
    # to turn off automatic commits after each statement.
    def commit
      @connection.commit
    end

    # Execute ROLLBACK in current database session.
    # Use beforehand
    #
    #   plsql.connection.autocommit = false
    #
    # to turn off automatic commits after each statement.
    def rollback
      @connection.rollback
    end

    # Create SAVEPOINT with specified name. Later use +rollback_to+ method to roll changes back
    # to specified savepoint.
    # Use beforehand
    #
    #   plsql.connection.autocommit = false
    #
    # to turn off automatic commits after each statement.
    def savepoint(name)
      execute "SAVEPOINT #{name}"
    end

    # Roll back changes to specified savepoint (that was created using +savepoint+ method)
    # Use beforehand
    #
    #   plsql.connection.autocommit = false
    #
    # to turn off automatic commits after each statement.
    def rollback_to(name)
      execute "ROLLBACK TO #{name}"
    end
  end
end
