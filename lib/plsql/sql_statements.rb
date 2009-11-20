module PLSQL
  module SQLStatements
    def select_first(sql, *bindvars)
      @connection.select_first(sql, *bindvars)
    end

    def select_one(sql, *bindvars)
      (row = @connection.select_first(sql, *bindvars)) && row[0]
    end

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

    # def insert(params)
    #   raise ArgumentError, "Wrong parameter passed to plsql.insert" unless params.is_a?(Hash) &&
    #     params.keys.all?(|k| [:into, :values].include? k)
    # 
    # end

    def execute(*args)
      @connection.exec(*args)
    end

    def commit
      connection.commit
    end

    def rollback
      connection.rollback
    end

  end
end

