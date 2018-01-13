module PLSQL #:nodoc:
  module ArrayHelpers #:nodoc:
    def self.to_hash(keys, values) #:nodoc:
      (0...keys.size).inject({}) { |hash, i| hash[keys[i]] = values[i]; hash }
    end
  end
end
