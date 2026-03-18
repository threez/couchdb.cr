module CouchDB
  # Base class for all CouchDB library exceptions.
  # Rescue `CouchDB::Error` to catch any error raised by this library.
  class Error < Exception
  end

  # Raised when a document ID does not exist or has been deleted (HTTP 404).
  class NotFound < Error
    def initialize(id : String)
      super("Document not found: #{id}")
    end
  end

  # Raised on a `put` or `remove` with a stale or missing revision (HTTP 409).
  class Conflict < Error
    def initialize(id : String, rev : String)
      super("Document update conflict: #{id} rev #{rev}")
    end
  end

  # Raised when the remote CouchDB returns HTTP 401.
  class Unauthorized < Error
    def initialize(message : String = "Unauthorized")
      super(message)
    end
  end

  # Raised for malformed requests such as a missing `_id` or missing `_rev`
  # on a replication write (HTTP 400).
  class BadRequest < Error
    def initialize(message : String = "Bad request")
      super(message)
    end
  end

  # Raised when replication encounters an unrecoverable failure.
  class ReplicationError < Error
    def initialize(message : String)
      super(message)
    end
  end
end
