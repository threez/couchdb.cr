require "json"
require "base64"

require "./couchdb/error"
require "./couchdb/document"
require "./couchdb/adapter/base"
require "./couchdb/adapter/sqlite"
require "./couchdb/adapter/http"
require "./couchdb/replication/session"
require "./couchdb/replication/checkpoint"
require "./couchdb/replication/replicator"
require "./couchdb/database"
require "./couchdb/local_replica"

module CouchDB
  VERSION = "0.1.1"
end
