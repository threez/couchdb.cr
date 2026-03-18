require "json"
require "digest/md5"

module CouchDB
  # An open-schema CouchDB document with typed `_id`, `_rev`, and `_deleted` fields.
  #
  # `Document` uses `JSON::Serializable` for round-trip JSON encoding and
  # `JSON::Serializable::Unmapped` to preserve any extra fields in `json_unmapped`.
  # Subclass it to add strongly-typed application fields:
  #
  # ```
  # class Note < CouchDB::Document
  #   property title : String = ""
  #   property body : String = ""
  # end
  # ```
  #
  # Subclass properties are serialized as top-level JSON keys alongside `_id` and `_rev`.
  # Unknown fields are transparently preserved through `json_unmapped` and survive
  # replication without loss.
  class Document
    include JSON::Serializable
    include JSON::Serializable::Unmapped

    @[JSON::Field(key: "_id")]
    property id : String = ""

    @[JSON::Field(key: "_rev", emit_null: false)]
    property rev : String? = nil

    @[JSON::Field(key: "_deleted", emit_null: false)]
    property deleted : Bool? = nil

    # Creates a new empty document. Set `id` before calling `put`.
    def initialize
    end

    # Returns `true` when `deleted` is explicitly set to `true`.
    def deleted? : Bool
      @deleted == true
    end

    # Computes the next revision string `"N-<md5>"` without mutating `self`.
    #
    # The hash is computed over the document JSON with `_rev` removed, matching
    # the CouchDB wire format. Delegates to `DocumentHelper.next_rev`.
    def next_rev : String
      DocumentHelper.next_rev(self)
    end

    # Returns the integer generation counter from the current `rev` (e.g. `"3-abc"` → `3`).
    # Returns `0` for new documents that have no `rev`.
    def rev_num : Int32
      DocumentHelper.rev_num(@rev || "0-")
    end

    # Reads a field by JSON key.
    #
    # Routes `"_id"`, `"_rev"`, and `"_deleted"` to their typed properties;
    # all other keys are read from `json_unmapped`. Raises `KeyError` if the
    # key is absent (use `[]?` for a nil-safe variant).
    def [](key : String) : JSON::Any
      case key
      when "_id"      then JSON::Any.new(@id)
      when "_rev"     then JSON::Any.new(@rev)
      when "_deleted" then JSON::Any.new(@deleted)
      else                 json_unmapped[key]
      end
    end

    # Reads a field by JSON key, returning `nil` if the key is absent.
    #
    # Routes `"_id"`, `"_rev"`, and `"_deleted"` to their typed properties;
    # all other keys are read from `json_unmapped`.
    def []?(key : String) : JSON::Any?
      case key
      when "_id"      then JSON::Any.new(@id)
      when "_rev"     then @rev ? JSON::Any.new(@rev) : nil
      when "_deleted" then @deleted.nil? ? nil : JSON::Any.new(@deleted)
      else                 json_unmapped[key]?
      end
    end

    # Writes a field by JSON key.
    #
    # Routes `"_id"`, `"_rev"`, and `"_deleted"` to their typed properties;
    # all other keys are stored in `json_unmapped`.
    def []=(key : String, value : JSON::Any)
      case key
      when "_id"      then @id = value.as_s
      when "_rev"     then @rev = value.as_s
      when "_deleted" then @deleted = value.as_bool
      else                 json_unmapped[key] = value
      end
    end
  end

  # Utility module for low-level document operations.
  #
  # Prefer instance methods on `Document` for new code (`doc.next_rev`, `doc.rev_num`,
  # `doc.deleted?`). This module is provided for callers that work with documents
  # generically and may also be used by adapter internals.
  module DocumentHelper
    # Computes the next revision for `doc` using the CouchDB `"N-<md5>"` scheme.
    #
    # The hash is derived from the document JSON with `_rev` removed so that the
    # revision string is deterministic given the same document content.
    def self.next_rev(doc : Document) : String
      current_rev = doc.rev || "0-"
      n = current_rev.split("-", 2).first.to_i? || 0
      # Serialize without _rev for hashing — matches Hash-based path field order
      # (_id first, then json_unmapped fields in insertion order)
      data = JSON.parse(doc.to_json).as_h
      data.delete("_rev")
      hash = Digest::MD5.hexdigest(data.to_json)
      "#{n + 1}-#{hash}"
    end

    # Parses the generation number from a revision string (e.g. `"3-abc"` → `3`).
    # Returns `0` for malformed input.
    def self.rev_num(rev : String) : Int32
      rev.split("-", 2).first.to_i? || 0
    end

    # Returns `true` when the document's `deleted` field is `true`.
    def self.deleted?(doc : Document) : Bool
      doc.deleted?
    end
  end
end
