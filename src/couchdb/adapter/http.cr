require "http/client"
require "uri"
require "./base"

module CouchDB
  module Adapter
    # Remote-CouchDB adapter that proxies all operations over HTTP/HTTPS.
    #
    # Instantiate via `CouchDB::Database.new("http://...")` — the `Database` facade
    # auto-selects this adapter when the location starts with `http://` or `https://`.
    #
    # URL format: `http[s]://[user:password@]host[:port]/dbname`
    # Credentials embedded in the URL are sent as HTTP Basic authentication.
    class HTTP
      include Adapter

      @base_url : String
      @db_path : String
      @db_name : String
      @auth : String?

      # Parses a CouchDB URL and configures the adapter.
      # Credentials in the URL are extracted and used for Basic auth on every request.
      def initialize(url : String)
        uri = URI.parse(url)
        @base_url = "#{uri.scheme}://#{uri.host}:#{uri.port || default_port(uri.scheme)}"
        @db_path = uri.path.to_s.chomp("/")
        @db_name = @db_path.split("/").last
        @auth = if uri.user
                  credentials = "#{uri.user}:#{uri.password}"
                  "Basic #{Base64.strict_encode(credentials)}"
                end
      end

      # HTTP implementation of `Adapter#info`. See `Adapter#info` for the contract.
      def info : NamedTuple(db_name: String, doc_count: Int64, update_seq: Int64)
        resp = get_request(@db_path)
        check_response!(resp)
        data = JSON.parse(resp.body)
        doc_count = data["doc_count"]?.try(&.as_i64?) || 0_i64
        update_seq = data["update_seq"]?.try(&.as_i64?) ||
                     data["update_seq"]?.try(&.as_s?.try(&.to_i64?)) || 0_i64
        {db_name: @db_name, doc_count: doc_count, update_seq: update_seq}
      end

      # HTTP implementation of `Adapter#get`. See `Adapter#get` for the contract.
      def get(id : String) : Document
        resp = get_request("#{@db_path}/#{URI.encode_path(id)}")
        raise NotFound.new(id) if resp.status_code == 404
        check_response!(resp)
        Document.from_json(resp.body)
      end

      # HTTP implementation of `Adapter#put`. See `Adapter#put` for the contract.
      def put(doc : Document) : NamedTuple(ok: Bool, id: String, rev: String)
        id = doc.id
        raise BadRequest.new("Missing _id") if id.empty?
        resp = put_request("#{@db_path}/#{URI.encode_path(id)}", doc.to_json)
        raise Conflict.new(id, doc.rev || "") if resp.status_code == 409
        check_response!(resp)
        data = JSON.parse(resp.body)
        {ok: true, id: data["id"].as_s, rev: data["rev"].as_s}
      end

      # HTTP implementation of `Adapter#remove`. See `Adapter#remove` for the contract.
      def remove(id : String, rev : String) : NamedTuple(ok: Bool)
        resp = delete_request("#{@db_path}/#{URI.encode_path(id)}?rev=#{rev}")
        raise NotFound.new(id) if resp.status_code == 404
        raise Conflict.new(id, rev) if resp.status_code == 409
        check_response!(resp)
        {ok: true}
      end

      # HTTP implementation of `Adapter#bulk_docs`. See `Adapter#bulk_docs` for the contract.
      def bulk_docs(docs : Array(Document), new_edits : Bool = true) : Array(NamedTuple(id: String, rev: String, ok: Bool))
        # Build payload explicitly to avoid mixed-type Hash serialization issues
        payload = String.build do |io|
          io << %({"docs":)
          io << "["
          docs.each_with_index do |doc, i|
            io << "," if i > 0
            doc.to_json(io)
          end
          io << %(],"new_edits":)
          new_edits.to_json(io)
          io << "}"
        end
        resp = post_request("#{@db_path}/_bulk_docs", payload)
        check_response!(resp)

        JSON.parse(resp.body).as_a.map do |item|
          id = item["id"]?.try(&.as_s?) || ""
          rev = item["rev"]?.try(&.as_s?) || ""
          ok = item["ok"]?.try(&.as_bool?) || false
          {id: id, rev: rev, ok: ok}
        end
      end

      # HTTP implementation of `Adapter#all_docs`. See `Adapter#all_docs` for the contract.
      def all_docs(include_docs : Bool = false, limit : Int32? = nil, skip : Int32 = 0,
                   startkey : String? = nil, endkey : String? = nil) : NamedTuple(
        total_rows: Int64,
        offset: Int32,
        rows: Array(JSON::Any))
        params = "include_docs=#{include_docs}&skip=#{skip}"
        params += "&limit=#{limit}" if limit
        # CouchDB requires keys to be JSON-encoded strings in the URL
        params += "&startkey=#{URI.encode_path("\"#{startkey}\"")}" if startkey
        params += "&endkey=#{URI.encode_path("\"#{endkey}\"")}" if endkey
        resp = get_request("#{@db_path}/_all_docs?#{params}")
        check_response!(resp)
        data = JSON.parse(resp.body)
        total = data["total_rows"]?.try(&.as_i64?) || 0_i64
        offset = data["offset"]?.try(&.as_i?) || skip
        rows = data["rows"]?.try(&.as_a?) || [] of JSON::Any
        {total_rows: total, offset: offset, rows: rows}
      end

      # HTTP implementation of `Adapter#changes`. See `Adapter#changes` for the contract.
      def changes(since : String = "0", limit : Int32? = nil, include_docs : Bool = false) : NamedTuple(
        last_seq: String,
        results: Array(JSON::Any))
        params = "since=#{since}&include_docs=#{include_docs}"
        params += "&limit=#{limit}" if limit
        resp = get_request("#{@db_path}/_changes?#{params}")
        check_response!(resp)
        data = JSON.parse(resp.body)
        last_seq = data["last_seq"]?.try(&.as_s?) ||
                   data["last_seq"]?.try(&.as_i64?.try(&.to_s)) || since
        results = data["results"]?.try(&.as_a?) || [] of JSON::Any
        {last_seq: last_seq, results: results}
      end

      # HTTP implementation of `Adapter#revs_diff`. See `Adapter#revs_diff` for the contract.
      def revs_diff(id_revs : Hash(String, Array(String))) : Hash(String, NamedTuple(missing: Array(String)))
        payload = id_revs.to_json
        resp = post_request("#{@db_path}/_revs_diff", payload)
        check_response!(resp)

        result = {} of String => NamedTuple(missing: Array(String))
        JSON.parse(resp.body).as_h.each do |id, val|
          missing = val["missing"]?.try(&.as_a?.try(&.map(&.as_s))) || [] of String
          result[id] = {missing: missing}
        end
        result
      end

      # HTTP implementation of `Adapter#bulk_get`. See `Adapter#bulk_get` for the contract.
      def bulk_get(id_revs : Array(NamedTuple(id: String, rev: String))) : Array(Document)
        docs_param = id_revs.map { |pair| {"id" => pair[:id], "rev" => pair[:rev]} }
        payload = {"docs" => docs_param}.to_json
        resp = post_request("#{@db_path}/_bulk_get", payload)
        check_response!(resp)

        docs = [] of Document
        results = JSON.parse(resp.body)["results"]?.try(&.as_a?) || [] of JSON::Any
        results.each do |result|
          (result["docs"]?.try(&.as_a?) || [] of JSON::Any).each do |item|
            ok = item["ok"]?
            docs << Document.from_json(ok.to_json) if ok
          end
        end
        docs
      end

      # HTTP implementation of `Adapter#get_local`. See `Adapter#get_local` for the contract.
      def get_local(id : String) : Document
        full_id = local_id(id)
        resp = get_request("#{@db_path}/#{full_id}")
        raise NotFound.new(full_id) if resp.status_code == 404
        check_response!(resp)
        Document.from_json(resp.body)
      end

      # HTTP implementation of `Adapter#put_local`. See `Adapter#put_local` for the contract.
      def put_local(doc : Document) : NamedTuple(ok: Bool, id: String, rev: String)
        raw_id = doc.id
        raise BadRequest.new("Missing _id") if raw_id.empty?
        full_id = local_id(raw_id)
        resp = put_request("#{@db_path}/#{full_id}", doc.to_json)
        raise Conflict.new(full_id, doc.rev || "") if resp.status_code == 409
        check_response!(resp)
        data = JSON.parse(resp.body)
        {ok: true, id: data["id"].as_s, rev: data["rev"].as_s}
      end

      private def get_request(path : String) : ::HTTP::Client::Response
        client.get(path, headers: default_headers)
      end

      private def put_request(path : String, body : String) : ::HTTP::Client::Response
        client.put(path, headers: json_headers, body: body)
      end

      private def post_request(path : String, body : String) : ::HTTP::Client::Response
        client.post(path, headers: json_headers, body: body)
      end

      private def delete_request(path : String) : ::HTTP::Client::Response
        client.delete(path, headers: default_headers)
      end

      private def client : ::HTTP::Client
        ::HTTP::Client.new(URI.parse(@base_url))
      end

      private def default_headers : ::HTTP::Headers
        h = ::HTTP::Headers.new
        if auth = @auth
          h["Authorization"] = auth
        end
        h["Accept"] = "application/json"
        h
      end

      private def json_headers : ::HTTP::Headers
        h = default_headers
        h["Content-Type"] = "application/json"
        h
      end

      private def check_response!(resp : ::HTTP::Client::Response)
        case resp.status_code
        when 200, 201, 202
          # ok
        when 400
          raise BadRequest.new(resp.body)
        when 401
          raise Unauthorized.new(resp.body)
        when 404
          raise NotFound.new(resp.body)
        when 409
          raise Conflict.new("unknown", "unknown")
        else
          raise Error.new("HTTP #{resp.status_code}: #{resp.body}")
        end
      end

      private def local_id(id : String) : String
        id.starts_with?("_local/") ? id : "_local/#{id}"
      end

      private def default_port(scheme : String?) : Int32
        scheme == "https" ? 443 : 5984
      end
    end
  end
end
