.PHONY: all clean fmt fmtcheck lint fix docs spec goydb e2e version tag

COUCHDB_URL ?= http://admin:secret@localhost:7070

all: clean fmt lint docs spec

fmt:
	crystal tool format

fmtcheck:
	crystal tool format --check

spec:
	crystal spec -v

lib/ameba/bin/ameba:
	shards install

lint: lib/ameba/bin/ameba
	lib/ameba/bin/ameba

fix: lib/ameba/bin/ameba
	lib/ameba/bin/ameba --fix

docs:
	crystal docs

clean:
	rm -rf docs/

# Start goydb with project-local settings (foreground; Ctrl-C to stop)
goydb:
	mkdir -p tmp/goydb-dbs tmp/goydb-public
	goydb -addr :7070 -admins "admin:secret" -dbs tmp/goydb-dbs -public tmp/goydb-public

# Run HTTP e2e tests against a live goydb/CouchDB instance
e2e:
	COUCHDB_URL=$(COUCHDB_URL) crystal spec spec/adapter_http_spec.cr -v

# Sync the VERSION constant in src/ to match shard.yml's version field.
# Bump shard.yml's version first, then run `make version`.
version:
	@V=$$(grep '^version:' shard.yml | sed -E 's/^version:[[:space:]]*//'); \
	for f in $$(grep -rl '^[[:space:]]*VERSION[[:space:]]*=[[:space:]]*"[^"]*"' src/ 2>/dev/null); do \
		sed -E "s/^([[:space:]]*VERSION[[:space:]]*=[[:space:]]*)\"[^\"]*\"/\\1\"$$V\"/" "$$f" > "$$f.tmp" && mv "$$f.tmp" "$$f"; \
		echo "updated $$f to $$V"; \
	done

# Create an annotated git tag "vX.Y.Z" from shard.yml's version field.
tag:
	@V=$$(grep '^version:' shard.yml | sed -E 's/^version:[[:space:]]*//'); \
	git tag -a "v$$V" -m "Release v$$V"; \
	echo "tagged v$$V"
