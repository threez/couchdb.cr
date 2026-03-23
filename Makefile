.PHONY: all clean fmt lint docs spec fix goydb e2e version

COUCHDB_URL ?= http://admin:secret@localhost:7070

all: clean fmt lint docs spec

fmt:
	crystal tool format src/ spec/

spec:
	crystal spec --verbose

lint: lib/ameba/bin/ameba
	lib/ameba/bin/ameba

fix: lib/ameba/bin/ameba
	lib/ameba/bin/ameba --fix

lib/ameba/bin/ameba:
	shards install

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
	COUCHDB_URL=$(COUCHDB_URL) crystal spec spec/adapter_http_spec.cr --verbose

# Bump the version in shard.yml and src/couchdb.cr and commit
# Usage: VERSION=x.y.z make version
version:
	@test -n "$(VERSION)" || (echo "Usage: VERSION=x.y.z make version" && exit 1)
	sed -i 's/^version: .*/version: $(VERSION)/' shard.yml
	sed -i 's/VERSION = ".*"/VERSION = "$(VERSION)"/' src/couchdb.cr
	git add shard.yml src/couchdb.cr
	git commit -m "Bump version to $(VERSION)"
