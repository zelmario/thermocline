BIN     := bin/cline
DEV     := .dev
NODE    ?= thermocline-zelmar-node0
HOT_PORT  := 28017
COLD_PORT := 28018

.PHONY: build store test check race fmt clean dev dev-down dev-status install check-installed

# CGO_ENABLED=0 because the binary is built here and run in the test containers,
# which carry an older glibc. A dynamically linked build fails there at exec with
# a version error, which reads like the router refusing to start.
#
# -buildvcs=false because nothing reads the stamp and it makes the binary a
# function of the Go sources alone. With it on, editing this Makefile or a test
# script flips vcs.modified and the binary changes though no code did, which is
# what check-installed below has to be able to tell apart.
build:
	@mkdir -p bin
	CGO_ENABLED=0 go build -buildvcs=false -o $(BIN) ./cmd/cline
	@echo "built $(BIN)"

## store builds the shop the host-side suites drive.
##
## It needs CGO_ENABLED=0 for the same reason `build` does, and getting it wrong
## costs more here, because nothing downstream says so. testrollback copies this
## into the node and runs the seed with output suppressed, so a binary that
## cannot exec there leaves an empty fixture: on 2026-09-24 a plain
## `go build -o bin/store` produced ten failures reading as rollback clearing
## more than it should, when nothing had been seeded at all.
store:
	@mkdir -p bin
	CGO_ENABLED=0 go build -buildvcs=false -o bin/store ./test/store
	@echo "built bin/store"

## install puts this working tree's binary where the suites look for it.
##
## The suites run `cline` inside the test node, so what they measure is whatever
## /usr/local/bin/cline is in there, not what is in bin/ and not what is
## committed. Use this rather than a docker cp by hand.
install: build
	docker cp $(BIN) $(NODE):/usr/local/bin/cline
	@docker exec $(NODE) chmod +x /usr/local/bin/cline
	@CGO_ENABLED=0 go build -buildvcs=false -o bin/seqwrite ./test/seqwrite
	@docker cp bin/seqwrite $(NODE):/root/seqwrite >/dev/null
	@docker exec $(NODE) chmod +x /root/seqwrite
	@echo "installed into $(NODE)"

## check-installed answers one question before a sweep: is the node running this
## working tree's code?
##
## Build it the same way before comparing, which is why this depends on build
## rather than hashing whatever bin/cline happens to hold. A plain `go build`
## differs from the installed binary byte for byte at the same commit, because
## build sets CGO_ENABLED=0 and the setting is recorded in the binary. Comparing
## against one is a check that reports a mismatch every time and so means
## nothing: it cost two needless reinstalls on 2026-09-13 before anybody diffed
## `go version -m`.
check-installed: build
	@here=$$(md5sum $(BIN) | cut -d' ' -f1); \
	 there=$$(docker exec $(NODE) md5sum /usr/local/bin/cline 2>/dev/null | cut -d' ' -f1); \
	 if [ -z "$$there" ]; then echo "$(NODE) has no /usr/local/bin/cline; run make install"; exit 1; \
	 elif [ "$$here" = "$$there" ]; then echo "$(NODE) is running this working tree ($$here)"; \
	 else echo "$(NODE) is running something else: $$there, this tree builds $$here"; \
	      echo "run make install"; exit 1; fi

test:
	go test ./...

check: fmt
	go vet ./...
	go test ./...
	@bash test/partition.sh

## race runs the same unit tests under the race detector.
##
## Separate from check because it is several times slower, and worth running
## before a release and after anything that adds shared state to the router: it
## holds a claim cache, an index cache, a bracket cache and a set of namespaces
## the archive is not a superset of, and every one of those is read by every
## connection and written by one.
##
## What it does not cover is worth saying. It sees the races the unit tests
## reach, and the unit tests do not open two hundred client connections; the
## suites do that and they run against a binary, where the detector is not
## looking. A clean run here is a floor rather than a guarantee.
race:
	go test -race ./...

fmt:
	gofmt -w .

clean:
	rm -rf bin

## dev spins up two single-node replica sets to develop against.
dev:
	@mkdir -p $(DEV)/hot $(DEV)/cold
	@mongod --replSet rsHot  --port $(HOT_PORT)  --dbpath $(DEV)/hot \
		--bind_ip 127.0.0.1 --fork --logpath $(DEV)/hot.log  --oplogSize 128 >/dev/null
	@mongod --replSet rsCold --port $(COLD_PORT) --dbpath $(DEV)/cold \
		--bind_ip 127.0.0.1 --fork --logpath $(DEV)/cold.log --oplogSize 128 >/dev/null
	@mongosh --quiet --port $(HOT_PORT)  --eval \
		'rs.initiate({_id:"rsHot",members:[{_id:0,host:"127.0.0.1:$(HOT_PORT)"}]})' >/dev/null
	@mongosh --quiet --port $(COLD_PORT) --eval \
		'rs.initiate({_id:"rsCold",members:[{_id:0,host:"127.0.0.1:$(COLD_PORT)"}]})' >/dev/null
	@sleep 3
	@echo "fast tier  mongodb://127.0.0.1:$(HOT_PORT)/?replicaSet=rsHot"
	@echo "cold tier  mongodb://127.0.0.1:$(COLD_PORT)/?replicaSet=rsCold"

dev-status:
	@mongosh --quiet --port $(HOT_PORT)  --eval 'print("fast: primary=" + db.hello().isWritablePrimary)'
	@mongosh --quiet --port $(COLD_PORT) --eval 'print("cold: primary=" + db.hello().isWritablePrimary)'

dev-down:
	-@mongosh --quiet --port $(HOT_PORT)  --eval 'db.getSiblingDB("admin").shutdownServer()' 2>/dev/null
	-@mongosh --quiet --port $(COLD_PORT) --eval 'db.getSiblingDB("admin").shutdownServer()' 2>/dev/null
	@sleep 1
	rm -rf $(DEV)
	@echo "dev clusters removed"

## check-integration runs the tests that need a live pair, after `make dev`.
check-integration:
	TC_TEST_COLD='mongodb://127.0.0.1:$(COLD_PORT)/?replicaSet=rsCold' go test ./... -count=1
