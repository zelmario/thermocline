BIN     := bin/cline
DEV     := .dev
HOT_PORT  := 28017
COLD_PORT := 28018

.PHONY: build test check fmt clean dev dev-down dev-status

build:
	@mkdir -p bin
	go build -o $(BIN) ./cmd/cline
	@echo "built $(BIN)"

test:
	go test ./...

check: fmt
	go vet ./...
	go test ./...

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
