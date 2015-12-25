all:install

GOPATH:=$(CURDIR)/../../../../
export GOPATH
export GO15VENDOREXPERIMENT=1

godep:
	which godep >/dev/null 2>&1 || go get github.com/tools/godep

savedep: godep
	rm -r Godeps
	godep save ./...

test:
	./misc/gobeansdb_server.sh start
	go version
	go test github.intra.douban.com/coresys/gobeansproxy/config
	go test github.intra.douban.com/coresys/gobeansproxy/dstore
	./misc/gobeansdb_server.sh stop

install:
	go install github.intra.douban.com/coresys/gobeansproxy/proxy
