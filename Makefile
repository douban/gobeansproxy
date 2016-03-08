all:install

GOPATH:=$(CURDIR)/../../../../
export GOPATH
export GO15VENDOREXPERIMENT=1
export PYTHONPATH=.

godep:
	which godep >/dev/null 2>&1 || go get github.com/tools/godep

savedep: godep
	if [ -d "./Godeps" ]; then rm -r Godeps; fi
	godep save ./...

test:
	./misc/gobeansdb_server.sh start
	go version
	go test github.intra.douban.com/coresys/gobeansproxy/config
	go test github.intra.douban.com/coresys/gobeansproxy/dstore
	./misc/gobeansdb_server.sh stop

template:
	rm -r /var/lib/gobeansproxy/templates
	cp -r templates /var/lib/gobeansproxy/

pytest:install
	./tests/run_test.sh

install:
	go install github.intra.douban.com/coresys/gobeansproxy/gobeansproxy
