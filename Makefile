all:install

GOPATH:=$(CURDIR)/../../../../
export GOPATH
export GO15VENDOREXPERIMENT=1

godep:
	which godep >/dev/null 2>&1 || go get github.com/tools/godep

savedep: godep
	godep save ./...

test:
	go version

install:
	go install github.intra.douban.com/coresys/gobeansproxy/proxy
