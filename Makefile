all:install

GOPATH:=$(CURDIR)/../../../../
export GOPATH
export PYTHONPATH=.

test:
	./misc/gobeansdb_server.sh start
	go version
	vgo test github.com/douban/gobeansproxy/config
	vgo test github.com/douban/gobeansproxy/dstore
	./misc/gobeansdb_server.sh stop

template:
	rm -r /var/lib/gobeansproxy/templates
	cp -r templates /var/lib/gobeansproxy/

pytest:install
	./tests/run_test.sh

install:
	vgo install github.com/douban/gobeansproxy
