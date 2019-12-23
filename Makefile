all:install

export PYTHONPATH=.

test:
	./misc/gobeansdb_server.sh start
	go version
	go test github.com/douban/gobeansproxy/config
	go test github.com/douban/gobeansproxy/dstore
	./misc/gobeansdb_server.sh stop

template:
	rm -r /var/lib/gobeansproxy/templates
	cp -r templates /var/lib/gobeansproxy/

pytest:install
	./tests/run_test.sh

install:
	go install ./
