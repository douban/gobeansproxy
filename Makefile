all:install

export PYTHONPATH=.

.PHONY: test
test:
	go version
	go test github.com/douban/gobeansproxy/config
	go test github.com/douban/gobeansproxy/dstore

template:
	rm -r /var/lib/gobeansproxy/templates
	cp -r templates /var/lib/gobeansproxy/

pytest: install
	./tests/run_test.sh

install:
	go install ./
