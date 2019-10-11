all:install

export PYTHONPATH=.

# FIXME: When this issue is done(https://github.com/golang/go/issues/23965#issuecomment-409232583)
# Determine the compiler and version
COMPILER_HELP := $(shell $(CC) --help | head -n 1)
ifneq (,$(findstring clang,$(COMPILER_HELP)))
    COMPILER = clang
else ifneq (,$(findstring gcc,$(COMPILER_HELP)))
    COMPILER = gcc
else
    COMPILER = unknown
endif

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
	GO111MODULE=on go mod vendor
	CC=$(COMPILER) go install ./
