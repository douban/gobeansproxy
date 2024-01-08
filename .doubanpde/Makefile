SHELL := /bin/bash
PROJECT_DIR := /home/project

env:
	dpi -y -D "-y"
	mkdir -p /tmp/gobeansproxy_prefix/proxy/

build:
	go build -o gobeansproxy_bin

start-proxy: build
	./gobeansproxy_bin -confdir .doubanpde/scripts/bdb/gobeansproxy/prefix-switch-cfg/conf/

start-riven-proxy: build
	./gobeansproxy_bin -confdir .doubanpde/scripts/bdb/rivenbeansproxy/conf/

start-proxy-gc-trace: build
	GODEBUG=gctrace=1 ./gobeansproxy_bin -confdir .doubanpde/scripts/bdb/gobeansproxy/prefix-switch-cfg/conf/

start-proxy-valgrind: build
	G_SLICE=always-malloc G_DEBUG=gc-friendly  valgrind -v --tool=memcheck --leak-check=full --num-callers=40 --error-limit=no --log-file=valgrind.log ./gobeansproxy_bin -confdir .doubanpde/scripts/bdb/gobeansproxy/prefix-switch-cfg/conf/

tail-log:
	tail -f /tmp/gobeansproxy_prefix/proxy/*.log

cqlsh:
	cqlsh -u cassandra -p cassandra
