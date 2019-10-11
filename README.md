# GoBeansproxy [![Build Status](https://travis-ci.org/douban/gobeansproxy.svg?branch=master)](https://travis-ci.org/douban/gobeansproxy)

A proxy for [Gobeansdb](https://github.com/douban/gobeansdb).

## Prepare

Supported Go version: > 1.11.0

## Install

```
$ git clone http://github.com/douban/gobeansproxy.git
$ cd gobeansproxy
$ make
```

## test

```
$ make test  # unit test
```

## run

```
# Run with conf
$ ${GOPATH}/bin/proxy -confdir pathToConfDir

# Others
$ ${GOPATH}/bin/proxy -h
```
