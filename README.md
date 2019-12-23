# GoBeansProxy ![](https://github.com/douban/gobeansproxy/workflows/GoBeansProxy%20Test/badge.svg)

A proxy for [Gobeansdb](https://github.com/douban/gobeansdb).

## Prepare

Supported Go version: > 1.11.0

## Install

```
$ git clone http://github.com/douban/gobeansproxy.git
$ cd gobeansproxy
$ go mod vendor
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
