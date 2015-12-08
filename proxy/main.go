package main

import (
	"flag"
	"fmt"
	"log"
	"path"
	"runtime"

	"github.intra.douban.com/coresys/gobeansdb/loghub"
	mc "github.intra.douban.com/coresys/gobeansdb/memcache"

	"github.intra.douban.com/coresys/gobeansproxy/config"
	"github.intra.douban.com/coresys/gobeansproxy/dstore"
)

var (
	server *mc.Server
	logger = loghub.Default
	conf   = &config.Proxy
)

func initLog() {
	if conf.LogDir != "" {
		logpath := path.Join(conf.LogDir, "gobeansproxy.log")
		log.Printf("loggging to %s\n", logpath)
		bufferSize := 200
		loghub.SetDefault(logpath, loghub.INFO, bufferSize)
	}
}

func main() {
	var version = flag.Bool("version", false, "print vresion of beansproxy")
	var confdir = flag.String("confdir", "", "path of proxy config dir")
	var dumpconf = flag.Bool("dumpconf", false, "print configuration")
	flag.Parse()

	if *version {
		fmt.Println("gobeansproxy version", config.Version)
		return
	} else {
		log.Printf("gobeansproxy version %s", config.Version)
	}

	conf.InitDefault()
	if *confdir != "" {
		log.Printf("use confdir %s", *confdir)
		conf.Load(*confdir)
	}

	if *dumpconf {
		config.DumpConfig(conf)
		return
	}

	runtime.GOMAXPROCS(conf.Threads)

	initLog()
	logger.Infof("gobeansproxy version %s starting at %d, config: %#v",
		config.Version, conf.Port, conf)
	logger.Infof("route table: %#v", config.Route)

	// TODO: start web

	storage := new(dstore.Storage)
	addr := fmt.Sprintf("%s:%d", conf.Listen, conf.Port)
	server = mc.NewServer(storage)
	server.Listen(addr)

	logger.Infof("ready")
	log.Printf("ready")

	server.Serve()
}
