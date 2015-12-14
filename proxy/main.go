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
	server    *mc.Server
	logger    = loghub.Default
	proxyConf = &config.Proxy
)

func initLog() {
	if proxyConf.LogDir != "" {
		logpath := path.Join(proxyConf.LogDir, "gobeansproxy.log")
		log.Printf("loggging to %s\n", logpath)
		bufferSize := 200
		loghub.SetDefault(logpath, loghub.DEBUG, bufferSize)
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

	proxyConf.InitDefault()
	if *confdir != "" {
		log.Printf("use confdir %s", *confdir)
		proxyConf.Load(*confdir)
	}
	log.Printf("server port: %d, web port: %d", proxyConf.Port, proxyConf.WebPort)

	if *dumpconf {
		config.DumpConfig(proxyConf)
		return
	}

	runtime.GOMAXPROCS(proxyConf.Threads)

	initLog()
	logger.Infof("start gobeansproxy")
	logger.Infof("gobeansproxy version %s starting at %d, config: %#v",
		config.Version, proxyConf.Port, proxyConf)
	logger.Infof("route table: %#v", config.Route)

	startWeb()

	dstore.InitGlobalManualScheduler(config.Route, proxyConf.N)
	storage := new(dstore.Storage)
	addr := fmt.Sprintf("%s:%d", proxyConf.Listen, proxyConf.Port)
	server = mc.NewServer(storage)
	server.Listen(addr)

	logger.Infof("ready")
	log.Printf("ready")

	server.Serve()
}
