package gobeansproxy

import (
	"flag"
	"fmt"
	"log"
	"runtime"

	dbcfg "github.com/douban/gobeansdb/config"
	"github.com/douban/gobeansdb/loghub"
	mc "github.com/douban/gobeansdb/memcache"

	"github.com/douban/gobeansproxy/config"
	"github.com/douban/gobeansproxy/dstore"
)

var (
	server       *mc.Server
	proxyConf    = &config.Proxy
	logger       = loghub.ErrorLogger
	accessLogger = loghub.AccessLogger
)

func Main() {
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

	loghub.InitLogger(proxyConf.ErrorLog, proxyConf.AccessLog, proxyConf.AnalysisLog)
	logger.Infof("start gobeansproxy")
	logger.Infof("gobeansproxy version %s starting at %d, config: %#v",
		config.Version, proxyConf.Port, proxyConf)
	logger.Infof("route table: %#v", config.Route)

	if proxyConf.DStoreConfig.ReadEnable || proxyConf.DStoreConfig.WriteEnable {
		dstore.InitGlobalManualScheduler(config.Route, proxyConf.N)
	}
	storage := new(dstore.Storage)
	err := storage.InitStorageEngine(proxyConf)
	if err != nil {
		log.Fatalf("Init storage engine err: %s", err)
	}
	addr := fmt.Sprintf("%s:%d", proxyConf.Listen, proxyConf.Port)
	server = mc.NewServer(storage)
	server.Listen(addr)

	logger.Infof("ready")
	log.Printf("ready")

	server.HandleSignals(proxyConf.ErrorLog, proxyConf.AccessLog, proxyConf.AnalysisLog)
	dbcfg.AllowReload = true
	startWeb()
	server.Serve()
}
