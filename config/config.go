package config

import (
	"log"
	"path"

	dbcfg "github.intra.douban.com/coresys/gobeansdb/config"
	dbutils "github.intra.douban.com/coresys/gobeansdb/utils"
)

const (
	Version = "1.0.0.3"
)

var (
	Proxy ProxyConfig
	Route *dbcfg.RouteTable
)

type ProxyConfig struct {
	dbcfg.ServerConfig `yaml:"proxy,omitempty"`
	dbcfg.MCConfig     `yaml:"mc,omitempty"`
	DStoreConfig       `yaml:"dstore,omitempty"`
}

type DStoreConfig struct {
	N                   int     `yaml:",omitempty"`
	W                   int     `yaml:",omitempty"`
	R                   int     `yaml:",omitempty"`
	MaxFreeConnsPerHost int     `yaml:"max_free_conns_per_host,omitempty"`
	ConnectTimeoutMs    int     `yaml:"connect_timeout_ms,omitempty"`
	DialFailSilenceMs   int     `yaml:"dial_fail_silence_ms,omitempty"`
	WriteTimeoutMs      int     `yaml:"write_timeout_ms,omitempty"`
	ReadTimeoutMs       int     `yaml:"read_timeout_ms,omitempty"`
	ResTimeSeconds      int     `yaml:"response_time_seconds,omitempty"`
	ErrorSeconds        int     `yaml:"error_seconds,omitempty"`
	MaxConnectErrors    int     `yaml:"max_connect_errors,omitempty"`
	ScoreDeviation      float64 `yaml:"score_deviation,omitempty"`
	ItemSizeStats       int     `yaml:"item_size_stats,omitempty"`
}

func (c *ProxyConfig) InitDefault() {
	c.ServerConfig = DefaultServerConfig
	c.MCConfig = dbcfg.DefaultMCConfig
	c.DStoreConfig = DefaultDStoreConfig
}

func (c *ProxyConfig) ConfigPackages() {
	dbcfg.ServerConf = c.ServerConfig
	dbcfg.MCConf = c.MCConfig
	dbcfg.Version = Version
}

func (c *ProxyConfig) Load(confdir string) {
	if confdir != "" {
		var err error

		// proxy
		proxyPath := path.Join(confdir, "proxy.yaml")
		if err = dbcfg.LoadYamlConfig(c, proxyPath); err != nil {
			log.Fatalf("bad config %s: %s", proxyPath, err.Error())
		}

		// route
		routePath := path.Join(confdir, "route.yaml")
		var route *dbcfg.RouteTable

		if len(c.ZKServers) > 0 {
			route, err = dbcfg.LoadRouteTableZK(routePath, c.ZKPath, c.ZKServers)
			if err != nil {
				log.Printf("fail to load route table from zk: %s, err: %s", c.ZKPath, err.Error())
			}
		}

		if len(c.ZKServers) == 0 || err != nil {
			route, err = dbcfg.LoadRouteTableLocal(routePath)
		}
		if err != nil {
			log.Fatal("fail to load route table: %s", err.Error())
		}

		Route = route
		checkConfig(c, Route)
	}
	dbutils.InitSizesPointer(c)
	c.ConfigPackages()
}

func checkConfig(proxy *ProxyConfig, route *dbcfg.RouteTable) {
	if route == nil {
		log.Fatal("empty route config")
	}
}

func DumpConfig(config interface{}) {
	dbcfg.DumpConfig(config)
}
