package config

import (
	"log"
	"path"

	dbcfg "github.intra.douban.com/coresys/gobeansdb/config"
	dbutils "github.intra.douban.com/coresys/gobeansdb/utils"
)

const (
	Version = "1.0.0.0"
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
	N                   int `yaml:",omitempty"`
	W                   int `yaml:",omitempty"`
	R                   int `yaml:",omitempty"`
	MaxFreeConnsPerHost int `yaml:"max_free_conns_per_host,omitempty"`
	ConnectTimeoutMs    int `yaml:"connect_timeout_ms,omitempty"`
	DialFailSilenceMs   int `yaml:"dial_fail_silence_ms,omitempty"`
	WriteTimeoutMs      int `yaml:"write_timeout_ms,omitempty"`
	ReadTimeoutMs       int `yaml:"read_timeout_ms,omitempty"`
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
		var f string

		// proxy
		f = path.Join(confdir, "proxy.yaml")
		if err := dbcfg.LoadYamlConfig(c, f); err != nil {
			log.Fatalf("bad config %s: %s", f, err.Error())
		}

		// route
		f = path.Join(confdir, "route.yaml")
		if route, err := dbcfg.LoadRouteTable(f, c.ZK); err != nil {
			log.Fatalf("bad config %s: %s", f, err.Error())
		} else {
			Route = route
		}
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
