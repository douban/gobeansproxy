package config

import (
	"log"
	"path"

	dbcfg "github.intra.douban.com/coresys/gobeansdb/config"
)

const (
	Version = "0.1"
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
	MaxFreeConnsPerHost int `yaml:",omitempty"`
	ConnectTimeoutMs    int `yaml:",omitempty"`
	DialFailSilenceMs   int `yaml:",omitempty"`
	WriteTimeoutMs      int `yaml:",omitempty"`
}

func (c *ProxyConfig) InitDefault() {
	c.ServerConfig = DefaultServerConfig
	c.MCConfig = dbcfg.DefaultMCConfig
	c.DStoreConfig = DefaultDStoreConfig
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
	}
}

func DumpConfig(config interface{}) {
	dbcfg.DumpConfig(config)
}
