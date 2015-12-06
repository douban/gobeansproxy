package config

import (
	"log"
	"path"

	dbcfg "github.intra.douban.com/coresys/gobeansdb/config"
)

const (
	Version = 0.1
)

var (
	Proxy ProxyConfig
	Route *dbcfg.RouteTable
)

type ProxyConfig struct {
	ProxyServerConfig `yaml:"proxy,omitempty"`
	dbcfg.MCConfig    `yaml:"mc,omitempty"`
}

type ProxyServerConfig struct {
	Hostname string `yaml:",omitempty"`
	ZK       string `yaml:",omitempty"` // e.g. "zk1:2100"
	Listen   string `yaml:",omitempty"` // ip
	Port     int    `yaml:",omitempty"`
	WebPort  int    `yaml:",omitempty"`
	Threads  int    `yaml:",omitempty"` // NumCPU
	LogDir   string `yaml:",omitempty"`
	N        int    `yaml:",omitempty"`
	W        int    `yaml:",omitempty"`
	R        int    `yaml:",omitempty"`
}

func (c *ProxyConfig) InitDefault() {
	c.ProxyServerConfig = DefaultProxyServerConfig
	c.MCConfig = dbcfg.DefaultMCConfig

}

func (c *ProxyConfig) Load(confdir string) {
	c.InitDefault()

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
