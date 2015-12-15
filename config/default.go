package config

import (
	dbcfg "github.intra.douban.com/coresys/gobeansdb/config"
)

var (
	DefaultServerConfig = dbcfg.ServerConfig{
		Hostname:  "127.0.0.1",
		Listen:    "0.0.0.0",
		Port:      7905,
		WebPort:   7908,
		Threads:   8,
		ZK:        "NO",
		LogDir:    "./",
		StaticDir: "/var/lib/gobeansdb",
	}

	DefaultDStoreConfig = DStoreConfig{
		N:                   3,
		W:                   2,
		R:                   1,
		MaxFreeConnsPerHost: 20,
		ConnectTimeoutMs:    300,
		WriteTimeoutMs:      2000,
		DialFailSilenceMs:   5000,
	}
)
