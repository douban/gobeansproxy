package config

import (
	dbcfg "github.com/douban/gobeansdb/config"
)

var (
	DefaultServerConfig = dbcfg.ServerConfig{
		Hostname:  "127.0.0.1",
		Listen:    "0.0.0.0",
		Port:      7905,
		WebPort:   7908,
		Threads:   8,
		ZKServers: nil,
		ErrorLog:  "./proxy-error.log",
		AccessLog: "./proxy-access.log",
		StaticDir: "/var/lib/gobeansproxy",
	}

	DefaultDStoreConfig = DStoreConfig{
		N:                   3,
		W:                   2,
		R:                   1,
		MaxFreeConnsPerHost: 20,
		ConnectTimeoutMs:    300,
		WriteTimeoutMs:      2000,
		DialFailSilenceMs:   5000,
		ResTimeSeconds:      10,
		ErrorSeconds:        10,
		MaxConnectErrors:    10,
		ScoreDeviation:      10000, // 10000 Microseconds -> 10 Millisecond
		ItemSizeStats:       4096,
		ResponseTimeMin:     4000,
		Enable:              true,
	}
)
