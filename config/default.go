package config

var (
	DefaultProxyServerConfig = ProxyServerConfig{
		Hostname: "127.0.0.1",
		Listen:   "0.0.0.0",
		Port:     7905,
		WebPort:  7908,
		Threads:  8,
		ZK:       "NO",
		LogDir:   "./",
		N:        3,
		W:        2,
		R:        1,
	}
)
