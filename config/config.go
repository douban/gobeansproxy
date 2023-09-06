package config

import (
	"log"
	"path"

	dbcfg "github.com/douban/gobeansdb/config"
	dbutils "github.com/douban/gobeansdb/utils"
)

const (
	Version = "v2.0.1-rc2"
)

var (
	Proxy ProxyConfig
	Route *dbcfg.RouteTable
)

type ProxyConfig struct {
	dbcfg.ServerConfig `yaml:"proxy,omitempty"`
	dbcfg.MCConfig     `yaml:"mc,omitempty"`
	DStoreConfig       `yaml:"dstore,omitempty"`
	CassandraStoreCfg  `yaml:"cassandra,omitempty"`
	Confdir string
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
	ResponseTimeMin     float64 `yaml:"response_time_min,omitempty"`
	WriteEnable         bool    `yaml:"enable_write"`
	ReadEnable          bool    `yaml:"enable_read"`
}

type DualWErrCfg struct {
	DumpToDir string `yaml:"dump_to_dir"`
	FName string `yaml:"log_file_name"`
	LoggerLevel string `yaml:"logger_level"`
	RotateSize int `yaml:"rotate_size_mb"`
	Compress bool `yaml:"compress"`
	MaxAges int `yaml:"max_ages"`
	MaxBackups int `yaml:"max_backups"`
}

type CassandraStoreCfg struct {
	ReadEnable bool `yaml:"enable_read"`
	WriteEnable bool `yaml:"enable_write"`
	Hosts []string `yaml:"hosts"`
	DefaultKeySpace string `yaml:"default_key_space"`
	DefaultTable string `yaml:"default_table"`
	CstarTimeoutMs int `yaml:"timeout_ms"`
	CstarConnectTimeoutMs int `yaml:"connect_timeout_ms"`
	CstarWriteTimeoutMs int `yaml:"write_timeout_ms"`
	MaxConnForGetm int `yaml:"max_conn_for_getm"`
	// ref: https://pkg.go.dev/github.com/gocql/gocql?utm_source=godoc#ClusterConfig
	ReconnectIntervalSec int `yaml:"reconnect_interval_sec"`
	RetryNum int `yaml:"retry_num"`
	NumConns int `yaml:"num_conns"`
	Username string `yaml:"username"`
	Password string `yaml:"password"`
	PasswordFile string `yaml:"password_file"`
	Consistency string  `yaml:"consistency,omitempty"`
	TableToKeyPrefix map[string][]string `yaml:"table_to_keyprefix"`
	SwitchToKeyPrefixes map[string][]string `yaml:"switch_to_keyprefixes"`
	SwitchToKeyDefault string `yaml:"default_storage"`
	DualWErrCfg DualWErrCfg `yaml:"dual_write_err_cfg"`
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
			log.Fatalf("fail to load route table: %s", err.Error())
		}

		Route = route
		checkConfig(c, Route)
	}
	c.Confdir = confdir
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
