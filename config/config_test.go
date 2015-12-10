package config

import (
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.intra.douban.com/coresys/gobeansproxy/utils"
)

func TestLoadConfig(t *testing.T) {
	homeDir := utils.GetProjectHomeDir()
	confdir := path.Join(homeDir, "conf")

	proxyCfg := new(ProxyConfig)
	proxyCfg.Load(confdir)

	assert := assert.New(t)
	assert.Equal("127.0.0.1", proxyCfg.Hostname)
	assert.Equal(7905, proxyCfg.Port)
	assert.Equal(250, proxyCfg.MaxKeyLen)

	assert.Equal(1, proxyCfg.N)
	assert.Equal(1, proxyCfg.R)
	assert.Equal(20, proxyCfg.MaxFreeConnsPerHost)
	assert.Equal(300, proxyCfg.ConnectTimeoutMs)
	assert.Equal(2000, proxyCfg.ReadTimeoutMs)

	assert.Equal("127.0.0.1:7980", Route.Main[0].Addr)
}
