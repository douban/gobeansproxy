package dstore

import (
	"path"
	"testing"

	"github.com/douban/gobeansproxy/config"
	"github.com/douban/gobeansproxy/utils"
)

func TestHost(t *testing.T) {
	homeDir := utils.GetProjectHomeDir()
	confdir := path.Join(homeDir, "conf")
	proxyConf := &config.Proxy
	proxyConf.Load(confdir)
}
