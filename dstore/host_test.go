package dstore

import (
	"path"
	"testing"

	"github.intra.douban.com/coresys/gobeansproxy/config"
	"github.intra.douban.com/coresys/gobeansproxy/utils"
)

func TestHost(t *testing.T) {
	homeDir := utils.GetProjectHomeDir()
	confdir := path.Join(homeDir, "conf")
	proxyConf := &config.Proxy
	proxyConf.Load(confdir)
}
