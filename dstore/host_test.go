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

	host := NewHost("127.0.0.1:7980")
	testStoreClient(t, host)

	noExistsHost := NewHost("127.0.0.1:1234")
	testFailStoreClient(t, noExistsHost)
}
