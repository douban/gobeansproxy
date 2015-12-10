package dstore

import (
	"path"
	"testing"

	mc "github.intra.douban.com/coresys/gobeansdb/memcache"

	"github.com/stretchr/testify/assert"
	"github.intra.douban.com/coresys/gobeansproxy/config"
	"github.intra.douban.com/coresys/gobeansproxy/utils"
)

func newItem(flag int, val []byte) *mc.Item {
	item := &mc.Item{Flag: flag}
	length := len(val)
	item.Alloc(length)
	copy(item.CArray.Body, val)
	return item
}

func freeItem(item *mc.Item) {
	item.Free()
}

func testStoreClient(t *testing.T, c mc.StorageClient) {
	assert := assert.New(t)
	key1 := "/test/client/1"

	r, _ := c.Get(key1)
	assert.Nil(r)

	// set
	key2 := "/test/client/2"
	v := []byte("value")
	flag := 2
	item := newItem(flag, v)
	defer freeItem(item)
	ok, _ := c.Set(key2, item, false)
	assert.True(ok)
}

func testFailStoreClient(t *testing.T, c mc.StorageClient) {
	assert := assert.New(t)
	key := "/test/fail/client"

	_, err := c.Get(key)
	assert.NotNil(err)

	_, err = c.Set("key", &mc.Item{}, false)
	assert.NotNil(err)
}

func TestStore(t *testing.T) {
	homeDir := utils.GetProjectHomeDir()
	confdir := path.Join(homeDir, "conf")
	proxyConf := &config.Proxy
	proxyConf.Load(confdir)

	sch := NewManualScheduler(config.Route, proxyConf.N)
	c := NewStorageClient(sch, proxyConf.N, proxyConf.W, proxyConf.R)

	testStoreClient(t, c)
}
