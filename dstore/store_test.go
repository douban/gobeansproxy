package dstore

import (
	"fmt"
	"path"
	"testing"

	mc "github.intra.douban.com/coresys/gobeansdb/memcache"

	"github.com/stretchr/testify/assert"
	"github.intra.douban.com/coresys/gobeansproxy/config"
	"github.intra.douban.com/coresys/gobeansproxy/utils"
)

func testClientSet(t *testing.T, c mc.StorageClient, key string, val []byte) {
	assert := assert.New(t)
	flag := 2
	ok, err := clientSet(c, key, val, flag)
	setHosts := c.GetSuccessedTargets()
	c.Clean()
	assert.True(ok)
	assert.Nil(err)
	assert.True(len(setHosts) > 0)

	v, err := c.Get(key)
	getHosts := c.GetSuccessedTargets()
	c.Clean()
	assert.Equal(val, v.Body)
	assert.Equal(flag, v.Flag)
	assert.Equal(1, len(getHosts))
	assert.True(hasIntersection(setHosts, getHosts))
}

func clientSet(c mc.StorageClient, key string, val []byte, flag int) (bool, error) {
	item := newItem(flag, val)
	defer item.Free()
	noreply := false
	return c.Set(key, item, noreply)
}

func hasIntersection(arr1 []string, arr2 []string) bool {
	for _, i := range arr1 {
		for _, j := range arr2 {
			if i == j {
				return true
			}
		}
	}
	return false
}

func testFailStoreClient(t *testing.T, c mc.StorageClient) {
	assert := assert.New(t)
	key := "/test/fail/client"

	_, err := c.Get(key)
	assert.NotNil(err)

	_, err = c.Set("key", &mc.Item{}, false)
	assert.NotNil(err)

	_, err = c.GetMulti([]string{"key"})
	assert.NotNil(err)
}

func testStoreClient(t *testing.T, c mc.StorageClient) {
	assert := assert.New(t)
	key1 := "/test/client/1"

	r, _ := c.Get(key1)
	assert.Nil(r)
	assert.True(len(c.GetSuccessedTargets()) > 0)
	c.Clean()
	assert.True(len(c.GetSuccessedTargets()) == 0)

	// set
	key2 := "/test/client/2"
	val2 := []byte("value 2")
	testClientSet(t, c, key2, val2)

	key3 := "/test/client/3"
	val3 := []byte("value 3")
	testClientSet(t, c, key3, val3)

	// get multi
	items, _ := c.GetMulti([]string{key1, key2, key3})
	c.Clean()
	assert.Equal(2, len(items))

	keyNum := 100
	keys := make([]string, keyNum)
	flagm := 3
	valm := []byte("value multi")
	for i := 0; i < keyNum; i++ {
		keys[i] = fmt.Sprintf("/test/client/multi_%d", i)
		ok, _ := clientSet(c, keys[i], valm, flagm)
		c.Clean()
		assert.True(ok)
	}
	items, err := c.GetMulti(keys)
	c.Clean()
	assert.Nil(err)
	assert.Equal(keyNum, len(items))

	// large obj
	key4 := "/test/client/4"
	val4 := make([]byte, 1024*1000)
	testClientSet(t, c, key4, val4)

	// incr
	key5 := "/test/client/5"
	v5, _ := c.Incr(key5, 3)
	assert.Equal(3, v5)
	v5, _ = c.Incr(key5, 5)
	assert.Equal(8, v5)

	// delete
	key6 := "/test/client/6"
	val6 := []byte("value 6")
	testClientSet(t, c, key6, val6)
	ok, _ := c.Delete(key6)
	assert.True(ok)
	v6, _ := c.Get(key6)
	assert.Nil(v6)
}

func TestStore(t *testing.T) {
	homeDir := utils.GetProjectHomeDir()
	confdir := path.Join(homeDir, "conf")
	proxyConf := &config.Proxy
	proxyConf.Load(confdir)

	InitGlobalManualScheduler(config.Route, proxyConf.N)
	c := NewStorageClient(proxyConf.N, proxyConf.W, proxyConf.R)

	testStoreClient(t, c)
}
