package dstore

import (
	"github.intra.douban.com/coresys/gobeansdb/loghub"
	mc "github.intra.douban.com/coresys/gobeansdb/memcache"
	"github.intra.douban.com/coresys/gobeansproxy/config"
)

var (
	logger    = loghub.Default
	proxyConf = &config.Proxy
)

type Storage struct {
}

func (s *Storage) Client() mc.StorageClient {
	return new(StorageClient)
}

// client for gobeansdb
type StorageClient struct {
	// SuccessedTargets is a list of addrs on which the client request was ran
	// successfully.
	SuccessedTargets []string

	// Scheduler route request to nodes (beansdb servers)
	scheduler Scheduler

	// Dynamo NWR model, please refer to Dynamo paper for details.
	N, W, R int
}

func NewStorageClient(s Scheduler, n int, w int, r int) (c *StorageClient) {
	c = new(StorageClient)
	c.scheduler = s
	c.N = n
	c.W = w
	c.R = r
	return c
}

func (sc *StorageClient) Get(key string) (*mc.Item, error) {
	return nil, nil
}

func (sc *StorageClient) GetMulti(keys []string) (map[string]*mc.Item, error) {
	return nil, nil
}

func (sc *StorageClient) Set(key string, item *mc.Item, noreply bool) (bool, error) {
	return false, nil
}

func (sc *StorageClient) Append(key string, value []byte) (bool, error) {
	return false, nil
}

func (sc *StorageClient) Incr(key string, value int) (int, error) {
	return 0, nil
}

func (sc *StorageClient) Delete(key string) (bool, error) {
	return false, nil
}

func (sc *StorageClient) Len() int {
	return 0
}

func (sc *StorageClient) Close() {
}

func (sc *StorageClient) Process(key string, args []string) (string, string) {
	return "", ""
}
