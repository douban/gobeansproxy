package dstore

import (
	"errors"

	dbcfg "github.intra.douban.com/coresys/gobeansdb/config"
	"github.intra.douban.com/coresys/gobeansdb/loghub"
	mc "github.intra.douban.com/coresys/gobeansdb/memcache"

	"github.intra.douban.com/coresys/gobeansproxy/config"
)

var (
	logger          = loghub.Default
	proxyConf       = &config.Proxy
	routeConf       *dbcfg.RouteTable
	manualScheduler Scheduler
)

var (
	// ErrWriteFailed 表示成功写入的节点数小于 StorageClient.W
	ErrWriteFailed = errors.New("write failed")
)

type Storage struct {
}

func (s *Storage) Client() mc.StorageClient {
	if routeConf == nil {
		routeConf = config.Route
	}
	if manualScheduler == nil {
		manualScheduler = NewManualScheduler(routeConf, proxyConf.N)
	}
	return NewStorageClient(manualScheduler, proxyConf.N, proxyConf.W, proxyConf.R)
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

func (c *StorageClient) Get(key string) (*mc.Item, error) {
	return nil, nil
}

func (c *StorageClient) GetMulti(keys []string) (map[string]*mc.Item, error) {
	return nil, nil
}

func (c *StorageClient) Set(key string, item *mc.Item, noreply bool) (ok bool, err error) {
	logger.Debugf("Set key=%s", key)
	hosts := c.scheduler.GetHostsByKey(key)
	if len(hosts) >= c.N {
		mainSuc, mainTargets := c.setConcurrently(hosts[:c.N], key, item, noreply)
		if mainSuc >= c.W {
			ok = true
			c.SuccessedTargets = mainTargets
			return
		}

		backupSuc, backupTargets := c.setConcurrently(hosts[c.N:], key, item, noreply)
		if mainSuc+backupSuc >= c.W {
			ok = true
			c.SuccessedTargets = append(mainTargets, backupTargets...)
			return
		}
	}
	ok = false
	err = ErrWriteFailed
	return
}

// cmdReturnType 只在 setConcurrently 函数中使用，
// 用来在 goroutine 之间传递数据
type cmdReturnType struct {
	host *Host
	ok   bool
	err  error
}

func (c *StorageClient) setConcurrently(
	hosts []*Host,
	key string,
	item *mc.Item,
	noreply bool,
) (suc int, targets []string) {
	suc = 0
	results := make(chan cmdReturnType, len(hosts))
	for _, host := range hosts {
		go func(host *Host) {
			ok, err := host.Set(key, item, noreply)
			res := cmdReturnType{host: host, ok: ok, err: err}
			results <- res
		}(host)
	}

	for i := 0; i < len(hosts); i++ {
		res := <-results
		if res.ok {
			suc++
			targets = append(targets, res.host.Addr)
		} else if !isWaitForRetry(res.err) {
			c.scheduler.Feedback(res.host, key, -10)
		}
	}
	return
}

func (c *StorageClient) Append(key string, value []byte) (bool, error) {
	return false, nil
}

func (c *StorageClient) Incr(key string, value int) (int, error) {
	return 0, nil
}

func (c *StorageClient) Delete(key string) (bool, error) {
	return false, nil
}

func (c *StorageClient) Len() int {
	return 0
}

func (c *StorageClient) Close() {
}

func (c *StorageClient) Process(key string, args []string) (string, string) {
	return "", ""
}
