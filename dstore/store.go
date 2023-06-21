package dstore

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/douban/gobeansdb/cmem"
	"github.com/douban/gobeansdb/loghub"
	mc "github.com/douban/gobeansdb/memcache"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/douban/gobeansproxy/cassandra"
	"github.com/douban/gobeansproxy/config"
)

var (
	logger    = loghub.ErrorLogger
	proxyConf = &config.Proxy
)

var (
	// ErrWriteFailed 表示成功写入的节点数小于 StorageClient.W
	ErrWriteFailed = errors.New("write failed")
)

type Storage struct {
}

func (s *Storage) Client() mc.StorageClient {
	return NewStorageClient(proxyConf.N, proxyConf.W, proxyConf.R)
}

// client for gobeansdb
type StorageClient struct {
	// SuccessedTargets is a list of addrs on which the client request was ran
	// successfully.
	SuccessedTargets []string

	// Dynamo NWR model, please refer to Dynamo paper for details.
	N, W, R int

	// reinit by GetScheduler() for each request, i.e. entry of each puplic method
	sched Scheduler

	// cassandra
	cstar *cassandra.CassandraStore
}

func NewStorageClient(n int, w int, r int) (c *StorageClient) {
	c = new(StorageClient)
	c.N = n
	c.W = w
	c.R = r
	if proxyConf.CassandraStoreCfg.ReadEnable || proxyConf.CassandraStoreCfg.WriteEnable {
		cstar, err := cassandra.NewCassandraStore()
		if err != nil {
			panic(err)
		}

		c.cstar = cstar
	}
	return c
}

func (c *StorageClient) GetSuccessedTargets() []string {
	return c.SuccessedTargets
}

func (c *StorageClient) Clean() {
	c.SuccessedTargets = nil
	return
}

func (c *StorageClient) Get(key string) (item *mc.Item, err error) {
	if proxyConf.DStoreConfig.ReadEnable {
		timer := prometheus.NewTimer(cmdReqDurationSeconds.WithLabelValues("get", "beansdb"))
		defer timer.ObserveDuration()
		totalReqs.WithLabelValues("get", "beansdb").Inc()
		c.sched = GetScheduler()

		hosts := c.sched.GetHostsByKey(key)
		cnt := 0
		for _, host := range hosts[:c.N] {
			start := time.Now()
			item, err = host.Get(key)
			if err == nil {
				cnt++
				if item != nil {
					if item.Cap < proxyConf.ItemSizeStats {
						c.sched.FeedbackLatency(host, key, start, time.Now().Sub(start))
					}
					c.SuccessedTargets = []string{host.Addr}
					return
				} else {
					c.SuccessedTargets = append(c.SuccessedTargets, host.Addr)
				}
			} else {
				if isWaitForRetry(err) {
					c.sched.FeedbackError(host, key, start, FeedbackConnectErrDefault)
				} else {
					c.sched.FeedbackError(host, key, start, FeedbackNonConnectErrDefault)
				}
			}
		}

		if cnt >= c.R {
			// because hosts are sorted
			err = nil
		}

		// here is a failure exit
		return
	}

	if proxyConf.CassandraStoreCfg.ReadEnable {
		timer := prometheus.NewTimer(cmdReqDurationSeconds.WithLabelValues("get", "cstar"))
		defer timer.ObserveDuration()
		totalReqs.WithLabelValues("get", "cstar").Inc()
		return c.cstar.Get(key)
	}

	return nil, fmt.Errorf("You must enable at least one read engine for get")
}

func (c *StorageClient) getMulti(keys []string) (rs map[string]*mc.Item, targets []string, err error) {
	
	numKeys := len(keys)
	rs = make(map[string]*mc.Item, numKeys)
	hosts := c.sched.GetHostsByKey(keys[0])
	suc := 0
	for _, host := range hosts[:c.N] {
		start := time.Now()
		r, er := host.GetMulti(keys)
		if er == nil {
			suc += 1
			if r != nil {
				targets = append(targets, host.Addr)
			}

			for k, v := range r {
				rs[k] = v
			}
			if len(rs) == numKeys {
				break
			}

			newKeys := []string{}
			for _, k := range keys {
				if _, ok := rs[k]; !ok {
					newKeys = append(newKeys, k)
				}
			}
			keys = newKeys
			if len(keys) == 0 {
				break // repeated keys
			}
		} else {
			if isWaitForRetry(er) {
				if err == nil {
					err = er
				}
				c.sched.FeedbackError(host, keys[0], start, FeedbackConnectErrDefault)
			} else {
				err = er
				c.sched.FeedbackError(host, keys[0], start, FeedbackNonConnectErrDefault)
			}
		}
	}
	if suc >= c.R {
		err = nil
	}
	return
}

func (c *StorageClient) GetMulti(keys []string) (rs map[string]*mc.Item, err error) {
	if proxyConf.DStoreConfig.ReadEnable {
		timer := prometheus.NewTimer(cmdReqDurationSeconds.WithLabelValues("get", "beansdb"))
		defer timer.ObserveDuration()
		totalReqs.WithLabelValues("getm", "beansdb").Inc()
		c.sched = GetScheduler()
		var lock sync.Mutex
		rs = make(map[string]*mc.Item, len(keys))

		gs := c.sched.DivideKeysByBucket(keys)
		reply := make(chan bool, len(gs))
		for _, ks := range gs {
			if len(ks) > 0 {
				go func(keys []string) {
					r, t, e := c.getMulti(keys)
					if e != nil {
						err = e
					} else {
						for k, v := range r {
							lock.Lock()
							rs[k] = v
							c.SuccessedTargets = append(c.SuccessedTargets, t...)
							lock.Unlock()
						}
					}
					reply <- true
				}(ks)
			} else {
				reply <- true
			}
		}

		// wait for complete
		for range gs {
			<-reply
		}
		return
	}

	if proxyConf.CassandraStoreCfg.ReadEnable {
		timer := prometheus.NewTimer(cmdReqDurationSeconds.WithLabelValues("get", "cstar"))
		defer timer.ObserveDuration()
		totalReqs.WithLabelValues("getm", "cstar").Inc()
		rs, err = c.cstar.GetMulti(keys)
		return
	}

	return nil, fmt.Errorf("You must enable at least one read engine for get multi")
}

func (c *StorageClient) Set(key string, item *mc.Item, noreply bool) (ok bool, err error) {
	defer item.Free()
	if proxyConf.DStoreConfig.WriteEnable {
		timer := prometheus.NewTimer(cmdReqDurationSeconds.WithLabelValues("set", "beansdb"))
		defer timer.ObserveDuration()
		totalReqs.WithLabelValues("set", "beansdb").Inc()

		c.sched = GetScheduler()
		hosts := c.sched.GetHostsByKey(key)
		ok = false
		err = ErrWriteFailed
		if len(hosts) >= c.N {
			mainSuc, mainTargets := c.setConcurrently(hosts[:c.N], key, item, noreply)
			if mainSuc >= c.W {
				ok = true
				err = nil
				c.SuccessedTargets = mainTargets
			} else {
				backupSuc, backupTargets := c.setConcurrently(hosts[c.N:], key, item, noreply)
				if mainSuc+backupSuc >= c.W {
					ok = true
					err = nil
					c.SuccessedTargets = append(mainTargets, backupTargets...)
				}
			}
		}
		cmem.DBRL.SetData.SubSizeAndCount(item.Cap)
	}

	if proxyConf.CassandraStoreCfg.WriteEnable {
		if proxyConf.DStoreConfig.WriteEnable && err != nil {
			return
		}
		timer := prometheus.NewTimer(cmdReqDurationSeconds.WithLabelValues("set", "cstar"))
		defer timer.ObserveDuration()
		totalReqs.WithLabelValues("set", "cstar").Inc()
		ok, err = c.cstar.Set(key, item)
	}

	return
}

// cmdReturnType 只在 setConcurrently 函数中使用，
// 用来在 goroutine 之间传递数据
type cmdReturnType struct {
	host      *Host
	ok        bool
	err       error
	startTime time.Time
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
			start := time.Now()
			ok, err := host.Set(key, item, noreply)
			res := cmdReturnType{host: host, ok: ok, err: err, startTime: start}
			results <- res
		}(host)
	}

	for i := 0; i < len(hosts); i++ {
		res := <-results
		if res.ok {
			suc++
			targets = append(targets, res.host.Addr)
		} else if !isWaitForRetry(res.err) {
			c.sched.FeedbackError(res.host, key, res.startTime, FeedbackNonConnectErrSet)
		}
	}
	return
}

func (c *StorageClient) Append(key string, value []byte) (ok bool, err error) {
	// NOTE: gobeansdb now do not support `append`, this is not tested.
	c.sched = GetScheduler()
	suc := 0
	for i, host := range c.sched.GetHostsByKey(key) {
		start := time.Now()
		if ok, err = host.Append(key, value); err == nil && ok {
			suc++
			c.SuccessedTargets = append(c.SuccessedTargets, host.Addr)
		} else if !isWaitForRetry(err) {
			c.sched.FeedbackError(host, key, start, FeedbackNonConnectErrDefault)
		}

		if suc >= c.W && (i+1) >= c.N {
			// at least try N backends, and succeed W backends
			break
		}
	}
	if suc < c.W {
		ok = false
		err = ErrWriteFailed
	} else {
		ok = true
		err = nil
	}
	return
}

// NOTE: Incr command may has consistency problem
// link: http://github.com/douban/gobeansproxy/issues/7
func (c *StorageClient) Incr(key string, value int) (result int, err error) {
	c.sched = GetScheduler()
	suc := 0
	for i, host := range c.sched.GetHostsByKey(key) {
		r, e := host.Incr(key, value)
		if e != nil {
			err = e
			continue
		}
		// gobeansdb 的 incr 命令的返回值只有一个 int，当其为 0 时表示失败.
		// 但是如果一个 key 的初始值为 0，proxy 就无法区分是否失败了。这里先不管
		// key 初始值为 0 的情况了。
		if r > 0 {
			suc++
			c.SuccessedTargets = append(c.SuccessedTargets, host.Addr)
		}
		if r > result {
			result = r
		}
		if suc >= c.W && (i+1) >= c.N {
			// at least try N backends, and succeed W backends
			break
		}
	}
	// 只要有一个返回成功就返回成功，因为 incr 操作不是可重入的，
	// 总之最好不要用 incr 操作。
	if result > 0 {
		err = nil
	}
	cmem.DBRL.SetData.SubCount(1)
	return
}

// TODO: 弄清楚为什么 delete 不遵循 NWR 规则
func (c *StorageClient) Delete(key string) (flag bool, err error) {
	if proxyConf.DStoreConfig.WriteEnable {
		timer := prometheus.NewTimer(cmdReqDurationSeconds.WithLabelValues("del", "beansdb"))
		defer timer.ObserveDuration()
		totalReqs.WithLabelValues("del", "beansdb").Inc()
		c.sched = GetScheduler()
		suc := 0
		errCnt := 0
		lastErrStr := ""
		failedHosts := make([]string, 0, 2)
		for i, host := range c.sched.GetHostsByKey(key) {
			start := time.Now()
			ok, err := host.Delete(key)
			if ok {
				suc++
				c.SuccessedTargets = append(c.SuccessedTargets, host.Addr)
			} else if err != nil {
				errCnt++
				lastErrStr = err.Error()
				failedHosts = append(failedHosts, host.Addr)
				if i >= c.N {
					continue
				}
				if !isWaitForRetry(err) {
					c.sched.FeedbackError(host, key, start, FeedbackNonConnectErrDelete)
				}
			}

			// TODO: 弄清楚这里为什么不是 suc > c.W
			if suc >= c.N {
				break
			}
		}
		if errCnt > 0 {
			logger.Warnf("key: %s was delete failed in %v, and the last error is %s",
				key, failedHosts, lastErrStr)
		}
		if errCnt < 2 {
			err = nil
		}
		flag = suc > 0
	}

	if proxyConf.CassandraStoreCfg.WriteEnable {
		// when dual write we follow the beansdb principle
		// if bdb write failed we just return and wait for
		// client to retry that
		if proxyConf.DStoreConfig.WriteEnable && err != nil {
			return
		}
		timer := prometheus.NewTimer(cmdReqDurationSeconds.WithLabelValues("del", "cstar"))
		defer timer.ObserveDuration()
		totalReqs.WithLabelValues("del", "cstar").Inc()
		flag, err = c.cstar.Delete(key)
	}

	return
}

func (c *StorageClient) Len() int {
	return 0
}

func (c *StorageClient) Close() {
	return
}

func (c *StorageClient) Process(key string, args []string) (string, string) {
	return "", ""
}

func newItem(flag int, val []byte) *mc.Item {
	item := &mc.Item{Flag: flag}
	length := len(val)
	item.Alloc(length)
	copy(item.CArray.Body, val)
	return item
}
