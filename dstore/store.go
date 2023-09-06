package dstore

import (
	"errors"
	"fmt"
	"strconv"
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
	// ErrWriteFailed 表示成功写入的节点数小于 StorageClient.W
	ErrWriteFailed = errors.New("write failed")
	promBR string // enable bdb read
	promBW string // enable bdb write
	promCR string // enable cstar read
	promCW string // enable cstar write
	PrefixStorageSwitcher *cassandra.PrefixSwitcher
	PrefixTableFinder *cassandra.KeyTableFinder
)

type Storage struct {
	cstar *cassandra.CassandraStore
	PSwitcher *cassandra.PrefixSwitcher
	dualWErrHandler *cassandra.DualWriteErrorMgr
}

func (s *Storage) InitStorageEngine(pCfg *config.ProxyConfig) error {
	if pCfg.CassandraStoreCfg.ReadEnable || pCfg.CassandraStoreCfg.WriteEnable {
		cstar, err := cassandra.NewCassandraStore(&proxyConf.CassandraStoreCfg)
		if err != nil {
			return err
		}

		s.cstar = cstar

		switcher, err := cassandra.NewPrefixSwitcher(&proxyConf.CassandraStoreCfg)
		if err != nil {
			return err
		}
		s.PSwitcher = switcher
		PrefixStorageSwitcher = switcher
		PrefixTableFinder = cstar.GetPrefixTableFinder()
		dualWErrCfg := pCfg.CassandraStoreCfg.DualWErrCfg
		dualWErrHandler, err := cassandra.NewDualWErrMgr(
			&dualWErrCfg,
			nil,
		)
		if err != nil {
			return err
		}
		s.dualWErrHandler = dualWErrHandler
		logger.Infof("dual write log send to: %s", s.dualWErrHandler.EFile)
	}
	promBR = strconv.FormatBool(pCfg.DStoreConfig.ReadEnable)
	promBW = strconv.FormatBool(pCfg.DStoreConfig.WriteEnable)
	promCR = strconv.FormatBool(pCfg.CassandraStoreCfg.ReadEnable)
	promCW = strconv.FormatBool(pCfg.CassandraStoreCfg.WriteEnable)
	return nil
}

func (s *Storage) Client() mc.StorageClient {
	return NewStorageClient(
		proxyConf.N, proxyConf.W, proxyConf.R,
		s.cstar, s.PSwitcher, s.dualWErrHandler,
	)
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

	// prefix storage switcher
	pswitcher *cassandra.PrefixSwitcher

	// dual write error handler
	dualWErrHandler *cassandra.DualWriteErrorMgr
}

func NewStorageClient(n int, w int, r int,
	cstar *cassandra.CassandraStore,
	pStoreSwitcher *cassandra.PrefixSwitcher,
	dualEHandler *cassandra.DualWriteErrorMgr,
) (c *StorageClient) {
	c = new(StorageClient)
	c.N = n
	c.W = w
	c.R = r
	c.cstar = cstar
	c.pswitcher = pStoreSwitcher
	c.dualWErrHandler = dualEHandler
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
	timer := prometheus.NewTimer(
		cmdE2EDurationSeconds.WithLabelValues("get", promBR, promBW, promCR, promCW),
	)
	defer timer.ObserveDuration()

	bReadEnable, cReadEnable := c.pswitcher.ReadEnabledOn(key)

	if bReadEnable {
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

	if cReadEnable {
		totalReqs.WithLabelValues("get", "cstar").Inc()

		switch key[0] {
		// ref: https://github.com/douban/gobeansdb/wiki/protocol-extention
		// ref: https://github.com/douban/gobeansdb/blob/d06c2ff9fcd4f381c54b260ec64186c93d1a024f/gobeansdb/store.go#L157
		case '?':
			extended := false
			if len(key) > 1 {
				if key[1] == '?' {
					extended = true
					key = key[2:]
				} else {
					key = key[1:]
				}
				if !cassandra.IsValidKeyString(key) {
					return nil, nil
				}
			}
			return c.cstar.GetMeta(key, extended)
		default:
			
			return c.cstar.Get(key)
		}
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
	timer := prometheus.NewTimer(
		cmdE2EDurationSeconds.WithLabelValues("getm", promBR, promBW, promCR, promCW),
	)
	defer timer.ObserveDuration()

	bkeys, ckeys := c.pswitcher.ReadEnableOnKeys(keys)
	rs = make(map[string]*mc.Item, len(keys))
	
	if len(bkeys) > 0 {
		totalReqs.WithLabelValues("getm", "beansdb").Inc()
		c.sched = GetScheduler()
		var lock sync.Mutex

		gs := c.sched.DivideKeysByBucket(bkeys)
		reply := make(chan bool, len(gs))
		for _, ks := range gs {
			if len(ks) > 0 {
				go func(keys []string) {
					r, t, e := c.getMulti(bkeys)
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

		// keys all stored in bdb
		if len(ckeys) == 0 {
			return
		}
	}

	if len(ckeys) > 0 && err == nil {
		totalReqs.WithLabelValues("getm", "cstar").Inc()
		err = c.cstar.GetMulti(ckeys, rs)
		return
	}

	return nil, fmt.Errorf("You must enable at least one read engine for get multi")
}

func (c *StorageClient) Set(key string, item *mc.Item, noreply bool) (ok bool, err error) {
	defer func() {
		item.Free()
	}()
	timer := prometheus.NewTimer(
		cmdE2EDurationSeconds.WithLabelValues("set", promBR, promBW, promCR, promCW),
	)
	defer timer.ObserveDuration()

	bWriteEnable, cWriteEnable := c.pswitcher.WriteEnabledOn(key)

	if bWriteEnable {
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
		if err != nil {
			errorReqs.WithLabelValues("set", "beansdb").Inc()
		}
	}

	if cWriteEnable {
		if bWriteEnable && err != nil {
			return
		}
		totalReqs.WithLabelValues("set", "cstar").Inc()
		if !cassandra.IsValidKeyString(key) {
			return false, nil
		}
		cok, cerr := c.cstar.Set(key, item)
		if cerr != nil {
			errorReqs.WithLabelValues("set", "cstar").Inc()
			logger.Errorf("set on c* failed: %s, key: %s", cerr, key)

			// we only care c* dual write error only when bdb read enabled
			// brwcw -> return bdb result c* error just add to err log
			// bwcrw -> return c* error as final error, if bdb write err, c* write will not exec
			if bWriteEnable {
				errorReqs.WithLabelValues("set", "bcdual").Inc()
				c.dualWErrHandler.HandleErr(key, "set", cerr)

				br, _ := c.pswitcher.ReadEnabledOn(key)
				if br {
					return
				}
			}
		}
		return cok, cerr
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
	if proxyConf.CassandraStoreCfg.WriteEnable {
		return false, fmt.Errorf("cstar store do not support append")
	}
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
	if proxyConf.CassandraStoreCfg.WriteEnable {
		return 0, fmt.Errorf("cstar store do not support incr")
	}
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
	timer := prometheus.NewTimer(
		cmdE2EDurationSeconds.WithLabelValues("del", promBR, promBW, promCR, promCW),
	)
	defer timer.ObserveDuration()

	bWriteEnable, cWriteEnable := c.pswitcher.WriteEnabledOn(key)

	if bWriteEnable {
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
		if err != nil {
			errorReqs.WithLabelValues("del", "beansdb").Inc()
		}
	}

	if cWriteEnable {
		// when dual write we follow the beansdb principle
		// if bdb write failed we just return and wait for
		// client to retry that
		if bWriteEnable && err != nil {
			return
		}
		totalReqs.WithLabelValues("del", "cstar").Inc()
		if !cassandra.IsValidKeyString(key) {
			return false, nil
		}
		cflag, cerr := c.cstar.Delete(key)
		if cerr != nil {
			errorReqs.WithLabelValues("del", "cstar").Inc()
			logger.Errorf("del on c* failed: %s, key: %s", cerr, key)
			if bWriteEnable {
				errorReqs.WithLabelValues("del", "bcdual").Inc()
				c.dualWErrHandler.HandleErr(key, "del", cerr)

				br, _ := c.pswitcher.ReadEnabledOn(key)
				if br {
					return
				}
			}
		}
		return cflag, cerr
	}

	return
}

func (c *StorageClient) Len() int {
	return 0
}

func (c *StorageClient) Close() {
	if proxyConf.CassandraStoreCfg.WriteEnable {
		c.cstar.Close()
	}
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
