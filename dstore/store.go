package dstore

import (
	"errors"
	"math"
	"sync"
	"time"

	"github.intra.douban.com/coresys/gobeansdb/loghub"
	mc "github.intra.douban.com/coresys/gobeansdb/memcache"

	"github.intra.douban.com/coresys/gobeansproxy/config"
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

func (c *StorageClient) GetSuccessedTargets() []string {
	return c.SuccessedTargets
}

func (c *StorageClient) Clean() {
	c.SuccessedTargets = nil
	return
}

func (c *StorageClient) Get(key string) (item *mc.Item, err error) {
	hosts := c.scheduler.GetHostsByKey(key)
	cnt := 0
	for _, host := range hosts[:c.N] {
		start := time.Now()
		item, err = host.Get(key)
		if err == nil {
			cnt++
			if item != nil {
				// t 是 Get 命令消耗的时间(单位是秒)
				t := float64(time.Now().Sub(start)) / float64(time.Second)
				// TODO: 这个公式的物理含义
				c.scheduler.Feedback(host, key, 1-float64(math.Sqrt(t)*t))
				c.SuccessedTargets = []string{host.Addr}
				return
			} else {
				c.SuccessedTargets = append(c.SuccessedTargets, host.Addr)
			}
		} else {
			// TODO: 尽量解释下面两个 Magic Number 的含义
			if !isWaitForRetry(err) {
				c.scheduler.Feedback(host, key, -5)
			} else {
				c.scheduler.Feedback(host, key, -2)
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

func (c *StorageClient) getMulti(keys []string) (rs map[string]*mc.Item, targets []string, err error) {
	numKeys := len(keys)
	rs = make(map[string]*mc.Item, numKeys)
	hosts := c.scheduler.GetHostsByKey(keys[0])
	suc := 0
	for _, host := range hosts[:c.N] {
		start := time.Now()
		r, er := host.GetMulti(keys)
		if er == nil {
			suc += 1
			if r != nil {
				targets = append(targets, host.Addr)
				t := float64(time.Now().Sub(start)) / 1e9 // t 的时间单位为秒
				c.scheduler.Feedback(host, keys[0], 1-float64(math.Sqrt(t)*t))
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
			if !isWaitForRetry(er) {
				c.scheduler.Feedback(host, keys[0], -5)
				err = er
			} else {
				if err == nil {
					err = er
				}
				c.scheduler.Feedback(host, keys[0], -2)
			}
		}
	}
	if suc >= c.R {
		err = nil
	}
	return
}

func (c *StorageClient) GetMulti(keys []string) (rs map[string]*mc.Item, err error) {
	var lock sync.Mutex
	rs = make(map[string]*mc.Item, len(keys))

	gs := c.scheduler.DivideKeysByBucket(keys)
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
	for _, _ = range gs {
		<-reply
	}
	return
}

func (c *StorageClient) Set(key string, item *mc.Item, noreply bool) (ok bool, err error) {
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

func (c *StorageClient) Append(key string, value []byte) (ok bool, err error) {
	// NOTE: gobeansdb now do not support `append`, this is not tested.
	suc := 0
	for i, host := range c.scheduler.GetHostsByKey(key) {
		if ok, err = host.Append(key, value); err == nil && ok {
			suc++
			c.SuccessedTargets = append(c.SuccessedTargets, host.Addr)
		} else if !isWaitForRetry(err) {
			c.scheduler.Feedback(host, key, -5)
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
// link: http://github.intra.douban.com/coresys/gobeansproxy/issues/7
func (c *StorageClient) Incr(key string, value int) (result int, err error) {
	suc := 0
	for i, host := range c.scheduler.GetHostsByKey(key) {
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
	return
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
