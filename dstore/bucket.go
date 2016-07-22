package dstore

import (
	"sort"
	"time"
)

const RINGLEN = 60
const CONSISTENTLEN = 100

type HostInBucket struct {
	status   bool
	score    float64
	oldScore float64
	host     *Host
	resTimes *RingQueue
}

type Bucket struct {
	ID         int
	hostsList  []*HostInBucket
	consistent *Consistent
}

type ByName []*HostInBucket

func (b ByName) Len() int {
	return len(b)
}

func (b ByName) Swap(i, j int) {
	b[i], b[j] = b[j], b[i]
}

func (b ByName) Less(i, j int) bool {
	return b[i].host.Addr < b[j].host.Addr
}

func newBucket(id int, hosts ...*Host) *Bucket {
	bucket := new(Bucket)
	bucket.ID = id
	bucket.hostsList = []*HostInBucket{}
	for _, host := range hosts {
		bucket.hostsList = append(bucket.hostsList, newHostInBucket(host))
	}
	sort.Sort(ByName(bucket.hostsList))
	bucket.consistent = NewConsistent(CONSISTENTLEN, len(bucket.hostsList))
	return bucket
}

func newHostInBucket(host *Host) *HostInBucket {
	return &HostInBucket{
		status:   true,
		score:    0,
		oldScore: 0,
		host:     host,
		resTimes: NewRingQueue(),
	}
}

// get host by key
func (bucket *Bucket) GetHosts(key string) (hosts []*Host) {
	hostIndex := bucket.consistent.offsetGet(key)
	for i, host := range bucket.hostsList {
		if i != hostIndex {
			hosts = append(hosts, host.host)
		} else {
			hosts = append([]*Host{host.host}, hosts...)
		}
	}
	return
}

func (bucket *Bucket) ReBalance() {
	bucket.reScore()
	bucket.balance()
}

func (bucket *Bucket) reScore() {
	for _, host := range bucket.hostsList {
		var Sum float64
		var count int
		// while the host is down/
		if host.status == false {
			host.oldScore = host.score
			host.score = 0
		} else {
			host.oldScore = host.score
			res := host.resTimes.GetResponses(proxyConf.ResTimeSeconds)
			// use responseTime and responseCount
			for _, response := range res {
				Sum += response.Sum
				count += response.Count
			}
			if count > 0 {
				score := Sum / float64(count)
				if score < proxyConf.ResponseTimeMin {
					host.score = proxyConf.ResponseTimeMin
				} else {
					host.score = Sum / float64(count)
				}
			} else {
				host.score = 0
			}
		}
	}
}

func (bucket *Bucket) balance() {
	fromHost, toHost := bucket.getModify()
	// TODO
	if bucket.needBalance(fromHost, toHost) {
		logger.Errorf("bucket %d consistent is %v", bucket.ID, bucket.consistent.offsets)
		logger.Errorf("bucket %d BALANCE: from host-%s-%d to host-%s-%d ", bucket.ID, bucket.hostsList[fromHost].host.Addr, fromHost, bucket.hostsList[toHost].host.Addr, toHost)
		bucket.consistent.reBalance(fromHost, toHost, 1)
		logger.Errorf("bucket %d consistent is %v", bucket.ID, bucket.consistent.offsets)
	}
}

func (bucket *Bucket) needBalance(fromIndex, toIndex int) bool {
	if bucket.hostsList[fromIndex].score-bucket.hostsList[toIndex].score > proxyConf.ScoreDeviation {
		return true
	}
	return false
}

func (bucket *Bucket) getModify() (fromHost, toHost int) {
	var maxScore float64
	var minScore float64
	count := 0
	for i, host := range bucket.hostsList {
		// do nothing while the host is down/
		if host.status == false {
			continue
		}
		if count == 0 {
			minScore = host.score
			maxScore = host.score
			fromHost = i
			toHost = i
			count++
			continue
		}
		if host.score > maxScore {
			maxScore = host.score
			fromHost = i
		}
		if host.score < minScore {
			minScore = host.score
			toHost = i
		}
	}
	return
}

func (bucket *Bucket) hostIsAlive(addr string) bool {
	_, host := bucket.getHostByAddr(addr)
	errs := host.resTimes.GetErrors(proxyConf.ErrorSeconds)
	count := 0
	for _, err := range errs {
		count += err.Count
	}
	if count > proxyConf.MaxConnectErrors {
		return false
	} else {
		return true
	}
}

func (bucket *Bucket) aliveHost(addr string) {
	// TODO 清除历史上的 Errors
	// 还需要清除 response time
	// TODO Lock
	_, hostBucket := bucket.getHostByAddr(addr)
	if hostBucket.status == false {
		hostBucket.status = true
		hostBucket.resTimes.clear()
	}
}

func (bucket *Bucket) addResTime(host string, startTime time.Time, record float64) {
	// TODO 每次添加都会排除掉
	_, hostBucket := bucket.getHostByAddr(host)
	if record > 0 {
		bucket.aliveHost(host)
	}
	hostBucket.resTimes.Push(startTime, record)
}

func (bucket *Bucket) addConErr(host string, startTime time.Time, error float64) {
	_, hostBucket := bucket.getHostByAddr(host)
	hostBucket.resTimes.PushErr(startTime, error)
	hostisalive := bucket.hostIsAlive(host)
	if !hostisalive {
		bucket.downHost(host)
		logger.Errorf("host %s is removed from consistent", host)
	}
}

func (bucket *Bucket) getHostByAddr(addr string) (int, *HostInBucket) {
	for i, host := range bucket.hostsList {
		if host.host.Addr == addr {
			return i, host
		}
	}
	return -1, &HostInBucket{}
}

func (bucket *Bucket) downHost(addr string) {
	index, host := bucket.getHostByAddr(addr)
	host.down()
	// make suce bucket.hostsList[index].status = false
	bucket.consistent.remove(index)
}

func (hb *HostInBucket) down() {
	hb.status = false
	hb.resTimes.clear()
}
