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
	host     *Host
	lantency *RingQueue
}

type Bucket struct {
	ID        int
	hostsList []*HostInBucket
	partition *Partition
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
	bucket.partition = NewPartition(CONSISTENTLEN, len(bucket.hostsList))
	return bucket
}

func newHostInBucket(host *Host) *HostInBucket {
	return &HostInBucket{
		status:   true,
		score:    0,
		host:     host,
		lantency: NewRingQueue(),
	}
}

// get host by key
func (bucket *Bucket) GetHosts(key string) (hosts []*Host) {
	hostIndex := bucket.partition.offsetGet(key)
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
			host.score = 0
		} else {
			latencies := host.lantency.Get(proxyConf.ResTimeSeconds, latencyData)
			for _, latency := range latencies {
				Sum += latency.Sum
				count += latency.Count
			}
			if count > 0 {
				host.score = Sum / float64(count)
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
		var offsetOld, offsetNew []int
		offsetOld = bucket.partition.offsets
		bucket.partition.reBalance(fromHost, toHost, 1)
		offsetNew = bucket.partition.offsets
		logger.Errorf("bucket %d BALANCE: from host-%s-%d to host-%s-%d, make offsets %v to %v ", bucket.ID, bucket.hostsList[fromHost].host.Addr, fromHost, bucket.hostsList[toHost].host.Addr, toHost, offsetOld, offsetNew)
	}
}

func (bucket *Bucket) needBalance(fromIndex, toIndex int) bool {
	return bucket.roundScore(fromIndex)-bucket.roundScore(toIndex) > proxyConf.ScoreDeviation
}

func (bucket *Bucket) roundScore(hostIndex int) float64 {
	// while score is less than ResponseTimeMin, use ResponseTimeMin
	if v := bucket.hostsList[hostIndex].score; v < proxyConf.ResponseTimeMin {
		return v
	} else {
		return proxyConf.ResponseTimeMin
	}
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

// return false if have too much connection errors
func (bucket *Bucket) isHostAlive(addr string) bool {
	_, host := bucket.getHostByAddr(addr)
	errs := host.lantency.Get(proxyConf.ErrorSeconds, errorData)
	count := 0
	for _, err := range errs {
		count += err.Count
	}
	return count < proxyConf.MaxConnectErrors
}

func (bucket *Bucket) riseHost(addr string) {
	// TODO 清除历史上的 Errors
	// 还需要清除 response time
	// TODO Lock
	_, hostBucket := bucket.getHostByAddr(addr)
	if hostBucket.status == false {
		hostBucket.status = true
		hostBucket.lantency.clear()
	}
}

func (bucket *Bucket) addLatency(host string, startTime time.Time, latency float64) {
	// TODO 每次添加都会排除掉
	_, hostBucket := bucket.getHostByAddr(host)
	if latency > 0 && !hostBucket.isAlive() {
		bucket.riseHost(host)
	}
	hostBucket.lantency.Push(startTime, latency, latencyData)
}

func (bucket *Bucket) addConErr(host string, startTime time.Time, error float64) {
	_, hostBucket := bucket.getHostByAddr(host)
	if hostBucket.isAlive() {
		hostBucket.lantency.Push(startTime, error, errorData)
		hostisalive := bucket.isHostAlive(host)
		if !hostisalive {
			bucket.downHost(host)
			logger.Errorf("host %s is removed from partition", host)
		}
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
	bucket.partition.remove(index)
}

func (hb *HostInBucket) down() {
	hb.status = false
	hb.lantency.clear()
}

func (hb *HostInBucket) isAlive() bool {
	return hb.status
}
