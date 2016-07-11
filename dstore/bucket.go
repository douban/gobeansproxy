package dstore

import (
	"math"
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
	Id         int
	hostsList  []HostInBucket
	consistent *Consistent
}

type ByName []HostInBucket

func (b ByName) Len() int {
	return len(b)
}

func (b ByName) Swap(i, j int) {
	b[i], b[j] = b[j], b[i]
}

func (b ByName) Less(i, j int) bool {
	return b[i].host.Addr < b[j].host.Addr
}

func newBucket(id int, hosts ...*Host) Bucket {
	var bucket Bucket
	bucket.Id = id
	bucket.hostsList = []HostInBucket{}
	for _, host := range hosts {
		bucket.hostsList = append(bucket.hostsList, newHostInBucket(host))
	}
	sort.Sort(ByName(bucket.hostsList))
	bucket.consistent = NewConsistent(CONSISTENTLEN, len(bucket.hostsList))
	return bucket
}

func newHostInBucket(host *Host) HostInBucket {
	var hib HostInBucket
	hib.host = host
	hib.resTimes = NewRingQueue()
	return hib
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
		var score float64
		// do nothing while the host is down/
		if host.status == false {
			host.oldScore = host.score
			host.score = 0
		} else {
			host.oldScore = host.score
			res := host.resTimes.GetResponses(10)
			// use responseTime and responseCount
			// TODO response.count == 0 ??
			for i, response := range res {
				score += ((response.Sum / float64(response.count)) + float64(response.count)) * math.Pow(0.9, 10-float64(i))
			}
			host.score = score
		}
	}
}

func (bucket *Bucket) balance() {
	fromHost, toHost := bucket.getModify()
	if bucket.hostsList[fromHost].score-bucket.hostsList[toHost].score > 0.5 {
		bucket.consistent.reBalance(fromHost, toHost, 1)
	}
}

func (bucket *Bucket) getModify() (fromHost, toHost int) {
	var maxScore float64
	var minScore float64
	for i, host := range bucket.hostsList {
		// do nothing while the host is down/
		if host.status == false {
			continue
		}
		if minScore == 0 {
			minScore = host.score
			maxScore = host.score
			fromHost = i
			toHost = i
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
	//host := bucket.hostsList[hostIndex]
	// 10 需要可以配置
	errs := host.resTimes.GetErrors(10)
	count := 0
	for _, err := range errs {
		count += err.count
	}
	// 3 需要可以配置
	if count > 3 {
		host.status = false
	} else {
		host.status = true
	}
	return host.status
}

func (bucket *Bucket) addResTime(host string, startTime time.Time, record float64) {
	_, hostBucket := bucket.getHostByAddr(host)
	hostBucket.resTimes.Push(startTime, record)
}

func (bucket *Bucket) addConErr(host string, startTime time.Time, error float64) {
	_, hostBucket := bucket.getHostByAddr(host)
	hostBucket.resTimes.PushErr(startTime, error)
}

func (bucket *Bucket) getHostByAddr(addr string) (int, HostInBucket) {
	for i, host := range bucket.hostsList {
		if host.host.Addr == addr {
			return i, host
		}
	}
	return -1, HostInBucket{}
}

func (bucket *Bucket) downHost(addr string) {
	index, _ := bucket.getHostByAddr(addr)
	bucket.consistent.remove(index)
}
