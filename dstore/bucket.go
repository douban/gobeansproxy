package dstore

import (
	"math"
	"time"
)

const RINGLEN = 60

type HostInBucket struct {
	status   bool
	score    float64
	host     *Host
	resTimes RingQueue
}

type Bucket struct {
	Id         int
	Hosts      map[string]HostInBucket
	consistent *Consistent
}

func newBucket(id int) Bucket {
	var bucket Bucket
	bucket.Id = id
	bucket.consistent = NewConsistent(100)
	bucket.Hosts = make(map[string]HostInBucket)
	return bucket
}

// add host in bucket
func (bucket *Bucket) AddHost(host *Host) {
	logger.Infof("add host %v", host)
	hostInBucket := HostInBucket{
		true,
		0,
		host,
		*NewRingQueue(RINGLEN),
	}
	logger.Infof("host in bucket %v", hostInBucket.host)

	bucket.Hosts[host.Addr] = hostInBucket
	bucket.consistent.Add([]string{host.Addr}...)
}

// get host by key
func (bucket *Bucket) GetHosts(key string) (hosts []*Host) {
	// TODO  通过 consistent 拿到一个物理节点
	hostName := bucket.consistent.Get(key)
	for _, host := range bucket.Hosts {
		if host.host.Addr != hostName {
			hosts = append(hosts, host.host)
		} else {
			hosts = append([]*Host{host.host}, hosts...)
		}
	}
	return
}

// intput map of hostIndex -- percentage
func (bucket *Bucket) Rescore(map[int]int) {

}

func (bucket *Bucket) Score() {
	for addr, host := range bucket.Hosts {
		var score float64
		if host.status == false {
			host.score = 0
			println(addr)
		} else {
			res := host.resTimes.GetResponses(10)
			for i, response := range res {
				score += ((response.Sum / float64(response.count)) + float64(response.count)) * math.Pow(0.9, 10-float64(i))
			}
			host.score = score
		}

	}
}

func (bucket *Bucket) hostIsAlive(addr string) bool {
	host := bucket.Hosts[addr]
	// 10 设置成一个常量
	errs := host.resTimes.GetErrors(10)
	count := 0
	for _, err := range errs {
		count += err.count
	}
	if count > 3 {
		host.status = false
	} else {
		host.status = true
	}
	return host.status
}

func (bucket *Bucket) addResTime(host string, startTime time.Time, record float64) {
	hostBucket := bucket.Hosts[host]
	hostBucket.resTimes.Push(startTime, record)
}

func (bucket *Bucket) addConErr(host string, startTime time.Time, error float64) {
	hostBucket := bucket.Hosts[host]
	hostBucket.resTimes.PushErr(startTime, error)
}
