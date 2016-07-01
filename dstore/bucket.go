package dstore

import ()

const RINGLEN = 10

type HostInBucket struct {
	status   bool
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
	hostInBucket := HostInBucket{
		true,
		host,
		*NewRingQueue(RINGLEN),
	}

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

func (bucket *Bucket) addResTime(host string, record float64) {
	hostBucket := bucket.Hosts[host]
	hostBucket.resTimes.Push(record)
}
