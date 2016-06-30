package dstore

const RINGLEN = 10

type HostInBucket struct {
	Idx      int //host index of Schduler.hosts
	status   bool
	resTimes RingQueue
}

type Bucket struct {
	Id         int
	Hosts      []HostInBucket
	consistent Consistent
}

func newBucket(id int) Bucket {
	var bucket Bucket
	var consistent Consistent
	bucket.Id = id
	bucket.consistent = consistent
	return bucket
}

// add host in bucket
func (bucket *Bucket) AddHost(hostIdx int) {
	hostInBucket := HostInBucket{
		hostIdx,
		true,
		*NewRingQueue(RINGLEN),
	}

	bucket.Hosts = append(bucket.Hosts, hostInBucket)
}

// get host by key
func (bucket *Bucket) GetHosts(key string) (hosts []int) {
	// TODO  通过 consistent 拿到一个物理节点
	_ = bucket.consistent.Get(key)
	hosts[0] = 0
	for _, host := range bucket.Hosts {
		if host.Idx != 0 {
			hosts = append(hosts, host.Idx)
		}
	}
	return
}

// intput map of hostIndex -- percentage
func (bucket *Bucket) Rescore(map[int]int) {

}
