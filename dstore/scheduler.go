package dstore

import (
	"math/rand"
	"strconv"
	"time"

	dbcfg "github.intra.douban.com/coresys/gobeansdb/config"
	dbutil "github.intra.douban.com/coresys/gobeansdb/utils"
)

var (
	manualScheduler Scheduler
)

// Scheduler: route request to nodes
type Scheduler interface {
	// feedback for auto routing
	Feedback(host *Host, key string, adjust float64)

	// route a key to hosts
	GetHostsByKey(key string) []*Host

	// route some keys to group of hosts
	DivideKeysByBucket(keys []string) [][]string

	// internal status
	Stats() map[int]map[string]float64
}

// route request by configure
type ManualScheduler struct {
	N     int
	hosts []*Host

	// buckets[bucket] is a list of host index.
	buckets [][]int

	// backups[bucket] is a list of host index.
	backups [][]int

	// stats[bucket][host_index] is the score.
	stats [][]float64

	hashMethod dbutil.HashMethod

	// bucketWidth: 2^bucketWidth = route.NumBucket
	bucketWidth int

	// 传递 feedback 信息
	feedChan chan *Feedback
}

func GetScheduler() Scheduler {
	return manualScheduler
}

func InitGlobalManualScheduler(route *dbcfg.RouteTable, n int) {
	manualScheduler = NewManualScheduler(route, n)
}

func NewManualScheduler(route *dbcfg.RouteTable, n int) *ManualScheduler {
	sch := new(ManualScheduler)
	sch.N = n
	sch.hosts = make([]*Host, len(route.Servers))
	sch.buckets = make([][]int, route.NumBucket)
	sch.backups = make([][]int, route.NumBucket)
	sch.stats = make([][]float64, route.NumBucket)

	idx := 0
	for addr, bucketsFlag := range route.Servers {
		host := NewHost(addr)
		host.Index = idx
		sch.hosts[idx] = host
		for bucket, mainFlag := range bucketsFlag {
			if mainFlag {
				sch.buckets[bucket] = append(sch.buckets[bucket], idx)
			} else {
				sch.backups[bucket] = append(sch.backups[bucket], idx)
			}
		}
		idx++
	}

	// set sch.stats according to sch.buckets
	for b := 0; b < route.NumBucket; b++ {
		sch.stats[b] = make([]float64, len(sch.hosts))
	}
	sch.hashMethod = dbutil.Fnv1a
	sch.bucketWidth = calBitWidth(route.NumBucket)

	// scheduler 对各个 host 的打分机制
	go sch.procFeedback()
	go func() {
		for {
			sch.tryReward()
			time.Sleep(5 * time.Second)
		}
	}()

	return sch
}

func calBitWidth(number int) int {
	width := 0
	for number > 1 {
		width++
		number /= 2
	}
	return width
}

func hexToInt(str string) int {
	n, _ := strconv.ParseInt(str, 16, 16)
	return int(n)
}

func getBucketByKey(hashFunc dbutil.HashMethod, bucketWidth int, key string) int {
	hexPathLen := bucketWidth / 4
	if key[0] == '@' && len(key) > hexPathLen {
		return hexToInt(key[1 : 1+hexPathLen])
	}
	if len(key) >= 1 && key[0] == '?' {
		key = key[1:]
	}
	h := hashFunc([]byte(key))
	return (int)(h >> (uint)(32-bucketWidth))
}

func (sch *ManualScheduler) GetHostsByKey(key string) (hosts []*Host) {
	bucket := getBucketByKey(sch.hashMethod, sch.bucketWidth, key)
	hosts = make([]*Host, sch.N+len(sch.backups[bucket]))

	// set the main nodes
	for i, hostIdx := range sch.buckets[bucket] {
		if i < sch.N {
			hosts[i] = sch.hosts[hostIdx]
		}
	}
	// set the backup nodes in pos after main nodes
	for i, hostIdx := range sch.backups[bucket] {
		hosts[sch.N+i] = sch.hosts[hostIdx]
	}
	return
}

// feed back

type Feedback struct {
	hostIndex int
	bucket    int
	adjust    float64
}

func (sch *ManualScheduler) Feedback(host *Host, key string, adjust float64) {
	bucket := getBucketByKey(sch.hashMethod, sch.bucketWidth, key)
	sch.feedChan <- &Feedback{hostIndex: host.Index, bucket: bucket, adjust: adjust}
}

func (sch *ManualScheduler) procFeedback() {
	sch.feedChan = make(chan *Feedback, 256)
	for {
		fb := <-sch.feedChan
		sch.feedback(fb.hostIndex, fb.bucket, fb.adjust)
	}
}

func (sch *ManualScheduler) feedback(hostIndex, bucket int, adjust float64) {
	stats := sch.stats[bucket]
	old := stats[hostIndex]
	stats[hostIndex] += adjust

	// try to reduce the bucket's stats
	if stats[hostIndex] > 100 {
		for i := 0; i < len(stats); i++ {
			stats[i] /= 2
		}
	}

	bucketHosts := make([]int, sch.N)
	copy(bucketHosts, sch.buckets[bucket])

	k := 0
	// find the position
	for k = 0; k < sch.N; k++ {
		if bucketHosts[k] == hostIndex {
			break
		}
	}

	// move the position
	if stats[hostIndex]-old > 0 {
		for k > 0 && stats[bucketHosts[k]] > stats[bucketHosts[k-1]] {
			swap(bucketHosts, k, k-1)
		}
		k--
	} else {
		for k < sch.N-1 && stats[bucketHosts[k]] < stats[bucketHosts[k+1]] {
			swap(bucketHosts, k, k+1)
		}
		k++
	}

	// set it to origin
	sch.buckets[bucket] = bucketHosts
}

func (sch *ManualScheduler) tryReward() {
	for i, _ := range sch.buckets {
		// random reward 2nd, 3rd
		if sch.N > 1 {
			sch.rewardNode(i, 1, 10)
		}
		if sch.N > 2 {
			sch.rewardNode(i, 2, 16)
		}
	}
}

func swap(a []int, j, k int) {
	a[j], a[k] = a[k], a[j]
}

func (sch *ManualScheduler) rewardNode(bucket int, node int, maxReward int) {
	hostIdx := sch.buckets[bucket][node]
	if _, err := sch.hosts[hostIdx].Get("@"); err == nil {
		var reward float64 = 0.0
		stat := sch.stats[bucket][hostIdx]
		if stat < 0 {
			reward = 0 - stat
		} else {
			reward = float64(rand.Intn(maxReward))
		}
		sch.feedChan <- &Feedback{hostIndex: hostIdx, bucket: bucket, adjust: reward}
	} else {
		logger.Infof(
			"beansdb server %s in Bucket %X's second node Down while try_reward, err is %s",
			sch.hosts[hostIdx].Addr, bucket, err)
	}
}

func (sch *ManualScheduler) DivideKeysByBucket(keys []string) [][]string {
	rs := make([][]string, len(sch.buckets))
	for _, key := range keys {
		b := getBucketByKey(sch.hashMethod, sch.bucketWidth, key)
		rs[b] = append(rs[b], key)
	}
	return rs
}

func (sch *ManualScheduler) Stats() map[int]map[string]float64 {
	r := make(map[int]map[string]float64, len(sch.buckets))
	for i, hosts := range sch.buckets {
		r[i] = make(map[string]float64, len(hosts))
		for _, hostIdx := range hosts {
			r[i][sch.hosts[hostIdx].Addr] = sch.stats[i][hostIdx]
		}
	}
	return r
}
