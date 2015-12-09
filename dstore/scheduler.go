package dstore

// import (
// 	"math/rand"
// 	"strconv"
// 	"strings"
// 	"time"
// )
import (
	"strconv"
)

import (
	dbcfg "github.intra.douban.com/coresys/gobeansdb/config"
	dbutil "github.intra.douban.com/coresys/gobeansdb/utils"
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
	Stats() map[string][]float64
}

// route request by configure
type ManualScheduler struct {
	N     int
	hosts []*Host

	// buckets[bucket] is a list of host index.
	buckets [][]int

	// backups[bucket] is a list of host index.
	backups [][]int

	stats [][]float64

	hashMethod  dbutil.HashMethod
	bucketWidth int
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
	hosts = make([]*Host, sch.N+len(sch.buckets[bucket]))

	// set the main nodes
	for i, hostIdx := range sch.buckets[bucket] {
		hosts[i] = sch.hosts[hostIdx]
	}
	// set the backup nodes in pos after main nodes
	for i, hostIdx := range sch.backups[bucket] {
		hosts[sch.N+i] = sch.hosts[hostIdx]
	}
	return
}

func (sch *ManualScheduler) Feedback(host *Host, key string, adjust float64) {
	return
}

func (sch *ManualScheduler) DivideKeysByBucket(keys []string) [][]string {
	return nil
}

func (sch *ManualScheduler) Stats() map[string][]float64 {
	return nil
}
