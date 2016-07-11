package dstore

import (
	"fmt"
	"math"
	"math/rand"
	"strconv"
	"time"

	dbcfg "github.intra.douban.com/coresys/gobeansdb/config"
	dbutil "github.intra.douban.com/coresys/gobeansdb/utils"
)

const (
	FeedbackNonConnectErrSet     = -10
	FeedbackNonConnectErrDelete  = -10
	FeedbackConnectErrDefault    = -2
	FeedbackNonConnectErrDefault = -5
)

var (
	globalScheduler Scheduler
)

// Scheduler: route request to nodes
type Scheduler interface {
	// feedback for auto routing
	Feedback(host *Host, key string, startTime time.Time, adjust float64)
	FeedbackTime(host *Host, key string, startTime time.Time, timeUsed time.Duration)

	// route a key to hosts
	GetHostsByKey(key string) []*Host

	// route a key to hosts
	GetConsistentHosts(key string) (hosts []*Host)

	// route some keys to group of hosts
	DivideKeysByBucket(keys []string) [][]string

	// internal status
	Stats() map[string]map[string]float64

	Close()
}

// route request by configure
type ManualScheduler struct {
	N     int
	hosts []*Host

	// buckets[bucket] is a list of host index.
	buckets    [][]int
	bucketsCon []Bucket

	// backups[bucket] is a list of host index.
	backups    [][]int
	backupsCon []Bucket

	// stats[bucket][host_index] is the score.
	stats [][]float64

	hashMethod dbutil.HashMethod

	// bucketWidth: 2^bucketWidth = route.NumBucket
	bucketWidth int

	// 传递 feedback 信息
	feedChan chan *Feedback

	quit bool
}

func GetScheduler() Scheduler {
	return globalScheduler
}

func InitGlobalManualScheduler(route *dbcfg.RouteTable, n int) {
	globalScheduler = NewManualScheduler(route, n)
}

func NewManualScheduler(route *dbcfg.RouteTable, n int) *ManualScheduler {
	sch := new(ManualScheduler)
	sch.N = n
	sch.hosts = make([]*Host, len(route.Servers))
	sch.bucketsCon = make([]Bucket, route.NumBucket)
	sch.backupsCon = make([]Bucket, route.NumBucket)
	sch.stats = make([][]float64, route.NumBucket)

	idx := 0

	bucketHosts := make(map[int][]*Host)
	backupHosts := make(map[int][]*Host)
	for addr, bucketsFlag := range route.Servers {
		host := NewHost(addr)
		host.Index = idx
		sch.hosts[idx] = host
		for bucketNum, mainFlag := range bucketsFlag {
			if mainFlag {
				if len(bucketHosts[bucketNum]) == 0 {
					bucketHosts[bucketNum] = []*Host{host} // append(bucketHosts[bucketNum], host)
				} else {
					bucketHosts[bucketNum] = append(bucketHosts[bucketNum], host)
				}
			} else {
				if len(backupHosts[bucketNum]) == 0 {
					backupHosts[bucketNum] = []*Host{host}
				} else {
					backupHosts[bucketNum] = append(bucketHosts[bucketNum], host)
				}
			}
		}
		idx++
	}
	for bucketNum, hosts := range bucketHosts {
		sch.bucketsCon[bucketNum] = newBucket(bucketNum, hosts...)
	}

	for bucketNum, hosts := range backupHosts {
		sch.backupsCon[bucketNum] = newBucket(bucketNum, hosts...)
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
			if sch.quit {
				logger.Infof("close tryReward goroutine")
				close(sch.feedChan)
				break
			}
			sch.tryReward() //
			sch.tryRebalance()
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

func (sch *ManualScheduler) GetConsistentHosts(key string) (hosts []*Host) {
	bucketNum := getBucketByKey(sch.hashMethod, sch.bucketWidth, key)
	bucket := sch.bucketsCon[bucketNum]
	hosts = make([]*Host, sch.N+len(sch.backupsCon[bucketNum].hostsList))
	hostsCon := bucket.GetHosts(key)
	for i, host := range hostsCon {
		if i < sch.N {
			hosts[i] = host
		}
	}
	// set the backup nodes in pos after main nodes
	for index, host := range sch.backupsCon[bucketNum].hostsList {
		hosts[sch.N+index] = host.host
	}
	return
}

func (sch *ManualScheduler) GetHostsByKey(key string) (hosts []*Host) {
	bucket := getBucketByKey(sch.hashMethod, sch.bucketWidth, key)
	// Get Host By key
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
	host      *Host
	bucket    int
	adjust    float64
	startTime time.Time
}

func (sch *ManualScheduler) Feedback(host *Host, key string, startTime time.Time, adjust float64) {
	bucket := getBucketByKey(sch.hashMethod, sch.bucketWidth, key)
	sch.feedChan <- &Feedback{host: host, bucket: bucket, adjust: adjust}
}

func (sch *ManualScheduler) FeedbackTime(host *Host, key string, startTime time.Time, timeUsed time.Duration) {
	n := timeUsed.Seconds()
	sch.Feedback(host, key, startTime, 1-math.Sqrt(n)*n)
}

func (sch *ManualScheduler) procFeedback() {
	sch.feedChan = make(chan *Feedback, 256)
	for {
		fb, ok := <-sch.feedChan
		if !ok {
			// channel was closed
			logger.Infof("close procFeedback goroutine")
			break
		}
		sch.feedback(fb.host, fb.bucket, fb.startTime, fb.adjust)
	}
}

func (sch *ManualScheduler) feedback(host *Host, bucketNum int, startTime time.Time, adjust float64) {
	bucket := sch.bucketsCon[bucketNum]
	index, _ := bucket.getHostByAddr(host.Addr)
	if index < 0 {
		return
	} else {
		if adjust > 0 {
			bucket.addResTime(host.Addr, startTime, adjust)
		} else {
			bucket.addConErr(host.Addr, startTime, adjust)
			// do somethime while connection is Error
			hostIsAlive := bucket.hostIsAlive(host.Addr)
			if !hostIsAlive {
				bucket.downHost(host.Addr)
			}
		}
	}
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

func (sch *ManualScheduler) tryRebalance() {
	for _, bucket := range sch.bucketsCon {
		bucket.ReBalance()
	}

}

func swap(a []int, j, k int) {
	a[j], a[k] = a[k], a[j]
}

func (sch *ManualScheduler) rewardNode(bucket int, node int, maxReward int) {
	hostIdx := sch.buckets[bucket][node]
	if item, err := sch.hosts[hostIdx].Get("@"); err == nil {
		item.Free()
		var reward float64 = 0.0
		stat := sch.stats[bucket][hostIdx]
		if stat < 0 {
			reward = 0 - stat
		} else {
			reward = float64(rand.Intn(maxReward))
		}
		sch.feedChan <- &Feedback{host: sch.hosts[hostIdx], bucket: bucket, adjust: reward}
	} else {
		logger.Infof(
			"beansdb server %s in Bucket %X's second node Down while try_reward, err is %s",
			sch.hosts[hostIdx].Addr, bucket, err)
	}
}

func (sch *ManualScheduler) DivideKeysByBucket(keys []string) [][]string {
	rs := make([][]string, len(sch.bucketsCon))
	for _, key := range keys {
		b := getBucketByKey(sch.hashMethod, sch.bucketWidth, key)
		rs[b] = append(rs[b], key)
	}
	return rs
}

// Stats return the score of eache addr, it's used in web interface.
// Result structure is { bucket1: {host1: score1, host2: score2, ...}, ... }
func (sch *ManualScheduler) Stats() map[string]map[string]float64 {
	r := make(map[string]map[string]float64, len(sch.buckets))
	for _, bucket := range sch.bucketsCon {
		var bkt string
		if sch.bucketWidth > 4 {
			bkt = fmt.Sprintf("%02x", bucket.Id)
		} else {
			bkt = fmt.Sprintf("%x", bucket.Id)
		}
		r[bkt] = make(map[string]float64, len(bucket.hostsList))
		for _, host := range bucket.hostsList {
			r[bkt][host.host.Addr] = host.score
		}

	}

	return r
}

func (sch *ManualScheduler) Close() {
	sch.quit = true
}
