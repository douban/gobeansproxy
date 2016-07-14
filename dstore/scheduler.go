package dstore

import (
	"fmt"
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
	GetHostsByKey(key string) (hosts []*Host)

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
	bucketsCon []*Bucket

	// backups[bucket] is a list of host index.
	backupsCon []*Bucket

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
	sch.bucketsCon = make([]*Bucket, route.NumBucket)
	sch.backupsCon = make([]*Bucket, route.NumBucket)
	// sch.stats = make([][]float64, route.NumBucket)

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
	// for b := 0; b < route.NumBucket; b++ {
	// 	sch.stats[b] = make([]float64, len(sch.hosts))
	// }
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
			sch.checkFails() //
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

func (sch *ManualScheduler) GetHostsByKey(key string) (hosts []*Host) {
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

type Feedback struct {
	hostname  string
	bucket    int
	adjust    float64
	startTime time.Time
}

func (sch *ManualScheduler) Feedback(host *Host, key string, startTime time.Time, adjust float64) {
	bucket := getBucketByKey(sch.hashMethod, sch.bucketWidth, key)
	sch.feedChan <- &Feedback{hostname: host.Addr, bucket: bucket, adjust: adjust, startTime: startTime}
}

func (sch *ManualScheduler) FeedbackTime(host *Host, key string, startTime time.Time, timeUsed time.Duration) {
	n := timeUsed.Nanoseconds() / 1000
	sch.Feedback(host, key, startTime, float64(n))
}

func (sch *ManualScheduler) procFeedback() {
	sch.feedChan = make(chan *Feedback, 256)
	for {
		fb, ok := <-sch.feedChan
		if !ok {
			// channel was closed
			break
		}
		sch.feedback(fb.hostname, fb.bucket, fb.startTime, fb.adjust)
	}
}

func (sch *ManualScheduler) feedback(hostname string, bucketNum int, startTime time.Time, adjust float64) {
	bucket := sch.bucketsCon[bucketNum]
	index, _ := bucket.getHostByAddr(hostname)
	if index < 0 {
		return
	} else {
		if adjust > 0 {
			bucket.addResTime(hostname, startTime, adjust)
		} else {
			bucket.addConErr(hostname, startTime, adjust)
		}
	}
}

func (sch *ManualScheduler) checkFails() {
	for _, bucket := range sch.bucketsCon {
		sch.checkFailsForBucket(bucket)
	}
}

func (sch *ManualScheduler) tryRebalance() {
	for _, bucket := range sch.bucketsCon {
		bucket.ReBalance()
	}

}

func (sch *ManualScheduler) checkFailsForBucket(bucket *Bucket) {

	hosts := bucket.hostsList
	for _, hostBucket := range hosts {
		//		start := time.Now()
		if item, err := hostBucket.host.Get("@"); err == nil {
			item.Free()
			bucket.aliveHost(hostBucket.host.Addr)

			//		timeUsed := time.Now().Sub(start)
			//			n := timeUsed.Nanoseconds() / 1000 // to Microsecond
			//			sch.feedChan <- &Feedback{hostname: hostBucket.host.Addr, bucket: bucket, adjust: float64(n), startTime: start}
		} else {
			logger.Infof(
				"beansdb server %s in Bucket %X's second node Down while try_reward, err is %s",
				hostBucket.host.Addr, bucket, err)
		}
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
	r := make(map[string]map[string]float64, len(sch.bucketsCon))
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
	logger.Errorf("stats is %v", r)

	return r
}

func (sch *ManualScheduler) Close() {
	sch.quit = true
}
