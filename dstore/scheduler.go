package dstore

import (
	"fmt"
	"strconv"
	"time"

	dbcfg "github.com/douban/gobeansdb/config"
	dbutil "github.com/douban/gobeansdb/utils"
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
	FeedbackError(host *Host, key string, startTime time.Time, errorCode float64)
	FeedbackLatency(host *Host, key string, startTime time.Time, timeUsed time.Duration)

	// route a key to hosts
	GetHostsByKey(key string) (hosts []*Host)

	// route some keys to group of hosts
	DivideKeysByBucket(keys []string) [][]string

	// internal status
	Stats() map[string]map[string]float64

	// get latencies of hosts in the bucket
	LatenciesStats() map[string]map[string][QUEUECAP]Response

	// get percentage of hosts in the bucket
	Partition() map[string]map[string]int

	// return average latency  and arc(percentage)
	GetBucketInfo(bucketID int64) map[string]map[string]map[string][]Response

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

	sch.hashMethod = dbutil.Fnv1a
	sch.bucketWidth = calBitWidth(route.NumBucket)

	// scheduler 对各个 host 的打分机制
	go sch.procFeedback()

	go func() {
		for {
			if sch.quit {
				logger.Infof("close balance goroutine")
				//wait for all feedback done
				time.Sleep(10 * time.Second)
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
	addr      string
	bucket    int
	data      float64 //latency or errorCode
	startTime time.Time
}

func (sch *ManualScheduler) Feedback(host *Host, key string, startTime time.Time, data float64) {
	bucket := getBucketByKey(sch.hashMethod, sch.bucketWidth, key)
	sch.feedChan <- &Feedback{addr: host.Addr, bucket: bucket, data: data, startTime: startTime}
}

func (sch *ManualScheduler) FeedbackError(host *Host, key string, startTime time.Time, errorCode float64) {
	sch.Feedback(host, key, startTime, errorCode)
}

func (sch *ManualScheduler) FeedbackLatency(host *Host, key string, startTime time.Time, timeUsed time.Duration) {
	n := timeUsed.Nanoseconds() / 1000 // Nanoseconds to Microsecond
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
		sch.feedback(fb.addr, fb.bucket, fb.startTime, fb.data)
	}
}

func (sch *ManualScheduler) feedback(addr string, bucketNum int, startTime time.Time, data float64) {
	bucket := sch.bucketsCon[bucketNum]
	index, _ := bucket.getHostByAddr(addr)
	if index < 0 {
		logger.Errorf("Got nothing by addr %s", addr)
		return
	} else {
		if data < 0 {
			bucket.addConErr(addr, startTime, data)
		} else {
			bucket.addLatency(addr, startTime, data)
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
		if item, err := hostBucket.host.Get("@"); err == nil {
			item.Free()
			bucket.riseHost(hostBucket.host.Addr)
		} else {
			logger.Infof(
				"beansdb server %s in Bucket %X's Down while check fails , err is %s",
				hostBucket.host.Addr, bucket.ID, err)
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
			bkt = fmt.Sprintf("%02x", bucket.ID)
		} else {
			bkt = fmt.Sprintf("%x", bucket.ID)
		}
		r[bkt] = make(map[string]float64, len(bucket.hostsList))
		for _, host := range bucket.hostsList {
			r[bkt][host.host.Addr] = host.score
		}

	}
	return r
}

func (sch *ManualScheduler) LatenciesStats() map[string]map[string][QUEUECAP]Response {
	r := make(map[string]map[string][QUEUECAP]Response, len(sch.bucketsCon))

	for _, bucket := range sch.bucketsCon {
		var bkt string
		if sch.bucketWidth > 4 {
			bkt = fmt.Sprintf("%02x", bucket.ID)
		} else {
			bkt = fmt.Sprintf("%x", bucket.ID)
		}
		r[bkt] = make(map[string][QUEUECAP]Response, len(bucket.hostsList))
		for _, host := range bucket.hostsList {
			r[bkt][host.host.Addr] = *host.lantency.resData
		}

	}
	return r
}

func (sch *ManualScheduler) Partition() map[string]map[string]int {
	r := make(map[string]map[string]int, len(sch.bucketsCon))

	for _, bucket := range sch.bucketsCon {
		var bkt string
		if sch.bucketWidth > 4 {
			bkt = fmt.Sprintf("%02x", bucket.ID)
		} else {
			bkt = fmt.Sprintf("%x", bucket.ID)
		}
		r[bkt] = make(map[string]int, len(bucket.hostsList))
		for i, host := range bucket.hostsList {
			r[bkt][host.host.Addr] = bucket.partition.getArc(i)
		}

	}
	return r
}

// return addr:score:offset:response
func (sch *ManualScheduler) GetBucketInfo(bucketID int64) map[string]map[string]map[string][]Response {
	bkt := sch.bucketsCon[bucketID]
	r := make(map[string]map[string]map[string][]Response, len(bkt.hostsList))
	for i, hostInBucket := range bkt.hostsList {
		r[hostInBucket.host.Addr] = make(map[string]map[string][]Response)
		score := fmt.Sprintf("%f", hostInBucket.score)
		offset := fmt.Sprintf("%d", bkt.partition.getArc(i))
		r[hostInBucket.host.Addr][score] = map[string][]Response{
			offset: hostInBucket.lantency.Get(proxyConf.ResTimeSeconds, latencyDataType),
		}
	}
	return r
}

func (sch *ManualScheduler) Close() {
	sch.quit = true
}
