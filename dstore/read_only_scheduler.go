package dstore

import (
	"fmt"
	"math"
	"time"
	"sync/atomic"
	dbcfg "github.com/douban/gobeansdb/config"
)

type RRReadScheduler struct {
	hosts []*Host
	current atomic.Int32
	totalHosts int32
	quit bool
}


func NewRRReadScheduler(route *dbcfg.RouteTable) *RRReadScheduler {
	rrsche := new(RRReadScheduler)
	rrsche.hosts = make([]*Host, len(route.Main))
	for idx, server := range route.Main {
		host := NewHost(server.Addr)
		rrsche.hosts[idx] = host
	}
	rrsche.totalHosts = int32(len(rrsche.hosts))
	return rrsche
}

func (sch *RRReadScheduler) GetHostsByKey(key string) (hosts []*Host) {
	next := sch.current.Add(1) % sch.totalHosts
	sch.current.Store(next)
	rrrStoreReqs.WithLabelValues(sch.hosts[next].Addr).Inc()
	return sch.hosts[next:next+1]
}

func (sch *RRReadScheduler) FeedbackError(host *Host, key string, startTime time.Time, errorCode float64) {
	rrrStoreErr.WithLabelValues(host.Addr, fmt.Sprintf("%f", errorCode)).Inc()
	return
}


func (sch *RRReadScheduler) FeedbackLatency(host *Host, key string, startTime time.Time, timeUsed time.Duration) {
	rrrStoreLag.WithLabelValues(host.Addr).Set(float64(timeUsed.Milliseconds()))
	return
}

// route some keys to group of hosts
func (sch *RRReadScheduler) DivideKeysByBucket(keys []string) [][]string {
	numKeysPer := int(math.Round(float64(len(keys)) / float64(len(sch.hosts))))
	rs := make([][]string, len(sch.hosts))
	maxEndIdx := len(keys) - 1

	startIdx := 0
	partIdx := 0
	for {
		endIdx := startIdx + numKeysPer
		if endIdx >= len(keys) || partIdx == maxEndIdx {
			endIdx = len(keys)
			rs[partIdx] = keys[startIdx:endIdx]
			break
		}
		rs[partIdx] = keys[startIdx:endIdx]
		partIdx += 1
		startIdx = endIdx
	}
	return rs
}

// internal status
func (sch *RRReadScheduler) Stats() map[string]map[string]float64 {
	return nil
}

// get latencies of hosts in the bucket
func (sch *RRReadScheduler) LatenciesStats() map[string]map[string][QUEUECAP]Response {
	return nil
}

// get percentage of hosts in the bucket
func (sch *RRReadScheduler) Partition() map[string]map[string]int {
	return nil
}

// return average latency  and arc(percentage)
func (sch *RRReadScheduler) GetBucketInfo(bucketID int64) map[string]map[string]map[string][]Response {
	return nil
}

func (sch *RRReadScheduler) Close() {
	sch.quit = true
}
