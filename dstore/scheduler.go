package dstore

// import (
// 	"math/rand"
// 	"strconv"
// 	"strings"
// 	"time"
// )

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

// route request by configure by hand
type ManualScheduler struct {
	N           int
	hosts       []*Host
	buckets     [][]int
	backups     [][]int
	bucketWidth int
	stats       [][]float64
}

func NewManualScheduler() *ManualScheduler {
	sch := new(ManualScheduler)
	sch.hosts = make([]*Host, 2)
	sch.hosts[0] = NewHost("localhost:7980")
	return sch
}

func (sch *ManualScheduler) GetHostsByKey(key string) []*Host {
	return sch.hosts
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
