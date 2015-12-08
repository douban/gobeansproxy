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
	// Feedback(host *Host, key string, adjust float64)

	// route a key to hosts
	// GetHostsByKey(key string) []*Host

	// route some keys to group of hosts
	DivideKeysByBucket(keys []string) [][]string

	// internal status
	Stats() map[string][]float64
}

// // route request by configure by hand
// type ManualScheduler struct {
// 	N           int
// 	hosts       []*Host
// 	buckets     [][]int
// 	backups     [][]int
// 	bucketWidth int
// 	stats       [][]float64
// }

// // the string is a Hex int string, if it start with -, it means serve the bucket as a backup
// func NewManualScheduler(config map[string][]string, numBuckets int, n int) *ManualScheduler {

// 	defer func() {
// 		if r := recover(); r != nil {
// 			ErrorLog.Fatalln("NewManualScheduler panic, maybe node's supporting bucket more than buckets number")
// 		}
// 	}()
// 	c := new(ManualScheduler)
// 	c.hosts = make([]*Host, len(config))
// 	c.buckets = make([][]int, bs)
// 	c.backups = make([][]int, bs)
// 	c.stats = make([][]float64, bs)
// 	c.N = n

// 	no := 0
// 	for addr, serve_to := range config {
// 		host := NewHost(addr)
// 		host.offset = no
// 		c.hosts[no] = host
// 		for _, bucket_str := range serve_to {
// 			if strings.HasPrefix(bucket_str, "-") {
// 				if bucket, e := strconv.ParseInt(bucket_str[1:], 16, 16); e == nil {
// 					//c.buckets[bucket] = append(c.buckets[bucket], no)
// 					c.backups[bucket] = append(c.backups[bucket], no)

// 				} else {
// 					ErrorLog.Println("Parse serving bucket config failed, it was not digital")
// 				}
// 			} else {
// 				if bucket, e := strconv.ParseInt(bucket_str, 16, 16); e == nil {
// 					c.buckets[bucket] = append(c.buckets[bucket], no)
// 				} else {
// 					ErrorLog.Println("Parse serving bucket config failed, it was not digital")
// 				}
// 			}
// 		}
// 		no++
// 	}
// 	// set c.stats according to c.buckets
// 	for b := 0; b < bs; b++ {
// 		c.stats[b] = make([]float64, len(c.hosts))
// 	}
// 	c.hashMethod = fnv1a1
// 	c.bucketWidth = calBitWidth(bs)

// 	go c.procFeedback()
// 	go func() {
// 		for {
// 			c.try_reward()
// 			time.Sleep(5 * 1e9)
// 		}
// 	}()
// 	return c
// }

// func fastdivideKeysByBucket(hash_func HashMethod, bs int, bw int, keys []string) [][]string {
// 	rs := make([][]string, bs)
// 	//bw := calBitWidth(bs)
// 	for _, key := range keys {
// 		b := getBucketByKey(hash_func, bw, key)
// 		rs[b] = append(rs[b], key)
// 	}
// 	return rs
// }

// func (c *ManualScheduler) try_reward() {
// 	//c.dump_scores()
// 	for i, bucket := range c.buckets {
// 		// random raward 2nd, 3rd node
// 		second_node := bucket[1]
// 		if _, err := c.hosts[second_node].Get("@"); err == nil {
// 			var second_reward float64 = 0.0
// 			second_stat := c.stats[i][second_node]
// 			if second_stat < 0 {
// 				second_reward = 0 - second_stat
// 			} else {
// 				second_reward = float64(rand.Intn(10))
// 			}
// 			c.feedChan <- &Feedback{hostIndex: second_node, bucketIndex: i, adjust: second_reward}
// 		} else {
// 			ErrorLog.Printf("beansdb server : %s in Bucket %X's second node Down while try_reward, the err = %s", c.hosts[second_node].Addr, i, err)
// 		}

// 		if c.N > 2 {
// 			third_node := bucket[2]
// 			if _, err := c.hosts[third_node].Get("@"); err == nil {
// 				var third_reward float64 = 0.0
// 				third_stat := c.stats[i][third_node]
// 				if third_stat < 0 {
// 					third_reward = 0 - third_stat
// 				} else {
// 					third_reward = float64(rand.Intn(16))
// 				}
// 				c.feedChan <- &Feedback{hostIndex: third_node, bucketIndex: i, adjust: third_reward}
// 			} else {
// 				ErrorLog.Printf("beansdb server : %s in Bucket %X's third node Down while try_reward, the err = %s", c.hosts[third_node].Addr, i, err)
// 			}
// 		}
// 	}
// }

// func (c *ManualScheduler) procFeedback() {
// 	c.feedChan = make(chan *Feedback, 256)
// 	for {
// 		fb := <-c.feedChan
// 		c.feedback(fb.hostIndex, fb.bucketIndex, fb.adjust)
// 	}
// }

// func (c *ManualScheduler) feedback(i, bucket_index int, adjust float64) {

// 	stats := c.stats[bucket_index]
// 	old := stats[i]
// 	stats[i] += adjust

// 	// try to reduce the bucket's stats
// 	if stats[i] > 100 {
// 		for index := 0; index < len(stats); index++ {
// 			stats[index] = stats[index] / 2
// 		}
// 	}
// 	bucket := make([]int, c.N)
// 	copy(bucket, c.buckets[bucket_index])

// 	k := 0
// 	// find the position
// 	for k = 0; k < c.N; k++ {
// 		if bucket[k] == i {
// 			break
// 		}
// 	}

// 	if stats[i]-old > 0 {
// 		for k > 0 && stats[bucket[k]] > stats[bucket[k-1]] {
// 			swap(bucket, k, k-1)
// 			k--
// 		}
// 	} else {
// 		for k < c.N-1 && stats[bucket[k]] < stats[bucket[k+1]] {
// 			swap(bucket, k, k+1)
// 			k++
// 		}
// 	}
// 	// set it to origin
// 	c.buckets[bucket_index] = bucket
// }

// func (c *ManualScheduler) GetHostsByKey(key string) (hosts []*Host) {
// 	i := getBucketByKey(c.hashMethod, c.bucketWidth, key)
// 	hosts = make([]*Host, c.N+len(c.backups[i]))
// 	for j, offset := range c.buckets[i] {
// 		hosts[j] = c.hosts[offset]
// 	}
// 	// set the backup nodes in pos after N - 1
// 	for j, offset := range c.backups[i] {
// 		hosts[c.N+j] = c.hosts[offset]
// 	}
// 	return
// }

// func (c *ManualScheduler) DivideKeysByBucket(keys []string) [][]string {
// 	return fastdivideKeysByBucket(c.hashMethod, len(c.buckets), c.bucketWidth, keys)
// }

// func (c *ManualScheduler) Feedback(host *Host, key string, adjust float64) {
// 	index := getBucketByKey(c.hashMethod, c.bucketWidth, key)
// 	c.feedChan <- &Feedback{hostIndex: host.offset, bucketIndex: index, adjust: adjust}
// }

// func (c *ManualScheduler) Stats() map[string][]float64 {
// 	r := make(map[string][]float64, len(c.hosts))
// 	for _, h := range c.hosts {
// 		r[h.Addr] = make([]float64, len(c.buckets))
// 	}
// 	for i, st := range c.stats {
// 		for j, w := range st {
// 			r[c.hosts[j].Addr][i] = w
// 		}
// 	}
// 	return r
// }

// type Feedback struct {
// 	hostIndex   int
// 	bucketIndex int
// 	adjust      float64
// }
