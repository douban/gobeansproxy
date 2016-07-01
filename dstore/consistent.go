package dstore

import (
	"fmt"
	"hash/fnv"
	"sort"
	"sync"
)

// 一致性哈希变种
type Consistent struct {
	sync.RWMutex

	replicas   int
	keys       []int          // 所有虚拟节点有序列表。
	nodes      []string       // 物理节点列表 (按虚拟节点顺序)。
	percentage map[string]int // nodeIdx: 33 %
}

/* --- Consistent -------------------------------------------------------------- */

func NewConsistent(replicas int) *Consistent {
	return &Consistent{
		replicas:   replicas,
		percentage: make(map[string]int),
	}
}

// 哈希函数。
func (this *Consistent) hash(key string) int {
	hash := fnv.New32a()
	hash.Write([]byte(key))
	return int(hash.Sum32())
}

// 更新主键列表。
func (this *Consistent) update() {
	sort.Ints(this.keys)

	lenNodes := len(this.keys) / len(this.nodes)
	for i, k := range this.nodes {
		this.percentage[k] = lenNodes*(i+1) - 1
	}
}

// 添加主键。
func (this *Consistent) Add(keys ...string) {
	this.Lock()
	defer this.Unlock()

	var nodes []string
	filter := map[string]byte{}

	for _, k := range keys {
		for i := 0; i < this.replicas; i++ {
			h := this.hash(fmt.Sprintf("%s_%d_%s", k, i, k))
			this.keys = append(this.keys, h)
		}
		if _, ok := filter[k]; !ok {
			nodes = append(nodes, k)
			filter[k] = 1
		}
	}
	this.nodes = nodes

	this.update()
}

// 调整百分比。
func (this *Consistent) rePercent(hostPer map[int]int) {
	this.Lock()
	defer this.Unlock()
}

func (this *Consistent) reBalance(fromHost, toHost Host, percent int) {

}

// 获取匹配主键。
func (this *Consistent) Get(key string) string {
	this.RLock()

	index := sort.SearchInts(this.keys, this.hash(key))

	// current
	if index == len(this.keys) {
		index = 0
	}

	for _, node := range this.nodes {
		percentage := this.percentage[node]
		//		fmt.Println(percentage, "1000000000000000000")
		if index < percentage {
			this.RUnlock()
			return node
		}
	}
	this.RUnlock()
	return this.nodes[0]
}
