package dstore

import (
	"hash/fnv"
	"sync"
)

// 一致性哈希变种
type Consistent struct {
	sync.RWMutex

	replicas   int
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
	return int(hash.Sum32()) % 100
}

// 更新主键列表。
func (this *Consistent) update() {
	this.Lock()
	defer this.Unlock()
	//	sort.Ints(this.keys)
	lenNodes := this.replicas / len(this.nodes)

	for i, k := range this.nodes {
		this.percentage[k] = lenNodes*(i+1) - 1
	}
}

// 添加主键。
func (this *Consistent) Add(keys ...string) {

	var nodes []string
	filter := map[string]byte{}

	for _, k := range keys {
		if _, ok := filter[k]; !ok {
			nodes = append(nodes, k)
			filter[k] = 1
		}
	}
	this.nodes = nodes
	this.update()
}

// 调整百分比。
func (this *Consistent) rePercent(hostPer map[string]int) {
	this.Lock()
	defer this.Unlock()
	for node, percent := range hostPer {
		this.percentage[node] = percent
	}
}

func (this *Consistent) reBalance(fromHost, toHost Host, percent int) {

}

// 获取匹配主键。
func (this *Consistent) Get(key string) string {
	this.RLock()

	index := this.hash(key)
	for _, node := range this.nodes {
		if index < this.percentage[node] {
			this.RUnlock()
			return node
		}
	}
	this.RUnlock()
	return this.nodes[0]
}
