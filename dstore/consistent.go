package dstore

import (
	"hash/fnv"
	"sync"
)

// 一致性哈希变种
type Consistent struct {
	sync.RWMutex

	count   int
	nodes   []string // 物理节点列表 (按虚拟节点顺序)。
	perc    []int
	offsets map[string]int // nodeIdx: 33 %
}

/* --- Consistent -------------------------------------------------------------- */
func NewConsistent(count int) *Consistent {
	return &Consistent{
		count:   count,
		offsets: make(map[string]int),
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
	lenNodes := this.count / len(this.nodes)

	for i, k := range this.nodes {
		this.offsets[k] = lenNodes*(i+1) - 1
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
		this.offsets[node] = percent
	}
}

func (this *Consistent) reBalance(nodeFrom, nodeTo string, percentage int) {
	indexFrom := this.getNode(nodeFrom)
	indexTo := this.getNode(nodeTo)
	x := indexFrom - indexTo
	switch x {
	case 1: // node2 -> node1 or node3 -> node2
		this.perc[indexTo] += percentage
	case -1: // node1 -> node2 or node2 -> node3
		this.perc[indexFrom] -= percentage
	case 2: // node3 -> node1
		this.perc[indexFrom] -= percentage
	case -2: // node1 -> node3
		this.perc[indexFrom] += percentage
	}
}

// 获取匹配主键。
func (this *Consistent) Get(key string) string {
	this.RLock()
	defer this.RUnlock()

	index := this.hash(key)
	for _, node := range this.nodes {
		if index < this.offsets[node] {
			return node
		}
	}
	return this.nodes[0]
}

func (this *Consistent) getNode(node string) (index int) {
	for i, n := range this.nodes {
		if n == node {
			return i
		}
	}
	return
}
