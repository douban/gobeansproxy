package dstore

import (
	"hash/fnv"
	"sync"
)

// 一致性哈希变种
type Consistent struct {
	sync.RWMutex

	count   int
	offsets []int
}

/* --- Consistent -------------------------------------------------------------- */
func NewConsistent(count int, nodesNum int) *Consistent {
	consistent := &Consistent{
		count:   count,
		offsets: []int{},
	}
	lenNodes := consistent.count / nodesNum

	for i := 0; i < nodesNum; i++ {
		consistent.offsets = append(consistent.offsets, lenNodes*i)
	}
	return consistent
}

// 哈希函数。
func (this *Consistent) hash(key string) int {
	hash := fnv.New32a()
	hash.Write([]byte(key))
	return int(hash.Sum32()) % this.count
}

func (this *Consistent) remove(host int) {
	preIndex := (host - 1 + 3) % 3
	offsetsPre := this.offsets[preIndex]
	middle := 0
	if offsetsPre > this.offsets[host] {
		middle = (this.offsets[host] + this.count - offsetsPre)
	} else {
		middle = (this.offsets[host] - offsetsPre)
	}
	this.offsets[preIndex] += (middle / 2)
	this.offsets[host] = this.offsets[preIndex]
}

func (this *Consistent) reBalance(indexFrom, indexTo int, offsetsentage int) {
	x := indexFrom - indexTo
	switch x {
	case 1: // node2 -> node1 or node3 -> node2
		this.offsets[indexTo] += offsetsentage
	case -1: // node1 -> node2 or node2 -> node3
		this.offsets[indexFrom] -= offsetsentage
	case 2: // node3 -> node1
		this.offsets[indexFrom] -= offsetsentage
	case -2: // node1 -> node3
		this.offsets[indexFrom] += offsetsentage
	}
}

// 获取匹配主键。
func (this *Consistent) offsetGet(key string) int {

	index := this.hash(key)
	if this.offsets[0] > this.offsets[1] {
		if index < this.offsets[1] {
			return 1
		} else if index < this.offsets[2] {
			return 2
		} else if index < this.offsets[0] {
			return 0
		} else {
			return 1
		}

	} else if this.offsets[2] < this.offsets[1] {
		if index < this.offsets[2] {
			return 2
		} else if index < this.offsets[0] {
			return 0
		} else if index < this.offsets[1] {
			return 1
		} else {
			return 2
		}

	} else {
		for i, value := range this.offsets {
			if index < value {
				return i
			}
		}
		return 0
	}
}
