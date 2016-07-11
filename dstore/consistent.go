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
	//TODO 只允许有三个节点
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

func (this *Consistent) reBalance(indexFrom, indexTo int, step int) {
	// from 节点的下一个节点
	fromNext := (indexFrom + 1 + 3) % 3
	if indexTo == fromNext {
		fromPre := (indexFrom - 1 + 3) % 3
		step = this.clearStep(indexFrom, fromPre, step)
		value := this.offsets[indexFrom] - step
		this.offsets[indexFrom] = this.clearOffset(value)
	} else {
		toNext := (indexTo + 1 + 3) % 3
		step = this.clearStep(toNext, indexTo, step)
		value := this.offsets[indexTo] + step
		this.offsets[indexTo] = this.clearOffset(value)
	}
}

func (this *Consistent) clearStep(modify, indexPre, step int) int {
	interval := this.offsets[modify] - this.offsets[indexPre]
	if interval < 0 {
		interval += this.count
	}
	if step > interval {
		step = interval
	}
	return step
}

func (this *Consistent) clearOffset(offset int) int {
	if offset < 0 {
		offset += this.count
	} else if offset > this.count {
		offset = offset % this.count
	}
	return offset
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
