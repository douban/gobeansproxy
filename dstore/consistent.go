package dstore

import (
	"hash/fnv"
	"sync"
)

const (
	// 最少保留 MINKEYS/count 的 key 在某一个节点上
	MINKEYS = 1
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
func (consistent *Consistent) hash(key string) int {
	hash := fnv.New32a()
	hash.Write([]byte(key))
	return int(hash.Sum32()) % consistent.count
}

func (consistent *Consistent) getPre(index int) (pre int) {
	return (index - 1 + 3) % 3
}

func (consistent *Consistent) getNext(index int) (next int) {
	return (index + 1 + 3) % 3
}

func (consistent *Consistent) remove(host int) {
	consistent.Lock()
	defer consistent.Unlock()
	//TODO 只允许有三个节点
	preIndex := consistent.getPre(host)        //(host - 1 + 3) % 3
	offsetsPre := consistent.offsets[preIndex] //consistent.offsets[preIndex]
	offsetsCount := 0
	if offsetsPre > consistent.offsets[host] {
		offsetsCount = (consistent.offsets[host] + consistent.count - offsetsPre)
	} else {
		offsetsCount = (consistent.offsets[host] - offsetsPre)
	}
	prevalue := consistent.offsets[preIndex] + (offsetsCount / 2)
	consistent.offsets[preIndex] = consistent.clearOffset(prevalue)
	consistent.offsets[host] = consistent.offsets[preIndex]
}

func (consistent *Consistent) reBalance(indexFrom, indexTo int, step int) {
	consistent.Lock()
	defer consistent.Unlock()
	// from 节点的下一个节点
	fromNext := consistent.getNext(indexFrom) //(indexFrom + 1 + 3) % 3

	if indexTo == fromNext {
		fromPre := consistent.getPre(indexFrom) //(indexFrom - 1 + 3) % 3
		step = consistent.clearStep(indexFrom, fromPre, step)
		value := consistent.offsets[indexFrom] - step
		consistent.offsets[indexFrom] = consistent.clearOffset(value)
	} else {
		toNext := consistent.getNext(indexTo) //(indexTo + 1 + 3) % 3
		step = consistent.clearStep(toNext, indexTo, step)
		value := consistent.offsets[indexTo] + step
		consistent.offsets[indexTo] = consistent.clearOffset(value)
	}
}

func (consistent *Consistent) clearStep(modify, indexPre, step int) int {
	interval := consistent.offsets[modify] - consistent.offsets[indexPre] - MINKEYS
	if interval < 0 {
		interval += consistent.count
	}
	if step > interval {
		step = interval
	}
	return step
}

func (consistent *Consistent) clearOffset(offset int) int {
	if offset < 0 {
		offset += consistent.count
	} else if offset > consistent.count {
		offset = offset % consistent.count
	}
	return offset
}

// 获取匹配主键。
func (consistent *Consistent) offsetGet(key string) int {
	consistent.RLock()
	defer consistent.RUnlock()

	//       A    0
	//            |
	//     	   -------
	//     	 /         \   B
	//      /   		\
	//     /             \
	//    |              | -- 1
	//     \             /
	//      \   		/
	//     	 \         /
	//   2--  ---------
	//              C
	//  A: 2<-A->0
	//  B: 0<-B->1
	//  C: 1<-C->2

	index := consistent.hash(key)
	// like offset[0] == 98, offset[1] == 32, offset [2] ==66
	// [98, 32, 66]
	// [98, 32, 98]
	// [98, 32, 32]
	if consistent.offsets[0] > consistent.offsets[1] {
		if index < consistent.offsets[1] {
			return 1
		} else if index < consistent.offsets[2] {
			return 2
		} else if index < consistent.offsets[0] {
			return 0
		} else {
			return 1
		}

		// like offset 0 == 23, offset 1 == 88, offset 2 == 1
		// [23, 88, 1]
		// [32, 88 ,32]
		// [88, 88, 32]
	} else if consistent.offsets[2] < consistent.offsets[1] {
		if index < consistent.offsets[2] {
			return 2
		} else if index < consistent.offsets[0] {
			return 0
		} else if index < consistent.offsets[1] {
			return 1
		} else {
			return 2
		}

		// offset 0 = 3 ,offset 1 = 34, offset 2 == 67
		// [3, 34, 67]
		// [3, 3, 67]
		// [3, 67, 67]
	} else {
		for i, value := range consistent.offsets {
			if index < value {
				return i
			}
		}
		return 0
	}
}
