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
type Partition struct {
	sync.RWMutex

	count   int
	offsets []int
}

/* --- Partition -------------------------------------------------------------- */
func NewPartition(count int, nodesNum int) *Partition {
	partition := &Partition{
		count:   count,
		offsets: []int{},
	}
	lenNodes := partition.count / nodesNum

	for i := 0; i < nodesNum; i++ {
		partition.offsets = append(partition.offsets, lenNodes*i)
	}
	return partition
}

// 哈希函数。
func (partition *Partition) hash(key string) int {
	hash := fnv.New32a()
	hash.Write([]byte(key))
	return int(hash.Sum32()) % partition.count
}

func (partition *Partition) getPre(index int) (pre int) {
	return (index - 1 + 3) % 3
}

func (partition *Partition) getNext(index int) (next int) {
	return (index + 1 + 3) % 3
}

func (partition *Partition) remove(host int) {
	partition.Lock()
	defer partition.Unlock()
	//TODO 只允许有三个节点
	preIndex := partition.getPre(host)        //(host - 1 + 3) % 3
	offsetsPre := partition.offsets[preIndex] //partition.offsets[preIndex]
	offsetsCount := 0
	if offsetsPre > partition.offsets[host] {
		offsetsCount = (partition.offsets[host] + partition.count - offsetsPre)
	} else {
		offsetsCount = (partition.offsets[host] - offsetsPre)
	}
	prevalue := partition.offsets[preIndex] + (offsetsCount / 2)
	partition.offsets[preIndex] = partition.clearOffset(prevalue)
	partition.offsets[host] = partition.offsets[preIndex]
}

// 获取某一段弧长
func (partition *Partition) getArc(index int) int {
	indexPre := partition.getPre(index)
	arc := partition.offsets[index] - partition.offsets[indexPre]
	if arc < 0 {
		arc += partition.count
	}
	return arc
}

func (partition *Partition) reBalance(indexFrom, indexTo int, step int) {
	partition.Lock()
	defer partition.Unlock()
	// from 节点的下一个节点
	fromNext := partition.getNext(indexFrom) //(indexFrom + 1 + 3) % 3

	if indexTo == fromNext {
		fromPre := partition.getPre(indexFrom) //(indexFrom - 1 + 3) % 3
		step = partition.clearStep(indexFrom, fromPre, step)
		value := partition.offsets[indexFrom] - step
		partition.offsets[indexFrom] = partition.clearOffset(value)
	} else {
		toNext := partition.getNext(indexTo) //(indexTo + 1 + 3) % 3
		step = partition.clearStep(toNext, indexTo, step)
		value := partition.offsets[indexTo] + step
		partition.offsets[indexTo] = partition.clearOffset(value)
	}
}

func (partition *Partition) clearStep(modify, indexPre, step int) int {
	interval := partition.offsets[modify] - partition.offsets[indexPre] - MINKEYS
	if interval < 0 {
		interval += partition.count
	}
	if step > interval {
		step = interval
	}
	return step
}

func (partition *Partition) clearOffset(offset int) int {
	if offset < 0 {
		offset += partition.count
	} else if offset > partition.count {
		offset = offset % partition.count
	}
	return offset
}

// 获取匹配主键。
func (partition *Partition) offsetGet(key string) int {
	partition.RLock()
	defer partition.RUnlock()

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

	index := partition.hash(key)
	// like offset[0] == 98, offset[1] == 32, offset [2] ==66
	// [98, 32, 66]
	// [98, 32, 98]
	// [98, 32, 32]
	if partition.offsets[0] > partition.offsets[1] {
		if index < partition.offsets[1] {
			return 1
		} else if index < partition.offsets[2] {
			return 2
		} else if index < partition.offsets[0] {
			return 0
		} else {
			return 1
		}

		// like offset 0 == 23, offset 1 == 88, offset 2 == 1
		// [23, 88, 1]
		// [32, 88 ,32]
		// [88, 88, 32]
	} else if partition.offsets[2] < partition.offsets[1] {
		if index < partition.offsets[2] {
			return 2
		} else if index < partition.offsets[0] {
			return 0
		} else if index < partition.offsets[1] {
			return 1
		} else {
			return 2
		}

		// offset 0 = 3 ,offset 1 = 34, offset 2 == 67
		// [3, 34, 67]
		// [3, 3, 67]
		// [3, 67, 67]
	} else {
		for i, value := range partition.offsets {
			if index < value {
				return i
			}
		}
		return 0
	}
}
