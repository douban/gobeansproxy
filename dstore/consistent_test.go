package dstore

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

// 基本功能。
func TestConsistent(t *testing.T) {
	assert := assert.New(t)

	hashs := NewPartition(100, 3)

	// 计数器
	counter := make(map[int]int)
	loop := 1000000
	threshold := loop / 3 / 10 * 4

	// 统计顺序 key 分布。
	for i := 0; i < loop; i++ {
		s := hashs.offsetGet(fmt.Sprintf("key%d", i))
		counter[s] += 1
	}

	// 检查计数是否超过最低阈值。
	for _, v := range counter {
		assert.True(v > threshold)
	}
}

// 阶段故障
func TestConsistentRemove(t *testing.T) {
	assert := assert.New(t)
	hashs := NewPartition(100, 3)
	hashs.remove(0)
	loop := 1000000
	for i := 0; i < loop; i++ {
		s := hashs.offsetGet(fmt.Sprintf("key%d", i))
		assert.NotEqual(s, 0)
	}
	t.Log(hashs)
	hashs.remove(0)
	t.Log(hashs)
}

func TestConsistentBalance(t *testing.T) {
	assert := assert.New(t)
	hashs := NewPartition(100, 3)
	hashs.reBalance(0, 2, 100)
	assert.Equal(hashs.offsets[2], 99)
}

func TestConsistentMulti(t *testing.T) {
	assert := assert.New(t)

	hashs1 := NewPartition(100, 3)

	hashs2 := NewPartition(100, 3)

	// 多次访问结果一致。
	s1 := hashs1.offsetGet("abc")
	s2 := hashs1.offsetGet("abc")
	assert.Equal(s1, s2)

	s1 = hashs1.offsetGet("abc")
	s2 = hashs2.offsetGet("abc")
	assert.Equal(s1, s2)

}

// 哈希函数性能。
func BenchmarkConsistentHash(b *testing.B) {
	h := NewPartition(100, 3)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h.hash("a" + strconv.Itoa(i))
	}
	b.StopTimer()
}
