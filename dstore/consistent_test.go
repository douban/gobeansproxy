package dstore

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"strconv"
	"testing"
)

// 基本功能。
func TestConsistent(t *testing.T) {
	assert := assert.New(t)
	keys := []string{"192.168.1.1", "192.168.1.2", "192.168.1.3"}

	hashs := NewConsistent(100)
	hashs.Add(keys...)
	// 计数器
	t.Log(hashs.percentage)
	counter := make(map[string]int)
	loop := 1000000
	threshold := loop / len(keys) / 10 * 8

	// 统计顺序 key 分布。
	for i := 0; i < loop; i++ {
		s := hashs.Get(fmt.Sprintf("key%d", i))
		counter[s] += 1
	}

	// 检查计数是否超过最低阈值。
	for _, v := range counter {
		assert.True(v > threshold)
	}
}

func TestConsistentMulti(t *testing.T) {
	assert := assert.New(t)

	keys := []string{"192.168.1.1", "192.168.1.2", "192.168.1.3"}

	hashs1 := NewConsistent(100)
	hashs1.Add(keys...)

	hashs2 := NewConsistent(100)
	hashs2.Add(keys...)

	// 多次访问结果一致。
	s1 := hashs1.Get("abc")
	s2 := hashs1.Get("abc")
	assert.Equal(s1, s2)

	s1 = hashs1.Get("abc")
	s2 = hashs2.Get("abc")
	assert.Equal(s1, s2)

}

// 哈希函数性能。
func BenchmarkConsistentHash(b *testing.B) {
	h := NewConsistent(100)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h.hash("a" + strconv.Itoa(i))
	}
	b.StopTimer()
}
