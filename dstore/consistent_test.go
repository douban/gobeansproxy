package dstore

import (
	"fmt"
	"strconv"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

// 基本功能。
func TestConsistent(t *testing.T) {
	Convey("add/get", t, func() {
		keys := []string{"192.168.1.1", "192.168.1.2", "192.168.1.3"}

		hashs := NewConsistent(100)
		hashs.Add(keys...)
		fmt.Println("keys", hashs.keys)
		for i, key := range hashs.keys {
			fmt.Println(i, "key", key)
		}

		// 计数器
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
			So(v, ShouldBeGreaterThan, threshold)
		}

		Println(counter)
	})

	// Convey("delete", t, func() {
	// 	keys := []string{"a", "d", "z"}

	// 	hashs := NewConsistent(100)
	// 	hashs.Add(keys...)
	// 	hashs.delete("d")

	// 	// 统计顺序 key 分布。
	// 	counter := make(map[string]int)
	// 	for i := 0; i < 10000; i++ {
	// 		s, _ := hashs.Get(fmt.Sprintf("key%d", i))
	// 		counter[s] += 1
	// 	}

	// 	// 阈值检查
	// 	So(counter["a"], ShouldBeGreaterThan, 0)
	// 	So(counter["d"], ShouldBeZeroValue)
	// 	So(counter["z"], ShouldBeGreaterThan, 0)
	// 	keys = []string{"d"}
	// 	hashs.Add(keys...)
	// 	// 统计顺序 key 分布。
	// 	counter = make(map[string]int)
	// 	for i := 0; i < 10000; i++ {
	// 		s, _ := hashs.Get(fmt.Sprintf("key%d", i))
	// 		counter[s] += 1
	// 	}

	// 	// 阈值检查
	// 	So(counter["a"], ShouldBeGreaterThan, 0)
	// 	So(counter["d"], ShouldBeGreaterThan, 0)
	// 	So(counter["z"], ShouldBeGreaterThan, 0)

	// })

	Convey("multi", t, func() {
		keys := []string{"192.168.1.1", "192.168.1.2", "192.168.1.3"}

		hashs1 := NewConsistent(100)
		hashs1.Add(keys...)

		hashs2 := NewConsistent(100)
		hashs2.Add(keys...)

		// 多次访问结果一致。
		s1 := hashs1.Get("abc")
		s2 := hashs1.Get("abc")
		So(s1, ShouldEqual, s2)

		s1 = hashs1.Get("abc")
		s2 = hashs2.Get("abc")
		So(s1, ShouldEqual, s2)
	})

	// Convey("next", t, func() {
	// 	keys := []string{"192.168.1.1", "192.168.1.2", "192.168.1.3"}
	// 	nodes := []string{"192.168.1.1", "192.168.1.3", "192.168.1.2"}

	// 	hashs := NewConsistent(100)
	// 	hashs.Add(keys...)

	// 	Println(hashs.nodes)
	// 	So(hashs.nodes, ShouldResemble, nodes)

	// 	// .1 -> .3
	// 	s, n := hashs.Get("akey")
	// 	So(s, ShouldEqual, keys[0])
	// 	So(n, ShouldEqual, nodes[1])

	// 	// .3 -> .2
	// 	s, n = hashs.Get("bkey")
	// 	So(s, ShouldEqual, keys[2])
	// 	So(n, ShouldEqual, nodes[2])

	// 	// .2 -> .1
	// 	s, n = hashs.Get("xkey")
	// 	So(s, ShouldEqual, keys[1])
	// 	So(n, ShouldEqual, nodes[0])

	// 	hashs.delete(keys[1])

	// 	// .1 -> .3
	// 	s, n = hashs.Get("xkey")
	// 	So(s, ShouldEqual, keys[0])
	// 	So(n, ShouldEqual, nodes[1])

	// 	// .3 -> .1
	// 	s, n = hashs.Get("bkey")
	// 	So(s, ShouldEqual, keys[2])
	// 	So(n, ShouldEqual, nodes[0])
	// })
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
