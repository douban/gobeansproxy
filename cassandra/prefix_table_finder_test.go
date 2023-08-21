package cassandra

import (
	"fmt"
	"testing"

	"github.com/douban/gobeansproxy/config"
)

var (
	cstarCfgTest = &config.CassandraStoreCfg{
		TableToKeyPrefix: map[string][]string{
			"a": []string{
				"/a",
				"/a/b/c",
				"/d/e/ffff",
				"/d/f/eeee",
			},

			"and": []string{
				"/and/anding",
				"/a/kkkk",
			},
		},
		DefaultTable: "misc",
	}
)

func TestKeyTableFinder(t *testing.T) {
	tree, err := NewKeyTableFinder(cstarCfgTest)
	if err != nil {
		t.Fatalf("init keytable finder err %s", err)
	}


	testData := map[string]string{
		"/a/fff/": "a",
		"/and/anding/kkk/fff": "and",
		"/d/e/ffff/fkljwe": "a",
		"iamnoting": "misc",
		"/a/kkkk/defa": "and",
	}

	for k, v := range testData {
		if tree.GetTableByKey(k) != v {
			t.Fatalf("%s table find err, should be: %s", k, v)
		}
	}
}

func BenchmarkKeyTableFinder(b *testing.B) {
	f, err := NewKeyTableFinder(cstarCfgTest)
	if err != nil {
		b.Failed()
	}

	for n := 0; n < b.N; n++ {
		k := fmt.Sprintf("send_me_toMisc_%d", n)
		m := f.GetTableByKey(k)
		if m != "misc" {
			panic(fmt.Sprintf("expect misc but got: %s, key: %s", m, k))
		}
	}
}
