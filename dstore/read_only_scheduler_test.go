package dstore

import (
	"testing"

	dbcfg "github.com/douban/gobeansdb/config"
	"github.com/stretchr/testify/assert"
)


func TestDivideKeyByHosts(t *testing.T) {
	route := new(dbcfg.RouteTable)
	route.Main = append(
		route.Main, dbcfg.Server{Addr: "127.0.0.1:7700"},
		dbcfg.Server{Addr: "127.0.0.1:7701"}, dbcfg.Server{Addr: "127.0.0.1:7702"},
	)
	InitGlobalManualScheduler(route, 1, NoBucketsRounRobinROSchduler)

	rrKeyHostCnt := map[string]int{
		"127.0.0.1:7700": 0,
		"127.0.0.1:7701": 0,
		"127.0.0.1:7702": 0,
	}
	for i := 1; i < 100; i++ {
		testKeys := []string{}
		for j := 0; j < i; j++ {
			hosts := globalScheduler.GetHostsByKey("j")
			assert.True(t, len(hosts) == 1, "rrr scheduler only return one host for one key")
			rrKeyHostCnt[hosts[0].Addr] += 1
			testKeys = append(testKeys, "")
		}
		result := globalScheduler.DivideKeysByBucket(testKeys)
		assert.Equal(t, len(route.Main), len(result), "keys should be split part max")
		totalK := 0
		for _, k := range result {
			totalK += len(k)
		}
		assert.Equal(t, len(testKeys), totalK, "all key must parted")
		assert.True(t, len(testKeys[len(testKeys)-1]) - len(testKeys[0]) < 3, "keys cap diff should less than server nums")
	}
	assert.True(t, rrKeyHostCnt["127.0.0.1:7700"] - rrKeyHostCnt["127.0.0.1:7701"] < 3, "rr should be balanced")
	assert.True(t, rrKeyHostCnt["127.0.0.1:7700"] - rrKeyHostCnt["127.0.0.1:7702"] < 3, "rr should be balanced")
}
