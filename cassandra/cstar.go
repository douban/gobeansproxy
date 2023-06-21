package cassandra

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/douban/gobeansdb/loghub"
	mc "github.com/douban/gobeansdb/memcache"
	"github.com/douban/gobeansproxy/config"
	"github.com/gocql/gocql"
)

var (
	logger = loghub.ErrorLogger
	proxyConf = &config.Proxy
	selectQ string
	insertQ string
	deleteQ string
)

type CassandraStore struct {
	cluster *gocql.ClusterConfig
	session *gocql.Session
}

func NewCassandraStore() (*CassandraStore, error) {
	cstarCfg := proxyConf.CassandraStoreCfg
	cluster := gocql.NewCluster(cstarCfg.Hosts...)
	cluster.Keyspace = cstarCfg.DefaultKeySpace
	// TODO: Use Quorum in production
	cluster.Consistency = gocql.LocalOne
	session, err := cluster.CreateSession()
	selectQ = fmt.Sprintf(
		"select value from %s.%s where key = ?",
		proxyConf.CassandraStoreCfg.DefaultKeySpace, proxyConf.CassandraStoreCfg.DefaultTable,
	)
	insertQ = fmt.Sprintf(
		"insert into %s.%s (key, value) values (?, ?)",
		proxyConf.CassandraStoreCfg.DefaultKeySpace, proxyConf.CassandraStoreCfg.DefaultTable,
	)
	deleteQ = fmt.Sprintf(
		"delete from %s.%s where key = ?",
		proxyConf.CassandraStoreCfg.DefaultKeySpace, proxyConf.CassandraStoreCfg.DefaultTable,
	)
	if err != nil {
		return nil, err
	} else {
		return &CassandraStore{
			cluster: cluster,
			session: session,
		}, nil
	}
}

func (c *CassandraStore) Close() {
	c.session.Close()
}

func (c *CassandraStore) Get(key string) (*mc.Item, error) {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		time.Duration(proxyConf.CassandraStoreCfg.TimeoutSec) * time.Second,
	)
	defer cancel()

	value := &BDBValue{}
	err := c.session.Query(
		selectQ,
		key).WithContext(ctx).Scan(&value)
	if err != nil {
		return nil, err
	} else {
		return value.ToMCItem(), nil
	}
}

func (c *CassandraStore) GetMulti(keys []string) (map[string]*mc.Item, error) {
	// not using IN for this reason
	// https://stackoverflow.com/questions/26999098/is-the-in-relation-in-cassandra-bad-for-queries
	var wg sync.WaitGroup
	var result map[string]*mc.Item
	
	for _, k := range keys {
		wg.Add(1)

		go func() {
			defer wg.Done()
			item, err := c.Get(k)
			if err == nil {
				result[k] = item
			}
		}()
	}
	wg.Wait()
	return result, nil
}

func (c *CassandraStore) Set(key string, item *mc.Item) (ok bool, err error) {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		time.Duration(proxyConf.CassandraStoreCfg.TimeoutSec) * time.Second,
	)
	defer cancel()

	v := NewBDBValue(item)
	err = c.session.Query(
		insertQ,
		key,
		v,
	).WithContext(ctx).Exec()

	if err != nil {
		logger.Errorf("Set key err: %s || %s", err, insertQ)
		return false, err
	}
	return true, nil
}

func (c *CassandraStore) Delete(key string) (bool, error) {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		time.Duration(proxyConf.CassandraStoreCfg.TimeoutSec) * time.Second,
	)
	defer cancel()

	err := c.session.Query(
		deleteQ,
		key,
	).WithContext(ctx).Exec()

	return err == nil, err
}
