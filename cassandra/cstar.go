package cassandra

import (
	"context"
	"fmt"
	"sync"
	"time"
	"unicode"

	"github.com/douban/gobeansdb/loghub"
	mc "github.com/douban/gobeansdb/memcache"
	"github.com/douban/gobeansproxy/config"
	"github.com/gocql/gocql"
	"golang.org/x/sync/errgroup"
)

const (
	MAX_KEY_LEN = 250
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

func NewCassandraStore(cstarCfg *config.CassandraStoreCfg) (*CassandraStore, error) {
	cluster := gocql.NewCluster(cstarCfg.Hosts...)
	if cstarCfg.Username != "" {
		cluster.Authenticator = gocql.PasswordAuthenticator{
			Username: cstarCfg.Username,
			Password: cstarCfg.Password,
		}
	}
	cluster.Keyspace = cstarCfg.DefaultKeySpace
	cluster.Consistency = gocql.Quorum
	cluster.ReconnectInterval = time.Duration(cstarCfg.ReconnectIntervalSec) * time.Second
	cluster.RetryPolicy = &gocql.SimpleRetryPolicy{NumRetries: cstarCfg.RetryNum}
	cluster.Timeout = time.Duration(cstarCfg.CstarTimeoutMs) * time.Millisecond
	cluster.ConnectTimeout = time.Duration(cstarCfg.CstarConnectTimeoutMs) * time.Millisecond
	cluster.WriteTimeout = time.Duration(cstarCfg.CstarWriteTimeoutMs) * time.Millisecond
	cluster.NumConns = cstarCfg.NumConns
	// cluster.SocketKeepalive = 600 * time.Second
	session, err := cluster.CreateSession()
	selectQ = fmt.Sprintf(
		"select value from %s.%s where key = ?",
		cstarCfg.DefaultKeySpace, cstarCfg.DefaultTable,
	)
	insertQ = fmt.Sprintf(
		"insert into %s.%s (key, value) values (?, ?)",
		cstarCfg.DefaultKeySpace, cstarCfg.DefaultTable,
	)
	deleteQ = fmt.Sprintf(
		"delete from %s.%s where key = ?",
		cstarCfg.DefaultKeySpace, cstarCfg.DefaultTable,
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
	value := &BDBValue{}
	query := c.session.Query(
		selectQ,
		key)
	defer query.Release()
	err := query.Scan(&value)
	if err == gocql.ErrNotFound {
		// https://github.com/douban/gobeansdb/blob/master/memcache/protocol.go#L499
		// just return nil for not found
		return nil, nil
	}

	if err != nil {
		return nil, err
	} else {
		return value.ToMCItem(), nil
	}
}

func (c *CassandraStore) GetMulti(keys []string) (map[string]*mc.Item, error) {
	// not using IN for this reason
	// https://stackoverflow.com/questions/26999098/is-the-in-relation-in-cassandra-bad-for-queries

	result := map[string]*mc.Item{}
	lock := sync.Mutex{}

	ctx := context.Background()
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(proxyConf.CassandraStoreCfg.MaxConnForGetm)

	for _, key := range keys {
		key := key // https://golang.org/doc/faq#closures_and_goroutines
		g.Go(func() error {
			item, err := c.Get(key)
			if item != nil {
				lock.Lock()
				defer lock.Unlock()
				result[key] = item
			} else {
				if err != nil {
					return err
				}
				// if item is nil, must be not found, we don't care
				return nil
			}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		logger.Errorf("getm %s err: %s", keys, err)
	}
	
	return result, nil
}

func (c *CassandraStore) Set(key string, item *mc.Item) (ok bool, err error) {
	v := NewBDBValue(item)
	query := c.session.Query(
		insertQ,
		key,
		v,
	)
	defer query.Release()
	err = query.Exec()

	if err != nil {
		logger.Debugf("Set key %s err: %s", key, err)
		return false, err
	}
	return true, nil
}

func (c *CassandraStore) Delete(key string) (bool, error) {
	query := c.session.Query(
		deleteQ,
		key,
	)
	defer query.Release()
	err := query.Exec()

	return err == nil, err
}

func (c *CassandraStore) GetMeta(key string, extended bool) (*mc.Item, error) {
	item, err := c.Get(key)
	if err != nil {
		return nil, err
	}

	if item == nil {
		return nil, err
	}

	// we fake beansdb metadata
	// in douban-beansdb those data used to check if records exists
	var body string
	if extended {
		body = fmt.Sprintf(
			"%d %d %d %d %d %d %d",
			1, 0, item.Flag, len(item.Body), item.ReceiveTime.Unix(), 0, 0,
		)
	} else {
		body = fmt.Sprintf(
			"%d %d %d %d %d",
			1, 0, item.Flag, len(item.Body), item.ReceiveTime.Unix(),
		)
	}
	defer item.CArray.Free()

	result := new(mc.Item)
	result.Body = []byte(body)
	result.Flag = 0
	return result, nil
}

func IsValidKeyString(key string) bool {
	length := len(key)
	if length == 0 || length > MAX_KEY_LEN {
		logger.Warnf("bad key len=%d", length)
		return false
	}

	if key[0] <= ' ' || key[0] == '?' || key[0] == '@' {
		logger.Warnf("bad key len=%d key[0]=%x", length, key[0])
		return false
	}

	for _, r := range key {
		if unicode.IsControl(r) || unicode.IsSpace(r) {
			logger.Warnf("bad key len=%d %s", length, key)
			return false
		}
	}
	return true
}
