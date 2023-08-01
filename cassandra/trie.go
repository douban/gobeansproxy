package cassandra

import (
	"fmt"

	"github.com/viant/ptrie"
	"github.com/douban/gobeansproxy/config"
)

var (
	selectQTpl string
	insertQTpl string
	deleteQTpl string
)

type KeyTableFinder struct {
	trie *ptrie.Trie
	defaultT string
}

func NewKeyTableFinder(config *config.CassandraStoreCfg) (*KeyTableFinder, error) {
	t2k := config.TableToKeyPrefix
	prefixTrie := ptrie.New()

	for t, kprefixs := range t2k {
		for _, prefix := range kprefixs {
			err := prefixTrie.Put([]byte(prefix), t)
			if err != nil {
				return nil, err
			}
		}
	}

	f := new(KeyTableFinder)
	f.trie = &prefixTrie
	f.defaultT = config.DefaultTable

	// init sql str
	selectQTpl = fmt.Sprintf(
		"select value from %s.%%s where key = ?",
		config.DefaultKeySpace,
	)
	insertQTpl = fmt.Sprintf(
		"insert into %s.%%s (key, value) values (?, ?)",
		config.DefaultKeySpace,
	)
	deleteQTpl = fmt.Sprintf(
		"delete from %s.%%s where key = ?",
		config.DefaultKeySpace,
	)
	
	return f, nil
}

func (f *KeyTableFinder) GetTableByKey(key string) string {
	var result string
	v := (*(f.trie)).MatchPrefix([]byte(key), func(key []byte, value interface{}) bool {
		result = value.(string)
		return true
	})

	if !v {
		return f.defaultT
	}
	return result
}

func (f *KeyTableFinder) GetSqlTpl(sqlType string, key string) string {
	switch sqlType {
	case "select":
		return fmt.Sprintf(selectQTpl, f.GetTableByKey(key))
	case "delete":
		return fmt.Sprintf(deleteQTpl, f.GetTableByKey(key))
	default:
		return fmt.Sprintf(insertQTpl, f.GetTableByKey(key))
	}
}
