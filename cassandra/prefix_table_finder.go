package cassandra

import (
	"fmt"
	"io/ioutil"
	"path/filepath"
	"sync"

	"github.com/acomagu/trie/v2"
	"gopkg.in/yaml.v3"

	"github.com/douban/gobeansproxy/config"
)

var (
	selectQTpl string
	insertQTpl string
	deleteQTpl string
)

type KeyTableFinder struct {
	trie *trie.Tree[rune, string]
	defaultT string
	lock sync.RWMutex
}

func getTableTrieFromCfg(config *config.CassandraStoreCfg) trie.Tree[rune, string] {
	t2k := config.TableToKeyPrefix
	var ptrie trie.Tree[rune, string]
	if len(t2k) > 0 {
		runesKeys := [][]rune{}
		strValues := []string{}
    
		for t, kprefixs := range t2k {
			for _, prefix := range kprefixs {
				runesKeys = append(runesKeys, []rune(prefix))
				strValues = append(strValues, t)
			}
		}
    
		ptrie = trie.New[rune, string](runesKeys, strValues)
	}
	return ptrie
}

func NewKeyTableFinder(config *config.CassandraStoreCfg) (*KeyTableFinder, error) {
	f := new(KeyTableFinder)
	t := getTableTrieFromCfg(config)
	f.trie = &t
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
	if f.trie == nil {
		return f.defaultT
	}

	var v string
	var match bool

	f.lock.RLock()
	defer f.lock.RUnlock()

	n := *(f.trie)

	for _, c := range key {
		if n = n.TraceOne(c); n == nil {
			break
		}

		if vv, ok := n.Terminal(); ok {
			v = vv
			match = true
		}
	}

	if match {
		return v
	} else {
		return f.defaultT
	}
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

func (f *KeyTableFinder) LoadCfg(cfgDir string) error {
	cfg := struct {
		CassandraCfg config.CassandraStoreCfg `yaml:"cassandra"`
	}{}

	configF, err := ioutil.ReadFile(filepath.Join(cfgDir, "proxy.yaml"))
	if err != nil {
		return err
	}
	err = yaml.Unmarshal(configF, &cfg)
	if err != nil {
		return err
	}

	logger.Infof("reloading c* cfg for table finder to: %v", cfg.CassandraCfg.TableToKeyPrefix)
	pTrie := getTableTrieFromCfg(&cfg.CassandraCfg)

	f.lock.Lock()
	defer f.lock.Unlock()
	f.trie = &pTrie
	return nil
}
