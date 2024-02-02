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
	currentMap map[string]string
}

func getTableTrieFromCfg(
	ccfg *config.CassandraStoreCfg, cqlStore *CassandraStore) (
	*trie.Tree[rune, string], map[string]string, error) {
	if !ccfg.PrefixTableDispatcherCfg.Enable {
		logger.Infof("table switcher disabled, skip init ...")
		return nil, nil, nil
	}
	
	tcfg := ccfg.PrefixTableDispatcherCfg
	s2k := tcfg.StaticCfg

	var prefixTrie *trie.Tree[rune, string]

	keysString := [][]rune{}
	vStrStatus := []string{}
	dedup := map[string]struct{}{}

	if tcfg.CfgFromCstarTable != "" && tcfg.CfgFromCstarKeySpace != "" {
		c := DisPatcherCfg(tcfg)
		pkeys, pvalues, err := c.LoadFromDB(cqlStore)
		if err != nil {
			return nil, nil, err
		}

		keysString = append(keysString, pkeys...)
		vStrStatus = append(vStrStatus, pvalues...)
	}

	if len(s2k) > 0 {
		for s, kprefixs := range s2k {
			for _, prefix := range kprefixs {
				keysString = append(keysString, []rune(prefix))
				vStrStatus = append(vStrStatus, s)
			}
		}
	}

	// check duplicate
	if len(vStrStatus) != len(keysString) {
		return nil, nil, fmt.Errorf("value list len not match with prefix list len")
	}

	duplicateKeys := []string{}
	loadedMap := map[string]string{}
	for idx, k := range keysString {
		ks := string(k)
		loadedMap[ks] = vStrStatus[idx]
		if _, ok := dedup[ks]; !ok {
			dedup[ks] = struct{}{}
		} else {
			duplicateKeys = append(duplicateKeys, ks)
		}
	}
	if len(duplicateKeys) > 0 {
		return nil, nil, fmt.Errorf("prefix cfg duplicate: %v", duplicateKeys)
	}
	logger.Infof("Loading from cfg: %v", loadedMap)

	if len(keysString) == len(vStrStatus) && len(keysString) > 0 {
		tr := trie.New[rune, string](keysString, vStrStatus)
		prefixTrie = &tr
	} else {
		prefixTrie = nil
	}

	return prefixTrie, loadedMap, nil
}

func NewKeyTableFinder(config *config.CassandraStoreCfg, cqlStore *CassandraStore) (*KeyTableFinder, error) {
	f := new(KeyTableFinder)
	t, nowMap, err := getTableTrieFromCfg(config, cqlStore)
	if err != nil {
		return nil, err
	}
	f.trie = t
	f.defaultT = config.DefaultTable
	f.currentMap = nowMap

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

func (f *KeyTableFinder) LoadStaticCfg(cfgDir string) (*config.CassandraStoreCfg, error) {
	cfg := struct {
		CassandraCfg config.CassandraStoreCfg `yaml:"cassandra"`
	}{}

	configF, err := ioutil.ReadFile(filepath.Join(cfgDir, "proxy.yaml"))
	if err != nil {
		return nil, err
	}
	err = yaml.Unmarshal(configF, &cfg)
	if err != nil {
		return nil, err
	}

	return &cfg.CassandraCfg, nil
}

func (f *KeyTableFinder) LoadCfg(cfg *config.CassandraStoreCfg, cqlStore *CassandraStore) error {
	if !cfg.PrefixTableDispatcherCfg.Enable {
		return fmt.Errorf("you can't disable key table finder online")
	}
	pTrie, nowMap, err := getTableTrieFromCfg(cfg, cqlStore)
	if err != nil {
		logger.Errorf("reloading c* cfg err: %s", err)
		return err
	}
	logger.Infof("reloading c* cfg for prefix switch to: %v", nowMap)

	defaultS := cfg.DefaultTable
	logger.Infof("reloading c* cfg for prefix default store to: %s", cfg.DefaultTable)
	

	f.lock.Lock()
	defer f.lock.Unlock()
	f.trie = pTrie
	f.defaultT = defaultS
	f.currentMap = nowMap
	cqlStore.staticTable = !cfg.PrefixTableDispatcherCfg.Enable
	return nil
}

func (f *KeyTableFinder) Upsert(cfg *config.CassandraStoreCfg, data map[string][]string, cqlStore *CassandraStore) error {
	dispatcherCfg := DisPatcherCfg(cfg.PrefixTableDispatcherCfg)
	return dispatcherCfg.SaveToDB(data, cqlStore)
}

func (f *KeyTableFinder) DeletePrefix(cfg *config.CassandraStoreCfg, prefix string, cqlStore *CassandraStore) error {
	dispatcherCfg := DisPatcherCfg(cfg.PrefixTableDispatcherCfg)
	return dispatcherCfg.DeletePrefixCfg(prefix, cqlStore)
}

func (f *KeyTableFinder) GetCurrentMap() map[string]string {
	return f.currentMap
}
