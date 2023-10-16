package cassandra

import (
	"fmt"
	"io/ioutil"
	"path/filepath"
	"sync"

	"github.com/acomagu/trie/v2"
	"github.com/douban/gobeansproxy/config"
	"gopkg.in/yaml.v3"
)

type PrefixSwitchStatus int

const (
	// bdb r/w c* disable
	PrefixSwitchBrw PrefixSwitchStatus = 0
	// bdb r/w c* w
	PrefixSwitchBrwCw PrefixSwitchStatus = 1
	// bdb w c* r/w
	PrefixSwitchBwCrw PrefixSwitchStatus = 2
	// c* rw bdb disable
	PrefixSwitchCrw PrefixSwitchStatus = 3
	// c* read only bdb disable
	PrefixSwitchCr PrefixSwitchStatus  = 4

	statusBrw string = "br1w1cr0w0"
	statusBrwCw string = "br1w1cr0w1"
	statusBwCrw string = "br0w1cr1w1"
	statusCrw string = "br0w0cr1w1"
	statusCr string = "br0w0cr1w0"
)

type PrefixSwitcher struct {
	trie *trie.Tree[rune, PrefixSwitchStatus]
	defaultT PrefixSwitchStatus
	lock sync.RWMutex
	currentTrieMap map[string]string
}

func (s PrefixSwitchStatus) IsReadOnBeansdb() bool {
	return s == PrefixSwitchBrw || s == PrefixSwitchBrwCw
}

func (s PrefixSwitchStatus) IsReadOnCstar() bool {
	return s == PrefixSwitchCrw || s == PrefixSwitchCr || s == PrefixSwitchBwCrw
}

func (s PrefixSwitchStatus) IsWriteOnBeansdb() bool {
	return s == PrefixSwitchBrw || s == PrefixSwitchBrwCw || s == PrefixSwitchBwCrw
}

func (s PrefixSwitchStatus) IsWriteOnCstar() bool {
	return s == PrefixSwitchCrw || s == PrefixSwitchBrwCw || s == PrefixSwitchBwCrw
}

func strToSwitchStatus(s string) (PrefixSwitchStatus, error) {
	switch s {
	case statusBrw:
		return PrefixSwitchBrw, nil
	case statusBrwCw:
		return PrefixSwitchBrwCw, nil
	case statusBwCrw:
		return PrefixSwitchBwCrw, nil
	case statusCrw:
		return PrefixSwitchCrw, nil
	case statusCr:
		return PrefixSwitchCr, nil
	default:
		return -1, fmt.Errorf("Unsupported switch type of %s", s) 
	}
}

func GetPrefixSwitchTrieFromCfg(
	cfg *config.CassandraStoreCfg, cqlStore *CassandraStore) (
	*trie.Tree[rune, PrefixSwitchStatus], map[string]string, error) {
	if !cfg.PrefixRWDispatcherCfg.Enable {
		logger.Infof("rw switcher disabled, skip init ...")
		return nil, nil, nil
	}
	
	s2k := cfg.PrefixRWDispatcherCfg.StaticCfg

	keysString := [][]rune{}
	vStatus := []PrefixSwitchStatus{}
	vStrStatus := []string{}
	dedup := map[string]struct{}{}

	if cfg.PrefixRWDispatcherCfg.CfgFromCstarTable != "" &&
		cfg.PrefixRWDispatcherCfg.CfgFromCstarKeySpace != "" {
		c := DisPatcherCfg(cfg.PrefixRWDispatcherCfg)
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

	// now init real value
	for _, sv := range vStrStatus {
		rv, err := strToSwitchStatus(sv)
		if err != nil {
			return nil, nil, fmt.Errorf("parse value %s to status err: %s", sv, err)
		}
		vStatus = append(vStatus, rv)
	}

	logger.Infof("Loading from cfg: %v", loadedMap)
	if len(keysString) == len(vStatus) && len(keysString) > 0 {
		tr := trie.New[rune, PrefixSwitchStatus](keysString, vStatus)
		return &tr, loadedMap, nil
	} else {
		return nil, loadedMap, nil
	}
}

func NewPrefixSwitcher(config *config.CassandraStoreCfg, cqlStore *CassandraStore) (*PrefixSwitcher, error) {
	prefixTrie, nowMap, err := GetPrefixSwitchTrieFromCfg(config, cqlStore)
	if err != nil {
		return nil, err
	}

	f := new(PrefixSwitcher)
	f.trie = prefixTrie

	defaultS, err := strToSwitchStatus(config.SwitchToKeyDefault)
	if err != nil {
		return nil, err
	}

	f.defaultT = defaultS
	f.currentTrieMap = nowMap
	return f, nil
}

// use this to match longest prefix of key
// You should lock the s trie to prevent trie update
func (s *PrefixSwitcher) matchStatus(key string) PrefixSwitchStatus {
	if s.trie == nil {
		return s.defaultT
	}

	var v PrefixSwitchStatus
	var match bool

	n := *(s.trie)

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
		return s.defaultT
	}
}

func (s *PrefixSwitcher) GetStatus(key string) PrefixSwitchStatus {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.matchStatus(key)
}

// check key prefix and return bdb read enable c* read enable
func (s *PrefixSwitcher) ReadEnabledOn(key string) (bool, bool) {
	status := s.GetStatus(key)
	return status.IsReadOnBeansdb(), status.IsReadOnCstar()
}

// check keys prefix list and return bdb read keys and c* read keys
func (s *PrefixSwitcher) ReadEnableOnKeys(keys []string) (bkeys []string, ckeys []string) {
	s.lock.RLock()
	defer s.lock.RUnlock()

	for _, k := range keys {
		status := s.matchStatus(k)
		if status.IsReadOnBeansdb() {
			bkeys = append(bkeys, k)
			// prevent wrong status
			// read can only be enable on 1 backend
			continue
		}

		if status.IsReadOnCstar() {
			ckeys = append(ckeys, k)
		}
	}
	return
}

// check key prefix and return bdb write enable c* write enable
func (s *PrefixSwitcher) WriteEnabledOn(key string) (bool, bool) {
	status := s.GetStatus(key)
	return status.IsWriteOnBeansdb(), status.IsWriteOnCstar()
}

func (s *PrefixSwitcher) LoadStaticCfg(cfgDir string) (*config.CassandraStoreCfg, error) {
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

func (s *PrefixSwitcher) LoadCfg(cfg *config.CassandraStoreCfg, cqlStore *CassandraStore) error {
	if !cfg.PrefixRWDispatcherCfg.Enable {
		logger.Errorf("You can't disable rw dispatcher online")
		return fmt.Errorf("You can't disable rw dispathcer online")
	}

	pTrie, nowMap, err := GetPrefixSwitchTrieFromCfg(cfg, cqlStore)
	if err != nil {
		logger.Errorf("reloading c* cfg err: %s", err)
		return err
	}
	logger.Infof("reloading c* cfg for prefix switch to: %v", nowMap)

	defaultS, err := strToSwitchStatus(cfg.SwitchToKeyDefault)
	if err != nil {
		logger.Errorf("default switch storage parse err: %s", err)
	}
	logger.Infof("reloading c* cfg for prefix default store to: %s", cfg.SwitchToKeyDefault)
	

	s.lock.Lock()
	defer s.lock.Unlock()
	s.trie = pTrie
	s.defaultT = defaultS
	s.currentTrieMap = nowMap
	return nil
}

func (s *PrefixSwitcher) Upsert(cfg *config.CassandraStoreCfg, data map[string][]string, cqlStore *CassandraStore) error {
	dispatcherCfg := DisPatcherCfg(cfg.PrefixRWDispatcherCfg)
	return dispatcherCfg.SaveToDB(data, cqlStore)
}

func (s *PrefixSwitcher) DeletePrefix(cfg *config.CassandraStoreCfg, prefix string, cqlStore *CassandraStore) error {
	dispatcherCfg := DisPatcherCfg(cfg.PrefixRWDispatcherCfg)
	return dispatcherCfg.DeletePrefixCfg(prefix, cqlStore)
}

func (s *PrefixSwitcher) GetCurrentMap() map[string]string {
	return s.currentTrieMap
}
