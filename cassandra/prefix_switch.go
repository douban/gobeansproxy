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
	statusBrw string = "br1w1cr0w0"
	statusBrwCw string = "br1w1cr0w1"
	statusBwCrw string = "br0w1cr1w1"
	statusCrw string = "br0w0cr1w1"
)

type PrefixSwitcher struct {
	trie *trie.Tree[rune, PrefixSwitchStatus]
	defaultT PrefixSwitchStatus
	lock sync.RWMutex
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
	default:
		return -1, fmt.Errorf("Unsupported switch type of %s", s) 
	}
}

func GetPrefixSwitchTrieFromCfg(cfg *config.CassandraStoreCfg) (*trie.Tree[rune, PrefixSwitchStatus], error) {
	s2k := cfg.SwitchToKeyPrefixes
	keysString := [][]rune{}
	vStatus := []PrefixSwitchStatus{}
	dedup := map[string]struct{}{}

	for s, kprefixs := range s2k {
		status, err := strToSwitchStatus(s)
		if err != nil {
			return nil, err
		}

		for _, prefix := range kprefixs {
			keysString = append(keysString, []rune(prefix))
			vStatus = append(vStatus, status)
			if _, ok := dedup[prefix]; !ok {
				dedup[prefix] = struct{}{}
			} else {
				// prefix can map to only one status
				return nil, fmt.Errorf("%s prefix duplicate in settings", prefix)
			}
		}
	}

	prefixTrie := trie.New[rune, PrefixSwitchStatus](keysString, vStatus)
	
	return &prefixTrie, nil
}

func NewPrefixSwitcher(config *config.CassandraStoreCfg) (*PrefixSwitcher, error) {
	prefixTrie, err := GetPrefixSwitchTrieFromCfg(config)
	if err != nil {
		return nil, err
	}

	f := new(PrefixSwitcher)
	f.trie = prefixTrie
	f.defaultT = PrefixSwitchBrw
	return f, nil
}

func (s *PrefixSwitcher) GetStatus(key string) PrefixSwitchStatus {
	s.lock.RLock()
	defer s.lock.RUnlock()

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

// check key prefix and return bdb read enable c* read enable
func (s *PrefixSwitcher) ReadEnabledOn(key string) (bool, bool) {
	status := s.GetStatus(key)
	return (status == PrefixSwitchBrw || status == PrefixSwitchBrwCw),
		(status == PrefixSwitchCrw || status == PrefixSwitchBwCrw)
}

// check keys prefix list and return bdb read keys and c* read keys
func (s *PrefixSwitcher) ReadEnableOnKeys(keys []string) (bkeys []string, ckeys []string) {
	for _, k := range keys {
		b, c := s.ReadEnabledOn(k)
		if b {
			bkeys = append(bkeys, k)
		}

		if c {
			ckeys = append(ckeys, k)
		}
	}
	return
}

// check key prefix and return bdb write enable c* write enable
func (s *PrefixSwitcher) WriteEnabledOn(key string) (bool, bool) {
	status := s.GetStatus(key)
	return (status == PrefixSwitchBrw || status == PrefixSwitchBrwCw || status == PrefixSwitchBwCrw),
		(status == PrefixSwitchCrw || status == PrefixSwitchBrwCw || status == PrefixSwitchBwCrw)
}

func (s *PrefixSwitcher) LoadCfg(cfgDir string) error {
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

	logger.Infof("reloading c* cfg for prefix switch to: %v", cfg.CassandraCfg.SwitchToKeyPrefixes)
	
	pTrie, err := GetPrefixSwitchTrieFromCfg(&cfg.CassandraCfg)
	if err != nil {
		logger.Errorf("reloading c* cfg err: %s", err)
		return err
	}

	s.lock.Lock()
	defer s.lock.Unlock()
	s.trie = pTrie
	return nil
}
