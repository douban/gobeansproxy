package cassandra

import (
	"fmt"

	"github.com/douban/gobeansproxy/config"
)

type PrefixDisPatcher interface {
	LoadStaticCfg(string) (*config.CassandraStoreCfg, error)
	LoadCfg(*config.CassandraStoreCfg, *CassandraStore) error
	Upsert(*config.CassandraStoreCfg, map[string][]string, *CassandraStore) error
	DeletePrefix(*config.CassandraStoreCfg, string, *CassandraStore) error
	GetCurrentMap() map[string]string
}

type DisPatcherCfg config.PrefixDisPatcherCfg

func (config *DisPatcherCfg) LoadFromDB(
	cqlStore *CassandraStore) (prefixKeys [][]rune, vstatus []string, err error) {
	r := cqlStore.session.Query(
		fmt.Sprintf(
			"select prefix, value from %s.%s",
			config.CfgFromCstarKeySpace,
			config.CfgFromCstarTable,
		),
	).Iter().Scanner()

	for r.Next() {
		var (
			prefix string
			value string
		)

		err := r.Scan(&prefix, &value)
		if err != nil {
			return nil, nil, fmt.Errorf("load cfg from c* table err: %s", err)
		}

		prefixKeys = append(prefixKeys, []rune(prefix))
		vstatus = append(vstatus, value)
	}

	if err := r.Err(); err != nil {
		return nil, nil, fmt.Errorf("load cfg from c* iter err: %s", err)
	}

	return prefixKeys, vstatus, err
}

func (c *DisPatcherCfg) SaveToDB(m map[string][]string, cqlStore *CassandraStore) error {
	for value, prefix := range m {
		for _, p := range prefix {
			err := cqlStore.session.Query(
				fmt.Sprintf(
					"insert into %s.%s (prefix, value) values (?, ?)",
					c.CfgFromCstarKeySpace, c.CfgFromCstarTable,
				), p, value,
			).Exec()

			if err != nil {
				return fmt.Errorf("insert %s -> %s err: %s", p, value)
			}
		}
	}
	return nil
}

func (c *DisPatcherCfg) DeletePrefixCfg(prefix string, cqlStore *CassandraStore) error {
	err := cqlStore.session.Query(
		fmt.Sprintf(
			"delete from %s.%s where prefix = ?",
			c.CfgFromCstarKeySpace, c.CfgFromCstarTable,
		), prefix,
	).Exec()

	if err != nil {
		return fmt.Errorf("delete cfg prefix %s err: %s", prefix, err)
	}
	return nil
}
