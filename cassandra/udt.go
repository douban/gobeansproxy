package cassandra

import (
	"fmt"
	"time"

	mc "github.com/douban/gobeansdb/memcache"
	"github.com/gocql/gocql"
)

type BDBValue struct {
	ReceiveTime time.Time `cql:"rtime"`
	Flag        int `cql:"flag"`
	Exptime     int `cql:"exptime"`
	Cas         int `cql:"cas"`
	Body        []byte `cql:"body"`
}

func NewBDBValue(item *mc.Item) *BDBValue {
	return &BDBValue{
		ReceiveTime: item.ReceiveTime,
		Flag: item.Flag,
		Exptime: item.Exptime,
		Cas: item.Cas,
		Body: item.CArray.Body,
	}
}

func (b *BDBValue) ToMCItem() *mc.Item {
	item := &mc.Item{
		ReceiveTime: b.ReceiveTime,
		Flag: b.Flag,
		Exptime: b.Exptime,
		Cas: b.Cas,
	}
	item.Alloc(len(b.Body))
	copy(item.CArray.Body, b.Body)
	return item
}

func (b BDBValue) MarshalUDT(name string, info gocql.TypeInfo) ([]byte, error) {
	switch name {
	case "rtime":
		return gocql.Marshal(info, b.ReceiveTime)
	case "flag":
		return gocql.Marshal(info, b.Flag)
	case "exptime":
		return gocql.Marshal(info, b.Exptime)
	case "cas":
		return gocql.Marshal(info, b.Cas)
	case "body":
		return gocql.Marshal(info, b.Body)
	default:
		return nil, fmt.Errorf("unknown column for position: %q", name)
	}
}

func (b *BDBValue) UnmarshalUDT(name string, info gocql.TypeInfo, data []byte) error {
	switch name {
	case "rtime":
		return gocql.Unmarshal(info, data, &b.ReceiveTime)
	case "flag":
		return gocql.Unmarshal(info, data, &b.Flag)
	case "exptime":
		return gocql.Unmarshal(info, data, &b.Exptime)
	case "cas":
		return gocql.Unmarshal(info, data, &b.Cas)
	case "body":
		return gocql.Unmarshal(info, data, &b.Body)
	default:
		return fmt.Errorf("unknown column for position: %q", name)
	}

}
