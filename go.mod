module github.com/douban/gobeansproxy

require (
	github.com/acomagu/trie/v2 v2.0.0
	github.com/douban/gobeansdb v1.1.3
	github.com/gocql/gocql v1.5.2
	github.com/sirupsen/logrus v1.9.3
	github.com/stretchr/testify v1.8.0
	gopkg.in/natefinch/lumberjack.v2 v2.2.1
	gopkg.in/yaml.v2 v2.4.0
	gopkg.in/yaml.v3 v3.0.1
)

require (
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.2.0 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/golang/protobuf v1.5.3 // indirect
	github.com/hailocab/go-hostpool v0.0.0-20160125115350-e80d13ce29ed // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.4 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/prometheus/client_model v0.3.0 // indirect
	github.com/prometheus/common v0.42.0 // indirect
	github.com/prometheus/procfs v0.10.1 // indirect
	github.com/rogpeppe/go-internal v1.11.0 // indirect
	github.com/samuel/go-zookeeper v0.0.0-20190923202752-2cc03de413da // indirect
	github.com/spaolacci/murmur3 v1.1.0 // indirect
	golang.org/x/exp v0.0.0-20230310171629-522b1b587ee0 // indirect
	golang.org/x/sys v0.10.0 // indirect
	google.golang.org/protobuf v1.33.0 // indirect
	gopkg.in/inf.v0 v0.9.1 // indirect
)

require (
	github.com/golang/snappy v0.0.4 // indirect
	github.com/prometheus/client_golang v1.16.0
	golang.org/x/sync v0.3.0
)

go 1.20

// for lcoal dev
// replace github.com/douban/gobeansdb => ../gobeansdb

// replace github.com/douban/gobeansproxy => ../gobeansproxy
