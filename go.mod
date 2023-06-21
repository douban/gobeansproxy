module github.com/douban/gobeansproxy

require (
	github.com/douban/gobeansdb v1.1.2
	github.com/gocql/gocql v1.5.2
	github.com/stretchr/testify v1.8.0
	github.com/vmihailenco/msgpack/v5 v5.3.5
	gopkg.in/yaml.v2 v2.4.0
)

require (
	github.com/golang/snappy v0.0.4 // indirect
	github.com/prometheus/client_golang v1.16.0
)

go 1.13

// for lcoal dev
replace github.com/douban/gobeansproxy => ../gobeansproxy
