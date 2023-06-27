module github.com/douban/gobeansproxy

require (
	github.com/douban/gobeansdb v1.1.2
	github.com/gocql/gocql v1.5.2
	github.com/stretchr/testify v1.8.0
	gopkg.in/yaml.v2 v2.4.0
)

require (
	github.com/golang/snappy v0.0.4 // indirect
	github.com/prometheus/client_golang v1.16.0
	golang.org/x/sync v0.3.0 // indirect
)

go 1.13

// for lcoal dev
replace github.com/douban/gobeansproxy => ../gobeansproxy
