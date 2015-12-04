package main

import (
	"fmt"

	mc "github.intra.douban.com/coresys/gobeansdb/memcache"
)

func main() {
	fmt.Println("hello gobeansproxy!")
	storage := new(Storage)
	server := mc.NewServer(storage)
	addr := fmt.Sprintf("%s:%d", "0.0.0.0", 7905)
	server.Listen(addr)
	fmt.Println("ready")
	server.Serve()
}
