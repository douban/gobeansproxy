package main

import (
	mc "github.intra.douban.com/coresys/gobeansdb/memcache"
)

type Storage struct {
}

func (s *Storage) Client() mc.StorageClient {
	return new(StorageClient)
}

type StorageClient struct {
	// SuccessedTargets is a list of addrs on which the client request was ran
	// successfully.
	SuccessedTargets []string
}

func (sc *StorageClient) Get(key string) (*mc.Item, error) {
	return nil, nil
}

func (sc *StorageClient) GetMulti(keys []string) (map[string]*mc.Item, error) {
	return nil, nil
}

func (sc *StorageClient) Set(key string, item *mc.Item, noreply bool) (bool, error) {
	return false, nil
}

func (sc *StorageClient) Append(key string, value []byte) (bool, error) {
	return false, nil
}

func (sc *StorageClient) Incr(key string, value int) (int, error) {
	return 0, nil
}

func (sc *StorageClient) Delete(key string) (bool, error) {
	return false, nil
}

func (sc *StorageClient) Len() int {
	return 0
}

func (sc *StorageClient) Close() {
}

func (sc *StorageClient) Process(key string, args []string) (string, string) {
	return "", ""
}
