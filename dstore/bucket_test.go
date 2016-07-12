package dstore

import (
	"github.intra.douban.com/coresys/gobeansproxy/config"
	"github.intra.douban.com/coresys/gobeansproxy/utils"
	"path"
	"sync"
	"testing"
	"time"
)

func TestAddResTime(t *testing.T) {
	homeDir := utils.GetProjectHomeDir()
	confdir := path.Join(homeDir, "conf")
	proxyConf := &config.Proxy
	proxyConf.Load(confdir)

	testHost := []struct {
		Addr    string
		resTime []float64
	}{
		{
			"127.0.0.1:1234",
			[]float64{1.1, 1.3, 1.4, 2},
		},
		{
			"10.0.0.1:1234",
			[]float64{1.4, 1, 6, 1.9, 2.1},
		},
		{
			"10.0.0.2:1234",
			[]float64{1.4, 1, 6, 1.9, 2.1},
		},
	}
	hosts := []*Host{}
	for _, addr := range testHost {
		host := NewHost(addr.Addr)
		hosts = append(hosts, host)
	}
	bucket := newBucket(0, hosts...)
	var wg sync.WaitGroup
	for _, host := range testHost {
		wg.Add(1)
		go func(addr string, resTime []float64) {
			for _, score := range resTime {
				now := time.Now()
				bucket.addResTime(addr, now, score)
				time.Sleep(1 * time.Second)
			}
			wg.Done()
		}(host.Addr, host.resTime)
	}
	wg.Wait()
	bucket.ReBalance()
	for _, h := range bucket.hostsList {
		if h.score == 0 {
			t.Errorf("the host %s got score %f", h.host.Addr, h.score)
		}
	}
}

func TestDownHost(t *testing.T) {
	homeDir := utils.GetProjectHomeDir()
	confdir := path.Join(homeDir, "conf")
	proxyConf := &config.Proxy
	proxyConf.Load(confdir)

	testHost := []struct {
		Addr    string
		resTime []float64
		status  bool
	}{
		{
			"127.0.0.1:1234",
			[]float64{1.1, 1.3, 1.4, 2},
			true,
		},
		{
			"10.0.0.1:1234",
			[]float64{1.4, 1, 6, 1.9, 2.1},
			true,
		},
		{
			"10.0.0.2:1234",
			[]float64{1.4, 1, 6, 1.9, 2.1},
			false,
		},
	}
	hosts := []*Host{}
	for _, addr := range testHost {
		host := NewHost(addr.Addr)
		hosts = append(hosts, host)
	}
	bucket := newBucket(0, hosts...)
	var wg sync.WaitGroup
	for _, host := range testHost {
		wg.Add(1)
		go func(addr string, resTime []float64) {
			for _, score := range resTime {
				now := time.Now()
				bucket.addResTime(addr, now, score)
				time.Sleep(1 * time.Second)
			}
			wg.Done()
		}(host.Addr, host.resTime)
	}
	wg.Wait()

	for _, host := range testHost {
		if !host.status {
			bucket.downHost(host.Addr)
		}
	}
	bucket.ReBalance()
	for _, h := range bucket.hostsList {
		if !h.status && h.score > 0 {
			t.Errorf("the host %s isdown, should got score %f", h.host.Addr, h.score)
		} else if h.score == 0 && h.status {
			t.Errorf("the host %s got score %f", h.host.Addr, h.score)
		}
	}
}
