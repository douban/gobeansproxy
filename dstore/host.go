package dstore

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	mc "github.intra.douban.com/coresys/gobeansdb/memcache"
)

const (
	WAIT_FOR_RETRY = "wait for retry"
)

type Host struct {
	// Addr is host:port pair
	Addr string

	// Index is the index of host in Scheduler.hosts
	Index int

	// nextDial is the next time to reconnect
	nextDial time.Time

	// conns is a free list of connections
	conns chan net.Conn

	sync.Mutex
}

func NewHost(addr string) *Host {
	host := new(Host)
	host.Addr = addr
	host.conns = make(chan net.Conn, proxyConf.MaxFreeConnsPerHost)
	return host
}

func isWaitForRetry(err error) bool {
	return err != nil && strings.HasPrefix(err.Error(), WAIT_FOR_RETRY)
}

func (host *Host) Close() {
	if host.conns == nil {
		return
	}
	ch := host.conns
	host.conns = nil
	close(ch)

	for c, closed := <-ch; closed; {
		c.Close()
	}
}

func (host *Host) isSilence(now time.Time) (time.Time, bool) {
	if host.nextDial.After(now) {
		return host.nextDial, true
	}
	return now, false
}

func (host *Host) createConn() (net.Conn, error) {
	now := time.Now()
	if nextDial, isSilence := host.isSilence(now); isSilence {
		return nil, fmt.Errorf("%s: next try %s", WAIT_FOR_RETRY, nextDial.Format("2006-01-02T15:04:05.999"))
	}

	conn, err := net.DialTimeout("tcp", host.Addr, time.Duration(proxyConf.ConnectTimeoutMs)*time.Millisecond)
	if err != nil {
		host.nextDial = now.Add(time.Millisecond * time.Duration(proxyConf.DialFailSilenceMs))
		return nil, err
	}
	return conn, nil
}

func (host *Host) getConn() (c net.Conn, err error) {
	if host.conns == nil {
		return nil, errors.New("host closed")
	}
	select {
	// Grab a connection if available; create if not.
	case c = <-host.conns:
		// Got one; nothing more to do.
	default:
		// None free, so create a new one.
		c, err = host.createConn()
	}
	return
}

func (host *Host) releaseConn(conn net.Conn) {
	if host.conns == nil {
		conn.Close()
		return
	}
	select {
	// Reuse connection if there's room.
	case host.conns <- conn:
		// Connection on free list; nothing more to do.
	default:
		// Free list full, just carry on.
		conn.Close()
	}
}

func (host *Host) execute(req *mc.Request) (resp *mc.Response, delta time.Duration, err error) {
	now := time.Now()
	conn, err := host.getConn()
	if err != nil {
		return
	}

	err = req.Write(conn)
	if err != nil {
		logger.Infof("%s write request failed: %v", host.Addr, err)
		conn.Close()
		return
	}

	resp = new(mc.Response)
	if req.NoReply {
		host.releaseConn(conn)
		resp.Status = "STORED"
		delta = time.Since(now)
		return
	}

	reader := bufio.NewReader(conn)
	if err = resp.Read(reader); err != nil {
		logger.Infof("%s read response failed: %v", host.Addr, err)
		conn.Close()
		return nil, 0, err
	}

	if err = req.Check(resp); err != nil {
		logger.Infof("%s unexpected response %s %v %v",
			host.Addr, req, resp, err)
		conn.Close()
		return nil, 0, err
	}

	host.releaseConn(conn)
	delta = time.Since(now)
	return
}

func (host *Host) executeWithTimeout(req *mc.Request, timeout time.Duration) (resp *mc.Response, err error) {
	done := make(chan bool, 1)
	isTimeout := false
	go func() {
		var delta time.Duration
		resp, delta, err = host.execute(req)
		done <- true
		if isTimeout && err == nil {
			logger.Infof("request %v to host %s return after timeout, use %d ms",
				req, host.Addr, delta/1e6)
		}
	}()

	select {
	case <-done:
	case <-time.After(timeout):
		isTimeout = true
		err = fmt.Errorf("request %v timeout", req)
		logger.Infof("request %v to host %s timeout", req, host.Addr)
	}
	return
}

func (host *Host) Len() int {
	return 0
}

func (host *Host) store(cmd string, key string, item *mc.Item, noreply bool) (bool, error) {
	req := &mc.Request{Cmd: cmd, Keys: []string{key}, Item: item, NoReply: noreply}
	resp, err := host.executeWithTimeout(req, time.Duration(proxyConf.WriteTimeoutMs)*time.Millisecond)
	return err == nil && resp.Status == "STORED", err
}

func (host *Host) Set(key string, item *mc.Item, noreply bool) (bool, error) {
	return host.store("set", key, item, noreply)
}

func (host *Host) Get(key string) (*mc.Item, error) {
	req := &mc.Request{Cmd: "get", Keys: []string{key}}
	resp, err := host.executeWithTimeout(req, time.Duration(proxyConf.ReadTimeoutMs)*time.Millisecond)
	if err != nil {
		return nil, err
	}
	item, _ := resp.Items[key]
	return item, nil
}

func (host *Host) GetMulti(keys []string) (map[string]*mc.Item, error) {
	req := &mc.Request{Cmd: "get", Keys: keys}
	resp, _, err := host.execute(req)
	if err != nil {
		return nil, err
	}
	return resp.Items, nil
}

func (host *Host) Append(key string, value []byte) (bool, error) {
	flag := 0
	item := newItem(flag, value)
	defer freeItem(item)
	req := &mc.Request{Cmd: "append", Keys: []string{key}, Item: item}
	resp, _, err := host.execute(req)
	if err == nil {
		return resp.Status == "STORED", nil
	} else {
		return false, err
	}
}

func (host *Host) Incr(key string, value int) (int, error) {
	flag := 0
	item := newItem(flag, []byte(strconv.Itoa(value)))
	defer freeItem(item)
	req := &mc.Request{Cmd: "incr", Keys: []string{key}, Item: item}
	resp, _, err := host.execute(req)
	if err != nil {
		return 0, err
	}
	return strconv.Atoi(resp.Msg)
}

func (host *Host) Delete(key string) (bool, error) {
	req := &mc.Request{Cmd: "delete", Keys: []string{key}}
	resp, _, err := host.execute(req)
	if err == nil {
		return resp.Status == "DELETED", nil
	} else {
		return false, err
	}
}

func (host *Host) Process(key string, args []string) (string, string) {
	return "", ""
}
