package dstore

import (
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"os/exec"
	"os/user"
	"path"
	"path/filepath"
	"testing"
	"time"

	dbcfg "github.com/douban/gobeansdb/gobeansdb"
	mc "github.com/douban/gobeansdb/memcache"
	yaml "gopkg.in/yaml.v2"

	"github.com/douban/gobeansproxy/config"
	"github.com/douban/gobeansproxy/utils"
	"github.com/stretchr/testify/assert"
)

var testDataDir = flag.String("testDataDir", "/tmp/gobeansdbproxy/bdb/data/", "this dir will be used by gobeansdb and proxy")


func setupSuite(tb testing.TB) func(tb  testing.TB) {
	user, err := user.Current()
	if err != nil {
		tb.Fatalf("get username err: %s", err)
	}
	gopath := os.Getenv("GOPATH")
	gobeansdbBin := filepath.Join(gopath, "bin", "gobeansdb")

	if _, err := os.Stat(gobeansdbBin); errors.Is(err, os.ErrNotExist) {
		tb.Fatalf("gobeansdb binary not exists, %s", gobeansdbBin)
	}

	projDir := utils.GetProjectHomeDir()

	allGobeansdb := []*exec.Cmd{}
	for _, p := range []string{"57980", "57981", "57982", "57983"} {
		conn, _ := net.DialTimeout("tcp", net.JoinHostPort("localhost", p), time.Second)
		if conn != nil {
			conn.Close()
			tb.Logf("%s port already listening ignore start ...", p)
			continue
		}

		// we modify config when developer run test without container
		gobeansdbCfg := fmt.Sprintf("%s/.doubanpde/scripts/bdb/gobeansproxy/%s/conf/", projDir, p)
		cfgParsed := dbcfg.DBConfig{}
		yfile, err := ioutil.ReadFile(filepath.Join(gobeansdbCfg, "global.yaml"))
		if err != nil {
			tb.Fatal(err)
		}
		err = yaml.Unmarshal(yfile, &cfgParsed)
		if err != nil {
			tb.Fatalf("load cfg %s err: %s", gobeansdbCfg, err)
		}
		dataPath := filepath.Join(*testDataDir, p, user.Username, "data")
		logPath := filepath.Join(*testDataDir, p, user.Username, "log")
		for _, pp := range []string{dataPath, logPath} {
			err = os.MkdirAll(pp, os.ModePerm)
			if err != nil {
				tb.Fatalf("create dir %s err: %s", pp, err)
			}
		}
		cfgParsed.ServerConfig.AccessLog = filepath.Join(logPath, "access.log")
		cfgParsed.ServerConfig.ErrorLog = filepath.Join(logPath, "error.log")
		cfgParsed.HStoreConfig.DBLocalConfig.Home = dataPath
		gobeansdbTestCfg := fmt.Sprintf("%s/.doubanpde/scripts/bdb/gobeansproxy/%s/testconf/", projDir, p)
		err = os.MkdirAll(gobeansdbTestCfg, os.ModePerm)
		if err != nil {
			tb.Fatalf("create dir %s err: %s", gobeansdbTestCfg, err)
		}
		c, err := yaml.Marshal(cfgParsed)
		if err != nil {
			tb.Fatalf("marshal cfg err: %s", err)
		}

		dbGlobalCfg := filepath.Join(gobeansdbTestCfg, "global.yaml")
		f, err := os.OpenFile(dbGlobalCfg, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0755)
		if err != nil {
			tb.Fatal(err)
		}
		defer f.Close()
		_, err = f.Write(c)
		if err != nil {
			tb.Fatal(err)
		}
		routeCfg := filepath.Join(gobeansdbTestCfg, "route.yaml")
		rcfg, err := ioutil.ReadFile(filepath.Join(gobeansdbCfg, "route.yaml"))
		if err != nil {
			tb.Fatal(err)
		}
		err = ioutil.WriteFile(routeCfg, rcfg, 0644)
		if err != nil {
			tb.Fatal(err)
		}
		
		cmd := exec.Command(
			gobeansdbBin,
			"-confdir",
			gobeansdbTestCfg,
		)
		if err := cmd.Start(); err != nil {
			tb.Fatalf("failed to start %s gobeansdb: %s", p, err)
		}
		tb.Logf("start %s with pid: %d", cmd, cmd.Process.Pid)
		allGobeansdb = append(allGobeansdb, cmd)
	}
	// wait some time let the server started
	time.Sleep(time.Second * 5)

	return func(tb testing.TB) {
		for _, execCmd := range allGobeansdb {
			if err := execCmd.Process.Kill(); err != nil {
				tb.Fatalf("failed to kill process %s: %s", execCmd, err)
			}
		}
	}
}

func testClientSet(t *testing.T, c mc.StorageClient, key string, val []byte) {
	assert := assert.New(t)
	flag := 2
	ok, err := clientSet(c, key, val, flag)
	setHosts := c.GetSuccessedTargets()
	c.Clean()
	assert.True(ok)
	assert.Nil(err)
	assert.True(len(setHosts) > 0)

	v, err := c.Get(key)
	getHosts := c.GetSuccessedTargets()
	c.Clean()

	assert.Equal(val, v.Body)
	assert.Equal(flag, v.Flag)
	assert.Equal(2, len(getHosts))
	assert.True(hasIntersection(setHosts, getHosts))
}

func clientSet(c mc.StorageClient, key string, val []byte, flag int) (bool, error) {
	item := newItem(flag, val)
	defer item.Free()
	noreply := false
	return c.Set(key, item, noreply)
}

func hasIntersection(arr1 []string, arr2 []string) bool {
	for _, i := range arr1 {
		for _, j := range arr2 {
			if i == j {
				return true
			}
		}
	}
	return false
}

func testFailStoreClient(t *testing.T, c mc.StorageClient) {
	assert := assert.New(t)
	key := "/test/fail/client"

	_, err := c.Get(key)
	assert.NotNil(err)

	_, err = c.Set("key", &mc.Item{}, false)
	assert.NotNil(err)

	_, err = c.GetMulti([]string{"key"})
	assert.NotNil(err)
}

func testStoreClient(t *testing.T, c mc.StorageClient) {
	assert := assert.New(t)
	key1 := "/test/client/1"

	r, _ := c.Get(key1)
	assert.Nil(r)
	assert.True(len(c.GetSuccessedTargets()) > 2)
	c.Clean()

	// set
	key2 := "/test/client/2"
	val2 := []byte("value 2")
	testClientSet(t, c, key2, val2)

	key3 := "/test/client/3"
	val3 := []byte("value 3")
	testClientSet(t, c, key3, val3)

	// get multi
	items, _ := c.GetMulti([]string{key1, key2, key3})
	c.Clean()
	assert.Equal(2, len(items))

	keyNum := 100
	keys := make([]string, keyNum)
	flagm := 3
	valm := []byte("value multi")
	for i := 0; i < keyNum; i++ {
		keys[i] = fmt.Sprintf("/test/client/multi_%d", i)
		ok, _ := clientSet(c, keys[i], valm, flagm)
		c.Clean()
		assert.True(ok)
	}
	items, err := c.GetMulti(keys)
	c.Clean()
	assert.Nil(err)
	assert.Equal(keyNum, len(items))

	// large obj
	key4 := "/test/client/4"
	val4 := make([]byte, 1024*1000)
	testClientSet(t, c, key4, val4)

	// delete
	key6 := "/test/client/6"
	val6 := []byte("value 6")
	testClientSet(t, c, key6, val6)
	ok, _ := c.Delete(key6)
	assert.True(ok)
	v6, _ := c.Get(key6)
	assert.Nil(v6)
}

func TestDStoreOnly(t *testing.T) {
	teardown := setupSuite(t)
	defer teardown(t)

	homeDir := utils.GetProjectHomeDir()
	confdir := path.Join(homeDir, ".doubanpde", "scripts", "bdb", "gobeansproxy", "dstore-only", "conf")
	proxyConf := &config.Proxy
	proxyConf.Load(confdir)

	InitGlobalManualScheduler(config.Route, proxyConf.N)
	storage := new(Storage)
	storage.InitStorageEngine(proxyConf)
	c := NewStorageClient(proxyConf.N, proxyConf.W, proxyConf.R, storage.cstar, storage.PSwitcher, storage.dualWErrHandler)

	testStoreClient(t, c)
}
