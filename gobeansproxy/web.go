package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"text/template"
	"time"

	"github.intra.douban.com/coresys/gobeansdb/cmem"
	dbcfg "github.intra.douban.com/coresys/gobeansdb/config"
	mc "github.intra.douban.com/coresys/gobeansdb/memcache"
	"github.intra.douban.com/coresys/gobeansdb/utils"
	"github.intra.douban.com/coresys/gobeansproxy/config"
	"github.intra.douban.com/coresys/gobeansproxy/dstore"

	yaml "gopkg.in/yaml.v2"
)

func handleWebPanic(w http.ResponseWriter) {
	r := recover()
	if r != nil {
		stack := utils.GetStack(2000)
		logger.Errorf("web req panic:%#v, stack:%s", r, stack)
		fmt.Fprintf(w, "\npanic:%#v, stack:%s", r, stack)
	}
}

func handleYaml(w http.ResponseWriter, v interface{}) {
	defer handleWebPanic(w)
	b, err := yaml.Marshal(v)
	if err != nil {
		w.Write([]byte(err.Error()))
	} else {
		w.Write(b)
	}
}

func handleJson(w http.ResponseWriter, v interface{}) {
	b, err := json.Marshal(v)
	if err != nil {
		w.Write([]byte(err.Error()))
	} else {
		w.Write(b)
	}
}

type templateHandler struct {
	once     sync.Once
	filename string
	templ    *template.Template
}

func (t *templateHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	t.once.Do(func() {
		t.templ = template.Must(template.New("base.html").Option("missingkey=error").ParseFiles(
			filepath.Join(proxyConf.StaticDir, t.filename),
			filepath.Join(proxyConf.StaticDir, "templates/base.html")))
	})
	var data map[string]interface{}
	if t.filename == "templates/score.html" {
		data = map[string]interface{}{
			"stats": dstore.GetScheduler().Stats(),
		}
	}
	e := t.templ.Execute(w, data)
	if e != nil {
		logger.Errorf("ServerHTTP filename:%s, error: %s", t.filename, e.Error())
	}
}

func startWeb() {
	http.Handle("/templates/", http.FileServer(http.Dir(proxyConf.StaticDir)))

	http.Handle("/", &templateHandler{filename: "templates/stats.html"})
	http.Handle("/score/", &templateHandler{filename: "templates/score.html"})
	http.HandleFunc("/score/json", handleScore)

	// same as gobeansdb
	http.HandleFunc("/config/", handleConfig)
	http.HandleFunc("/request/", handleRequest)
	http.HandleFunc("/buffer/", handleBuffer)
	http.HandleFunc("/memstat/", handleMemStat)
	http.HandleFunc("/rusage/", handleRusage)
	http.HandleFunc("/route/", handleRoute)
	http.HandleFunc("/route/version", handleRouteVersion)
	http.HandleFunc("/route/reload", handleRouteReload)

	webaddr := fmt.Sprintf("%s:%d", proxyConf.Listen, proxyConf.WebPort)
	go func() {
		logger.Infof("HTTP listen at %s", webaddr)
		if err := http.ListenAndServe(webaddr, nil); err != nil {
			logger.Fatalf("ListenAndServer: %s", err.Error())
		}
	}()
}

func handleConfig(w http.ResponseWriter, r *http.Request) {
	defer handleWebPanic(w)
	handleJson(w, proxyConf)
}

func handleRequest(w http.ResponseWriter, r *http.Request) {
	defer handleWebPanic(w)
	handleJson(w, mc.RL)
}

func handleRusage(w http.ResponseWriter, r *http.Request) {
	defer handleWebPanic(w)
	rusage := utils.Getrusage()
	handleJson(w, rusage)
}

func handleMemStat(w http.ResponseWriter, r *http.Request) {
	defer handleWebPanic(w)
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	handleJson(w, ms)
}

func handleBuffer(w http.ResponseWriter, r *http.Request) {
	defer handleWebPanic(w)
	handleJson(w, &cmem.DBRL)
}

func handleScore(w http.ResponseWriter, r *http.Request) {
	defer handleWebPanic(w)
	scores := dstore.GetScheduler().Stats()
	handleJson(w, scores)
}

func handleRoute(w http.ResponseWriter, r *http.Request) {
	defer handleWebPanic(w)
	handleYaml(w, config.Route)
}

func handleRouteVersion(w http.ResponseWriter, r *http.Request) {
	defer handleWebPanic(w)
	if len(proxyConf.ZKServers) == 0 {
		w.Write([]byte("-1"))
		return
	} else {
		w.Write([]byte(strconv.Itoa(dbcfg.ZKClient.Version)))
	}
}

func getFormValueInt(r *http.Request, name string, ndefault int) (n int, err error) {
	n = ndefault
	s := r.FormValue(name)
	if s != "" {
		n, err = strconv.Atoi(s)
	}
	return
}

func handleRouteReload(w http.ResponseWriter, r *http.Request) {
	var err error
	if !dbcfg.AllowReload {
		w.Write([]byte("err: reloading"))
		return
	}

	dbcfg.AllowReload = false
	defer func() {
		dbcfg.AllowReload = true
		if err != nil {
			logger.Errorf("handleRoute err", err.Error())
			w.Write([]byte(fmt.Sprintf(err.Error())))
			return
		}
	}()

	if len(proxyConf.ZKServers) == 0 {
		w.Write([]byte("err: not using zookeeper"))
		return
	}

	defer handleWebPanic(w)

	r.ParseForm()
	ver, err := getFormValueInt(r, "ver", -1)
	if err != nil {
		return
	}

	newRouteContent, ver, err := dbcfg.ZKClient.GetRouteRaw(ver)
	if ver == dbcfg.ZKClient.Version {
		w.Write([]byte(fmt.Sprintf("warn: same version %d", ver)))
		return
	}
	info := fmt.Sprintf("update with route version %d\n", ver)
	logger.Infof(info)
	newRoute := new(dbcfg.RouteTable)
	err = newRoute.LoadFromYaml(newRouteContent)
	if err != nil {
		return
	}

	oldScheduler := dstore.GetScheduler()
	dstore.InitGlobalManualScheduler(newRoute, proxyConf.N)
	config.Route = newRoute
	dbcfg.ZKClient.Version = ver
	w.Write([]byte("ok"))

	go func() {
		// sleep for request to be completed.
		time.Sleep(time.Duration(proxyConf.ReadTimeoutMs) * time.Millisecond * 5)
		oldScheduler.Close()
	}()
}
