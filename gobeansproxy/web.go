package gobeansproxy

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	_ "net/http/pprof"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"text/template"
	"time"

	"github.com/douban/gobeansdb/cmem"
	dbcfg "github.com/douban/gobeansdb/config"
	mc "github.com/douban/gobeansdb/memcache"
	"github.com/douban/gobeansdb/utils"
	"github.com/douban/gobeansproxy/config"
	"github.com/douban/gobeansproxy/dstore"
	"github.com/douban/gobeansproxy/cassandra"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	yaml "gopkg.in/yaml.v2"
)

func getBucket(r *http.Request) (bucketID int64, err error) {
	s := filepath.Base(r.URL.Path)
	return strconv.ParseInt(s, 16, 16)
}

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
	// add divide func
	fm := template.FuncMap{"divide": func(sumTime float64, count int) int {
		return int(sumTime) / count
	}}
	t.once.Do(func() {
		t.templ = template.Must(template.New("base.html").Funcs(fm).Option("missingkey=error").ParseFiles(
			filepath.Join(proxyConf.StaticDir, t.filename),
			filepath.Join(proxyConf.StaticDir, "templates/base.html")))
	})
	var data map[string]interface{}
	if t.filename == "templates/score.html" {
		data = map[string]interface{}{
			"stats": dstore.GetScheduler().Stats(),
		}
	}
	if t.filename == "templates/bucketinfo.html" {
		bucketID, err := getBucket(r)
		if err != nil {
		}
		data = map[string]interface{}{
			"bucketinfo": dstore.GetScheduler().GetBucketInfo(bucketID),
		}
	}

	if t.filename == "templates/buckets.html" {
		data = map[string]interface{}{
			"buckets": dstore.GetScheduler().Partition(),
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
	http.Handle("/bucketinfo/", &templateHandler{filename: "templates/bucketinfo.html"})
	http.Handle("/buckets", &templateHandler{filename: "templates/buckets.html"})
	http.HandleFunc("/score/json", handleScore)
	http.HandleFunc("/api/response_stats", handleSche)
	http.HandleFunc("/api/partition", handlePartition)
	http.HandleFunc("/api/bucket/", handleBucket)

	// same as gobeansdb
	http.HandleFunc("/config/", handleConfig)
	http.HandleFunc("/request/", handleRequest)
	http.HandleFunc("/buffer/", handleBuffer)
	http.HandleFunc("/memstat/", handleMemStat)
	http.HandleFunc("/rusage/", handleRusage)
	http.HandleFunc("/route/", handleRoute)
	http.HandleFunc("/route/version", handleRouteVersion)
	http.HandleFunc("/route/reload", handleRouteReload)
	http.Handle(
		"/metrics",
		promhttp.HandlerFor(dstore.BdbProxyPromRegistry,
			promhttp.HandlerOpts{Registry: dstore.BdbProxyPromRegistry}),
	)
	http.HandleFunc("/cstar-cfg", handleCstarCfgReload)

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

func handleSche(w http.ResponseWriter, r *http.Request) {
	defer handleWebPanic(w)
	responseStats := dstore.GetScheduler().LatenciesStats()
	handleJson(w, responseStats)
}

func handlePartition(w http.ResponseWriter, r *http.Request) {
	defer handleWebPanic(w)
	partition := dstore.GetScheduler().Partition()
	handleJson(w, partition)
}

func handleBucket(w http.ResponseWriter, r *http.Request) {
	defer handleWebPanic(w)
	bucketID, err := getBucket(r)
	if err != nil {
	}
	bktInfo := dstore.GetScheduler().GetBucketInfo(bucketID)
	handleJson(w, bktInfo)
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
		logger.Infof("scheduler closing when reroute, request: %v", r)
		oldScheduler.Close()
	}()
}

type ReloadableCfg struct {
	Cfg map[string]string `json:"cfg"`
	Message string        `json:"message"`
	Error string          `json:"error"`
}

func handleCstarCfgReload(w http.ResponseWriter, r *http.Request) {
	defer handleWebPanic(w)

	w.Header().Set("Content-Type", "application/json")
	resp := make(map[string]string)
	cfgName := r.URL.Query().Get("config")
	var dispatcher cassandra.PrefixDisPatcher

	switch cfgName {
	case "tablefinder":
		dispatcher = dstore.PrefixTableFinder
	case "rwswitcher":
		dispatcher = dstore.PrefixStorageSwitcher
	default:
		resp["error"] = "unsupported config query arg, must be: tablefinder/rwswitcher"
		w.WriteHeader(http.StatusBadRequest)
		handleJson(w, resp)
		return
	}

	switch r.Method {
	case "GET":
		response := ReloadableCfg{
			Cfg: dispatcher.GetCurrentMap(),
		}
		response.Message = "success"
		w.WriteHeader(http.StatusOK)
		handleJson(w, response)
		return
	case "POST":
		staticCfg, err := dispatcher.LoadStaticCfg(config.Proxy.Confdir)
		if err != nil {
			resp["error"] = fmt.Sprintf("load static cfg err: %s", err)
			break
		}

		err = dispatcher.LoadCfg(staticCfg, dstore.CqlStore)
		if err != nil {
			resp["error"] = fmt.Sprintf("load cfg from db err: %s", err)
			break
		}
		resp["message"] = "ok"
	case "PUT":
		// load cfg static
		staticCfg, err := dispatcher.LoadStaticCfg(config.Proxy.Confdir)
		if err != nil {
			resp["error"] = fmt.Sprintf("load static cfg err: %s", err)
			break
		}

		// upsert new data to db
		b, err := ioutil.ReadAll(r.Body)
		if err != nil {
			resp["error"] = fmt.Sprintf("get body from req err: %s", err)
			break
		}
		defer r.Body.Close()
		var data map[string](map[string][]string)
		err = json.Unmarshal(b, &data)
		if err != nil {
			resp["error"] = fmt.Sprintf("parse req err: %s", err)
			break
		}
		pdata, ok := data["prefix"]
		if !ok {
			resp["error"] = fmt.Sprintf("parse req err: doesn't match {'prefix': {'<dispatch_to>': ['prefix1', 'prefix2']}}")
			break
		}
		err = dispatcher.Upsert(staticCfg, pdata, dstore.CqlStore)
		if err != nil {
			resp["error"] = fmt.Sprintf("upsert data %v err: %s", data, err)
			break
		}

		// require load cfg actually
		err = dispatcher.LoadCfg(staticCfg, dstore.CqlStore)
		if err != nil {
			resp["error"] = fmt.Sprintf("load cfg to server err: %s", err)
			break
		}
	case "DELETE":
		// load cfg static
		staticCfg, err := dispatcher.LoadStaticCfg(config.Proxy.Confdir)
		if err != nil {
			resp["error"] = fmt.Sprintf("load static cfg err: %s", err)
			break
		}

		// upsert new data to db
		b, err := ioutil.ReadAll(r.Body)
		if err != nil {
			resp["error"] = fmt.Sprintf("get body from req err: %s", err)
			break
		}
		defer r.Body.Close()
		var data map[string]string
		err = json.Unmarshal(b, &data)
		if err != nil {
			resp["error"] = fmt.Sprintf("parse req err: %s", err)
			break
		}

		prefix, ok := data["prefix"]
		if !ok {
			resp["error"] = fmt.Sprintf("req data should like: {'prefix': <your data>}")
			break
		}
		err = dispatcher.DeletePrefix(staticCfg, prefix, dstore.CqlStore)
		if err != nil {
			resp["error"] = fmt.Sprintf("upsert data %v err: %s", data, err)
			break
		}

		// require load cfg actually
		err = dispatcher.LoadCfg(staticCfg, dstore.CqlStore)
		if err != nil {
			resp["error"] = fmt.Sprintf("load cfg to server err: %s", err)
			break
		}
	default:
		w.WriteHeader(http.StatusBadRequest)
		resp["error"] = "unsupported method"
	}

	
	if _, ok := resp["error"]; ok {
		w.WriteHeader(http.StatusBadGateway)
	} else {
		w.WriteHeader(http.StatusOK)
		resp["message"] = "success"
	}
	handleJson(w, resp)
}
