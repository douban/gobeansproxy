package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"path/filepath"
	"runtime"
	"sync"
	"syscall"
	"text/template"

	"github.intra.douban.com/coresys/gobeansdb/cmem"
	mc "github.intra.douban.com/coresys/gobeansdb/memcache"
	"github.intra.douban.com/coresys/gobeansdb/utils"

	"github.intra.douban.com/coresys/gobeansproxy/dstore"
)

func handleWebPanic(w http.ResponseWriter) {
	r := recover()
	if r != nil {
		stack := utils.GetStack(2000)
		logger.Errorf("web req panic:%#v, stack:%s", r, stack)
		fmt.Fprintf(w, "\npanic:%#v, stack:%s", r, stack)
	}
}

func handleJson(w http.ResponseWriter, v interface{}) {
	defer handleWebPanic(w)
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

	http.Handle("/", &templateHandler{filename: "templates/score.html"})
	http.Handle("/score/", &templateHandler{filename: "templates/score.html"})
	http.Handle("/stats/", &templateHandler{filename: "templates/stats.html"})

	http.HandleFunc("/stats/config/", handleConfig)
	http.HandleFunc("/stats/request/", handleRequest)
	http.HandleFunc("/stats/buffer/", handleBuffer)
	http.HandleFunc("/stats/memstat/", handleMemStat)
	http.HandleFunc("/stats/rusage/", handleRusage)
	http.HandleFunc("/stats/score/", handleScore)

	webaddr := fmt.Sprintf("%s:%d", proxyConf.Listen, proxyConf.WebPort)
	go func() {
		logger.Infof("HTTP listen at %s", webaddr)
		if err := http.ListenAndServe(webaddr, nil); err != nil {
			logger.Fatalf("ListenAndServer: %s", err.Error())
		}
	}()
}

func handleConfig(w http.ResponseWriter, r *http.Request) {
	handleJson(w, proxyConf)
}

func handleRequest(w http.ResponseWriter, r *http.Request) {
	handleJson(w, mc.RL)
}

func handleRusage(w http.ResponseWriter, r *http.Request) {
	var rusage syscall.Rusage
	syscall.Getrusage(syscall.RUSAGE_SELF, &rusage)
	handleJson(w, rusage)
}

func handleMemStat(w http.ResponseWriter, r *http.Request) {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	handleJson(w, ms)
}

func handleBuffer(w http.ResponseWriter, r *http.Request) {
	handleJson(w, &cmem.DBRL)
}

func handleScore(w http.ResponseWriter, r *http.Request) {
	scores := dstore.GetScheduler().Stats()
	handleJson(w, scores)
}
