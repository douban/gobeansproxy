package main

import (
	"fmt"
	"net/http"
	"path/filepath"
	"sync"
	"text/template"

	"github.intra.douban.com/coresys/gobeansproxy/dstore"
)

type templateHandler struct {
	once     sync.Once
	filename string
	templ    *template.Template
}

func (t *templateHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	t.once.Do(func() {
		t.templ = template.Must(template.ParseFiles(filepath.Join(proxyConf.StaticDir, t.filename)))
	})
	data := map[string]interface{}{
		"stats": dstore.GetScheduler().Stats(),
	}
	t.templ.Execute(w, data)
}

func startWeb() {
	http.Handle("/", &templateHandler{filename: "templates/index.html"})
	http.Handle("/templates/", http.FileServer(http.Dir(proxyConf.StaticDir)))
	webaddr := fmt.Sprintf("%s:%d", proxyConf.Listen, proxyConf.WebPort)
	go func() {
		logger.Infof("HTTP listen at %s", webaddr)
		if err := http.ListenAndServe(webaddr, nil); err != nil {
			logger.Fatalf("ListenAndServer: %s", err.Error())
		}
	}()
}
