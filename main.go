package main

import (
	"flag"
	"github.com/datastream/sessions"
	"github.com/gorilla/mux"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
)

var (
	confFile = flag.String("conf", "lazy.json", "lazy config file")
)

var sessionservice *sessions.RedisStore

func main() {
	flag.Parse()
	c, err := ReadConfig(*confFile)
	if err != nil {
		log.Fatal("config parse error", err)
	}
	var logParserPool *LogParserPool
	for _, v := range c.Modes {
		switch v {
		case "logparser":
			logParserPool = &LogParserPool{
				Setting:       c,
				exitChannel:   make(chan int),
				checklist:     make(map[string]string),
				logParserList: make(map[string]*LogParser),
			}
			logParserPool.Run()
		case "webui":
			webapi := &WebAPI{
				Setting: c,
			}
			webapi.Run()
			r := mux.NewRouter()
			s := r.PathPrefix("/api/v1").Subrouter()

			s.HandleFunc("/logtopic", webapi.LogTopicIndex).Methods("GET")
			s.HandleFunc("/logtopic", webapi.LogTopicCreate).Methods("POST").Headers("Content-Type", "application/json")
			s.HandleFunc("/logtopic/{name}", webapi.LogTopicShow).Methods("GET")
			s.HandleFunc("/logtopic/{name}", webapi.LogTopicDelete).Methods("DELETE")
			s.HandleFunc("/c", webapi.ClassifierIndex).Methods("GET")
			s.HandleFunc("/c", webapi.ClassifierCreate).Methods("POST").Headers("Content-Type", "application/json")
			s.HandleFunc("/c/{name}", webapi.ClassifierShow).Methods("GET")
			s.HandleFunc("/c/{name}/word", webapi.ClassifierWordCreate).Methods("POST")
			s.HandleFunc("/c/{name}", webapi.ClassifierDelete).Methods("DELETE")
			s.HandleFunc("/c/{name}/word/{word}", webapi.ClassifierWordDelete).Methods("DELETE")

			s.HandleFunc("/r", webapi.RegexpIndex).Methods("GET")
			s.HandleFunc("/r", webapi.RegexpCreate).Methods("POST").Headers("Content-Type", "application/json")
			s.HandleFunc("/r/{name}", webapi.RegexpShow).Methods("GET")
			s.HandleFunc("/r/{name}/rule", webapi.RegexpRuleCreate).Methods("POST")
			s.HandleFunc("/r/{name}", webapi.RegexpDelete).Methods("DELETE")
			s.HandleFunc("/r/{name}/rule/{regexp}", webapi.RegexpRuleDelete).Methods("DELETE")
			http.Handle("/", r)
			go http.ListenAndServe(c.ListenAddress, nil)

		}
	}
	termchan := make(chan os.Signal, 1)
	signal.Notify(termchan, syscall.SIGINT, syscall.SIGTERM)
	<-termchan
	logParserPool.Stop()
}
