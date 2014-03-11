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

var queryservice *RedisQueryTask

var sessionservice *sessions.RedisStore

type Task interface {
	Stop()
}

func main() {
	flag.Parse()
	c, err := ReadConfig(*confFile)
	if err != nil {
		log.Fatal("config parse error", err)
	}
	var tasks []Task
	queryservice = &RedisQueryTask{
		RedisServer:  c.redisServer,
		exitChannel:  make(chan int),
		queryChannel: make(chan *RedisQuery),
	}
	queryservice.Run()
	tasks = append(tasks, queryservice)
	for _, v := range c.modes {
		switch v {
		case "logparser":
			logParserPool := &LogParserPool{
				Setting:       c,
				exitChannel:   make(chan int),
				checklist:     make(map[string]string),
				logParserList: make(map[string]*LogParser),
			}
			logParserPool.Run()
			tasks = append(tasks, logParserPool)
		case "webui":
			r := mux.NewRouter()
			s := r.PathPrefix("/api/v1").Subrouter()

			s.HandleFunc("/logtopic", LogTopicIndex).Methods("GET")
			s.HandleFunc("/logtopic", LogTopicCreate).Methods("POST").Headers("Content-Type", "application/json")
			s.HandleFunc("/logtopic/{name}", LogTopicShow).Methods("GET")
			s.HandleFunc("/logtopic/{name}", LogTopicDelete).Methods("DELETE")
			/*
				s.HandleFunc("/logtopic/{name}/c", ClassifierIndex).Methods("GET")
				s.HandleFunc("/logtopic/{name}/c", ClassifierCreate).Methods("POST").Headers("Content-Type", "application/json")
				s.HandleFunc("/logtopic/{name}/c/{classifier}", ClassifierShow).Methods("GET")
				s.HandleFunc("/logtopic/{name}/c/{classifier}", ClassifierWordCreate).Methods("POST")
				s.HandleFunc("/logtopic/{name}/c/{classifier}", ClassifierDelete).Methods("DELETE")
				s.HandleFunc("/logtopic/{name}/c/{classifier}/{word}", ClassifierWordDelete).Methods("DELETE")

				s.HandleFunc("/logtopic/{name}/r", RegexpIndex).Methods("GET")
				s.HandleFunc("/logtopic/{name}/r", RegexpCreate).Methods("POST").Headers("Content-Type", "application/json")
				s.HandleFunc("/logtopic/{name}/r/{regexp}", RegexpShow).Methods("GET")
				s.HandleFunc("/logtopic/{name}/r/{regexp}", RegexpRuleCreate).Methods("POST")
				s.HandleFunc("/logtopic/{name}/r/{regexp}", RegexpDelete).Methods("DELETE")
				s.HandleFunc("/logtopic/{name}/t/{regexp}/{rule}", RegexpRuleDelete).Methods("DELETE")
			*/
			http.Handle("/", r)
			go http.ListenAndServe(c.listenAddress, nil)

		}
	}
	termchan := make(chan os.Signal, 1)
	signal.Notify(termchan, syscall.SIGINT, syscall.SIGTERM)
	<-termchan
	for _, t := range tasks {
		t.Stop()
	}
}
