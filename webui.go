package main

import (
	"flag"
	"github.com/garyburd/redigo/redis"
	"github.com/gorilla/mux"
	"log"
	"net/http"
)

var (
	confFile = flag.String("conf", "lazy.conf", "analyst config file")
)

var redisPool *redis.Pool

func main() {
	flag.Parse()
	c, err := ReadConfig(*confFile)
	if err != nil {
		log.Fatal("config parse error", err)
	}
	redisServer, _ := c["redis_server"]
	bind, _ := c["web_bind"]

	redisCon := func() (redis.Conn, error) {
		c, err := redis.Dial("tcp", redisServer)
		if err != nil {
			return nil, err
		}
		return c, err
	}
	redisPool = redis.NewPool(redisCon, 3)
	defer redisPool.Close()
	r := mux.NewRouter()
	s := r.PathPrefix("/api/v1").Subrouter()

	s.HandleFunc("/logtag", LogTagIndex).
		Methods("GET")
	s.HandleFunc("/logtag/{name}", LogTagShow).
		Methods("GET")
	s.HandleFunc("/logtag/{name}", LogTagDelete).
		Methods("DELETE")
	s.HandleFunc("/logtag/{tagname}/regex", LogTagRegexCreate).
		Methods("POST").
		Headers("Content-Type", "application/json")
	s.HandleFunc("/logtag/{tagname}/regex/{name}", LogTagRegexDelete).
		Methods("DELETE")

	s.HandleFunc("/classifier", ClassifierIndex).
		Methods("GET")
	s.HandleFunc("/classifier", ClassifierCreate).
		Methods("POST").
		Headers("Content-Type", "application/json")
	s.HandleFunc("/classifier/{classify}/{word}", ClassifierDelete).
		Methods("DELETE")

	s.HandleFunc("/logformat", LogFormatIndex).
		Methods("GET")
	s.HandleFunc("/logformat", LogFormatCreate).
		Methods("POST").
		Headers("Content-Type", "application/json")
	s.HandleFunc("/logformat/{name}", LogFormatDelete).
		Methods("DELETE")

	http.Handle("/", r)
	err = http.ListenAndServe(bind, nil)
	if err != nil {
		log.Println(err)
	}
}
