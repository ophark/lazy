package main

import (
	"encoding/json"
	"github.com/garyburd/redigo/redis"
	"github.com/gorilla/mux"
	"log"
	"net/http"
)

// LogTopicIndex GET /logtopic
func LogTopicIndex(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=\"utf-8\"")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET")
	q := &RedisQuery{
		Action:        "SMEMBERS",
		Options:       []interface{}{"logtopics"},
		resultChannel: make(chan *QueryResult),
	}
	queryservice.queryChannel <- q
	queryresult := <-q.resultChannel
	topics, err := redis.Strings(queryresult.Value, queryresult.Err)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	if body, err := json.Marshal(topics); err == nil {
		w.Write(body)
	} else {
		w.WriteHeader(http.StatusInternalServerError)
	}
}

func LogTopicCreate(w http.ResponseWriter, r *http.Request) {
	var items map[string]string
	defer r.Body.Close()
	if err := json.NewDecoder(r.Body).Decode(&items); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		log.Println(err)
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=\"utf-8\"")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "POST")
	q := &RedisQuery{
		Action:        "SADD",
		Options:       []interface{}{"logtopics", items["topic"]},
		resultChannel: make(chan *QueryResult),
	}
	queryservice.queryChannel <- q
	queryresult := <-q.resultChannel
	if queryresult.Err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	q = &RedisQuery{
		Action:        "SET",
		Options:       []interface{}{"logsetting:" + items["topic"], items["logsetting"]},
		resultChannel: make(chan *QueryResult),
	}
	queryservice.queryChannel <- q
	queryresult = <-q.resultChannel
	if queryresult.Err != nil {
		w.WriteHeader(http.StatusInternalServerError)
	}
}

func LogTopicShow(w http.ResponseWriter, r *http.Request) {
}
func LogTopicDelete(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "DELETE")
	name := mux.Vars(r)["name"]
	q := &RedisQuery{
		Action:        "SREM",
		Options:       []interface{}{"logtopics", name},
		resultChannel: make(chan *QueryResult),
	}
	queryservice.queryChannel <- q
	queryresult := <-q.resultChannel
	if queryresult.Err != nil {
		w.WriteHeader(http.StatusInternalServerError)
	}
	q = &RedisQuery{
		Action:        "DEL",
		Options:       []interface{}{"logsetting:" + name},
		resultChannel: make(chan *QueryResult),
	}
	queryservice.queryChannel <- q
	queryresult = <-q.resultChannel
	if queryresult.Err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	q = &RedisQuery{
		Action:        "SMEMBERS",
		Options:       []interface{}{"classifiers:" + name},
		resultChannel: make(chan *QueryResult),
	}
	queryservice.queryChannel <- q
	queryresult = <-q.resultChannel
	list, _ := redis.Strings(queryresult.Value, queryresult.Err)
	for _, v := range list {
		q = &RedisQuery{
			Action:        "DEL",
			Options:       []interface{}{"classifier:" + name + ":" + v},
			resultChannel: make(chan *QueryResult),
		}
		queryservice.queryChannel <- q
		queryresult = <-q.resultChannel
		if queryresult.Err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
	}
	q = &RedisQuery{
		Action:        "DEL",
		Options:       []interface{}{"classifiers:" + name},
		resultChannel: make(chan *QueryResult),
	}
	queryservice.queryChannel <- q
	queryresult = <-q.resultChannel
	if queryresult.Err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
}
