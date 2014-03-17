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
	con := pool.Get()
	topics, err := redis.Strings(con.Do("SMEMBERS", "logtopics"))
	con.Close()
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
	con := pool.Get()
	con.Send("SADD", "logtopics", items["topic"])
	con.Send("SET", "logsetting:"+items["topic"], items["logsetting"])
	con.Receive()
	_, err := con.Receive()
	con.Close()
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
	}
}

func LogTopicShow(w http.ResponseWriter, r *http.Request) {
}
func LogTopicDelete(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "DELETE")
	name := mux.Vars(r)["name"]
	con := pool.Get()
	con.Send("SREM", "logtopics", name)
	con.Send("DEL", "logsetting:"+name)
	con.Send("DEL", "classifier:"+name)
	con.Send("DEL", "regexp:"+name)
	_, err := con.Receive()
	_, err = con.Receive()
	_, err = con.Receive()
	_, err = con.Receive()
	con.Close()
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
}
