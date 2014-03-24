package main

import (
	"encoding/json"
	"github.com/garyburd/redigo/redis"
	"github.com/gorilla/mux"
	"log"
	"net/http"
)

// LogTopicIndex GET /logtopic
func (q *WebAPI) LogTopicIndex(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=\"utf-8\"")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET")
	con := q.Pool.Get()
	defer con.Close()
	topics, err := redis.Strings(con.Do("SMEMBERS", "logtopics"))
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

func (q *WebAPI) LogTopicCreate(w http.ResponseWriter, r *http.Request) {
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
	con := q.Pool.Get()
	defer con.Close()
	con.Send("SADD", "logtopics", items["topic"])
	con.Send("SET", "logsetting:"+items["topic"], items["logsetting"])
	con.Flush()
	con.Receive()
	_, err := con.Receive()
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
	}
}

func (q *WebAPI) LogTopicShow(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "DELETE")
	name := mux.Vars(r)["name"]
	con := q.Pool.Get()
	defer con.Close()
	data, err := redis.Bytes(con.Do("GET", "logsetting:"+name))
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Write(data)

}
func (q *WebAPI) LogTopicDelete(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "DELETE")
	name := mux.Vars(r)["name"]
	con := q.Pool.Get()
	defer con.Close()
	con.Send("SREM", "logtopics", name)
	con.Send("DEL", "logsetting:"+name)
	con.Flush()
	_, err := con.Receive()
	_, err = con.Receive()
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
}
