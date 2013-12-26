package main

import (
	"encoding/json"
	"github.com/garyburd/redigo/redis"
	"github.com/gorilla/mux"
	"log"
	"net/http"
)

// LogFormatIndex GET /logformat
func LogFormatIndex(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=\"utf-8\"")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET")
	con := redisPool.Get()
	defer con.Close()
	logs, err := redis.Strings(con.Do("SMEMBERS", "logtopics"))
	if err != nil && err != redis.ErrNil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	rst := make(map[string]LogFormat)
	for _, l := range logs {
		f, err := redis.String(con.Do("GET", "logformat:"+l))
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		var lf LogFormat
		err = json.Unmarshal([]byte(f), &lf)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		rst[l] = lf
	}
	body, _ := json.Marshal(rst)
	w.Write(body)
}

// LogFormatCreate POST /logformat
func LogFormatCreate(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=\"utf-8\"")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "POST")
	defer r.Body.Close()
	var items map[string]LogFormat
	var err error
	if err = json.NewDecoder(r.Body).Decode(&items); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		log.Println(err)
		return
	}
	con := redisPool.Get()
	defer con.Close()
	for k, v := range items {
		body, _ := json.Marshal(v)
		_, err = con.Do("SET", "logformat:"+k, body)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		_, err = con.Do("SADD", "logtopics", k)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
	}
}

// LogFormatDelete DELETE /logformat
func LogFormatDelete(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=\"utf-8\"")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "DELETE")
	name := mux.Vars(r)["name"]
	con := redisPool.Get()
	defer con.Close()
	_, err := con.Do("SREM", "logtopics", name)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	_, err = con.Do("DEL", "logformat:"+name)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
}
