package main

import (
	"encoding/json"
	"github.com/garyburd/redigo/redis"
	"github.com/gorilla/mux"
	"log"
	"net/http"
)

// BayesIndex GET /bayes
func BayesIndex(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=\"utf-8\"")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET")
	con := redisPool.Get()
	defer con.Close()
	n, _ := redis.Strings(con.Do("SMEMBERS", "NormalLog"))
	e, _ := redis.Strings(con.Do("SMEMBERS", "ErrorLog"))
	rst := make(map[string][]string)
	rst["normal_word"] = n
	rst["error_word"] = e
	body, _ := json.Marshal(rst)
	w.Write(body)
}

// BayesNormalCreate POST /bayes/normal
func BayesNormalCreate(w http.ResponseWriter, r *http.Request) {
	var items []string
	defer r.Body.Close()
	if err := json.NewDecoder(r.Body).Decode(&items); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		log.Println(err)
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=\"utf-8\"")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "POST")
	con := redisPool.Get()
	defer con.Close()
	for _, k := range items {
		_, err := con.Do("SADD", "NormalLog", k)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			break
		}
	}
}

// BayesNormalDelete DELETE /bayes/normal/{word}
func BayesNormalDelete(w http.ResponseWriter, r *http.Request) {
	var items []string
	defer r.Body.Close()
	if err := json.NewDecoder(r.Body).Decode(&items); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		log.Println(err)
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=\"utf-8\"")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "DELETE")
	word := mux.Vars(r)["word"]
	con := redisPool.Get()
	defer con.Close()
	_, err := con.Do("SREM", "NormalLog", word)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
	}
}

// BayesErrorCreate POST /bayes/error
func BayesErrorCreate(w http.ResponseWriter, r *http.Request) {
	var items []string
	defer r.Body.Close()
	if err := json.NewDecoder(r.Body).Decode(&items); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		log.Println(err)
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=\"utf-8\"")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "POST")
	con := redisPool.Get()
	defer con.Close()
	for _, k := range items {
		_, err := con.Do("SADD", "ErrorLog", k)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			break
		}
	}
}

// BayesErrorDelete DELETE /bayes/normal/{word}
func BayesErrorDelete(w http.ResponseWriter, r *http.Request) {
	var items []string
	defer r.Body.Close()
	if err := json.NewDecoder(r.Body).Decode(&items); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		log.Println(err)
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=\"utf-8\"")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "DELETE")
	word := mux.Vars(r)["word"]
	con := redisPool.Get()
	defer con.Close()
	_, err := con.Do("SREM", "ErrorLog", word)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
	}
}
