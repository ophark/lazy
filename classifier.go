package main

import (
	"encoding/json"
	"github.com/garyburd/redigo/redis"
	"github.com/gorilla/mux"
	"log"
	"net/http"
)

// ClassifierIndex GET /classifier
func ClassifierIndex(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=\"utf-8\"")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET")
	con := redisPool.Get()
	defer con.Close()
	classifiers, err := redis.Strings(con.Do("SMEMBERS", "classifiers"))
	if err != nil && err != redis.ErrNil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	rst := make(map[string][]string)
	for _, c := range classifiers {
		words, err := redis.Strings(con.Do("SMEMBERS", c))
		if err != nil && err != redis.ErrNil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		rst[c] = words
	}
	body, _ := json.Marshal(rst)
	w.Write(body)
}

// ClassifierCreate POST /classifier
func ClassifierCreate(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=\"utf-8\"")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "POST")
	var items map[string][]string
	defer r.Body.Close()
	var err error
	if err = json.NewDecoder(r.Body).Decode(&items); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		log.Println(err)
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=\"utf-8\"")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "POST")
	con := redisPool.Get()
	defer con.Close()
	for c, words := range items {
		for _, k := range words {
			_, err = con.Do("SADD", c, k)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
		}
		con.Do("SADD", "classifiers", c)
	}
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
}

// ClassifierDelete DELETE /classifier/{word}
func ClassifierDelete(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=\"utf-8\"")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "DELETE")
	word := mux.Vars(r)["word"]
	classify := mux.Vars(r)["classify"]
	con := redisPool.Get()
	defer con.Close()
	_, err := con.Do("SREM", classify, word)
	if err == nil {
		n, e := redis.Int(con.Do("SCARD", classify))
		if e == nil && n == 0 {
			_, err = con.Do("SREM", "classifiers", classify)
			if err == nil {
				_, err = con.Do("DEL", classify)
			}
		}
	}
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
	}
}
