package main

import (
	"encoding/json"
	"github.com/garyburd/redigo/redis"
	"github.com/gorilla/mux"
	"log"
	"net/http"
)

// ClassifierIndex GET /c
func (q *WebAPI) ClassifierIndex(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=\"utf-8\"")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET")
	con := q.Pool.Get()
	defer con.Close()
	classifiers, err := redis.Strings(con.Do("SMEMBERS", "classifiers"))
	if err != nil && err != redis.ErrNil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	rst := make(map[string][]string)
	for _, c := range classifiers {
		words, err := redis.Strings(con.Do("SMEMBERS", "classifier:"+c))
		if err != nil && err != redis.ErrNil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		rst[c] = words
	}
	body, _ := json.Marshal(rst)
	w.Write(body)
}

func (q *WebAPI) ClassifierShow(w http.ResponseWriter, r *http.Request) {
}

// ClassifierCreate POST /c
func (q *WebAPI) ClassifierCreate(w http.ResponseWriter, r *http.Request) {
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
	con := q.Pool.Get()
	defer con.Close()
	for c, words := range items {
		for _, k := range words {
			_, err = con.Do("SADD", "classifier:"+c, k)
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
func (q *WebAPI) ClassifierDelete(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=\"utf-8\"")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "DELETE")
	classify := mux.Vars(r)["name"]
	con := q.Pool.Get()
	defer con.Close()
	_, err := con.Do("DEL", "classify:"+classify)
	if err == nil {
		_, err = con.Do("SREM", "classifys", classify)
	}
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
	}
}
func (q *WebAPI) ClassifierWordCreate(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=\"utf-8\"")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "POST")
	c := mux.Vars(r)["name"]
	var words []string
	defer r.Body.Close()
	var err error
	if err = json.NewDecoder(r.Body).Decode(&words); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		log.Println(err)
		return
	}
	con := q.Pool.Get()
	defer con.Close()
	for _, k := range words {
		_, err = con.Do("SADD", "classifier:"+c, k)
	}
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
}
func (q *WebAPI) ClassifierWordDelete(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=\"utf-8\"")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "DELETE")
	word := mux.Vars(r)["word"]
	classify := mux.Vars(r)["name"]
	con := q.Pool.Get()
	defer con.Close()
	_, err := con.Do("SREM", "classify:"+classify, word)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
	}
}
