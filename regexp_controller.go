package main

import (
	"encoding/base64"
	"encoding/json"
	"github.com/garyburd/redigo/redis"
	"github.com/gorilla/mux"
	"log"
	"net/http"
)

// ClassifierIndex GET /r
func (q *WebAPI) RegexpIndex(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=\"utf-8\"")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET")
	con := q.Pool.Get()
	defer con.Close()
	tags, err := redis.Strings(con.Do("SMEMBERS", "logtags"))
	if err != nil && err != redis.ErrNil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	rst := make(map[string][]string)
	for _, c := range tags {
		words, err := redis.Strings(con.Do("SMEMBERS", "logtag:"+c))
		if err != nil && err != redis.ErrNil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		rst[c] = words
	}
	body, _ := json.Marshal(rst)
	w.Write(body)

}
func (q *WebAPI) RegexpCreate(w http.ResponseWriter, r *http.Request) {
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
	for c, rules := range items {
		for _, k := range rules {
			var status map[string]string
			if err := json.Unmarshal([]byte(k), &status); err != nil {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			if len(status["regexp"]) > 0 && len(status["ttl"]) > 0 {
				_, err = con.Do("SADD", "logtag:"+c, k)
				if err != nil {
					w.WriteHeader(http.StatusInternalServerError)
					return
				}
			} else {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
		}
		con.Do("SADD", "logtags", c)
	}
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
}
func (q *WebAPI) RegexpShow(w http.ResponseWriter, r *http.Request) {
}
func (q *WebAPI) RegexpDelete(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=\"utf-8\"")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "DELETE")
	tag := mux.Vars(r)["name"]
	con := q.Pool.Get()
	defer con.Close()
	_, err := con.Do("DEL", "logtag:"+tag)
	if err == nil {
		_, err = con.Do("SREM", "logtags", tag)
	}
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
	}
}
func (q *WebAPI) RegexpRuleCreate(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=\"utf-8\"")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "POST")
	c := mux.Vars(r)["name"]
	var rules []string
	defer r.Body.Close()
	var err error
	if err = json.NewDecoder(r.Body).Decode(&rules); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		log.Println(err)
		return
	}
	con := q.Pool.Get()
	defer con.Close()
	for _, k := range rules {
		var status map[string]string
		if err := json.Unmarshal([]byte(k), &status); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if len(status["regexp"]) > 0 && len(status["ttl"]) > 0 {
			_, err = con.Do("SADD", "logtag:"+c, k)
		}
	}
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
}
func (q *WebAPI) RegexpRuleDelete(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=\"utf-8\"")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "DELETE")
	name := mux.Vars(r)["name"]
	con := q.Pool.Get()
	defer con.Close()
	tag, err := base64.URLEncoding.DecodeString(name)
	if err == nil {
		_, err = con.Do("DEL", "logtag:"+string(tag))
	}
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
	}
}
