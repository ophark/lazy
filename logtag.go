package main

import (
	"encoding/base64"
	"encoding/json"
	"github.com/garyburd/redigo/redis"
	"github.com/gorilla/mux"
	"log"
	"net/http"
)

// LogTagIndex GET /audittag
func LogTagIndex(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=\"utf-8\"")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET")
	con := redisPool.Get()
	defer con.Close()
	tags, err := redis.Strings(con.Do("SMEMBERS", "logtags"))
	if err != nil && err != redis.ErrNil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	var rst []interface{}
	for _, tag := range tags {
		query := make(map[string]interface{})
		query["name"] = tag
		query["url"] = "/api/v1/logtag/" + tag
		rst = append(rst, query)
	}
	body, _ := json.Marshal(rst)
	w.Write(body)
}

// LogTagDelete DELETE /logtag/{name}
func LogTagDelete(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "DELETE")
	tag := mux.Vars(r)["name"]
	con := redisPool.Get()
	defer con.Close()
	_, err := con.Do("DEL", "tag:"+tag)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
	}
}

// LogTagShow GET /logtag/{tagname}
func LogTagShow(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=\"utf-8\"")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET")
	tag := mux.Vars(r)["tagname"]
	con := redisPool.Get()
	defer con.Close()
	regexps, err := redis.Strings(con.Do("SMEMBERS", "tag:"+tag))
	if err != nil && err != redis.ErrNil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	rst := make(map[string]interface{})
	rst["name"] = tag
	var querys []interface{}
	for _, t := range regexps {
		query := make(map[string]interface{})
		query["regexp"] = t
		rg := base64.URLEncoding.EncodeToString([]byte(t))
		query["url"] = "/api/v1/logtag/" + tag + "/regex/" + rg
		querys = append(querys, query)
	}
	rst["regexps"] = querys
	body, _ := json.Marshal(rst)
	w.Write(body)
}

// LogTagRegexCreate POST /logtag/{tagname}/regex
func LogTagRegexCreate(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=\"utf-8\"")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "POST")
	var items map[string]string
	defer r.Body.Close()
	if err := json.NewDecoder(r.Body).Decode(&items); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		log.Println(err)
		return
	}
	tag := mux.Vars(r)["tagname"]
	con := redisPool.Get()
	defer con.Close()
	var rst []interface{}
	for _, rg := range items {
		_, err := con.Do("SADD", "tag:"+tag, rg)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		query := make(map[string]interface{})
		query["name"] = tag
		t := base64.URLEncoding.EncodeToString([]byte(rg))
		query["url"] = "/api/v1/logtag/" + tag + "/regex/" + t
		rst = append(rst, query)
	}
	body, _ := json.Marshal(rst)
	w.Write(body)
}

// LogTagRegexDelete DELETE /logtag/{tagname}/regex/{name}
func LogTagRegexDelete(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=\"utf-8\"")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "DELETE")
	tag := mux.Vars(r)["tagname"]
	name := mux.Vars(r)["name"]
	rg, err := base64.URLEncoding.DecodeString(name)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	con := redisPool.Get()
	defer con.Close()
	_, err = con.Do("SREM", "tag:"+tag, rg)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
	}
}
