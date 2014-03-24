package main

import (
	"github.com/garyburd/redigo/redis"
)

type WebAPI struct {
	*redis.Pool
	*Setting
}

func (w *WebAPI) Run() {
	redisCon := func() (redis.Conn, error) {
		c, err := redis.Dial("tcp", w.RedisServer)
		if err != nil {
			return nil, err
		}
		return c, err
	}
	w.Pool = redis.NewPool(redisCon, 3)
}
