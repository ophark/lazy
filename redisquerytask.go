package main

import (
	"github.com/garyburd/redigo/redis"
)

type RedisQueryTask struct {
	RedisServer string
	*redis.Pool
	exitChannel  chan int
	queryChannel chan *RedisQuery
}

type RedisQuery struct {
	Action        string
	Options       []interface{}
	resultChannel chan *QueryResult
}

type QueryResult struct {
	Err   error
	Value interface{}
}

func (q *RedisQueryTask) Run() {
	dial := func() (redis.Conn, error) {
		c, err := redis.Dial("tcp", q.RedisServer)
		if err != nil {
			return nil, err
		}
		return c, err
	}
	q.Pool = redis.NewPool(dial, 3)
	go q.queryTask()
}

func (q *RedisQueryTask) queryTask() {
	con := q.Get()
	defer con.Close()
	for {
		select {
		case <-q.exitChannel:
			return
		case query := <-q.queryChannel:
			value, err := con.Do(query.Action, query.Options...)
			if err != nil && err != redis.ErrNil {
				con.Close()
				con = q.Get()
			}
			query.resultChannel <- &QueryResult{
				Err:   err,
				Value: value,
			}
		}
	}
}

func (q *RedisQueryTask) Stop() {
	close(q.exitChannel)
	q.Pool.Close()
}
