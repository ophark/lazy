package main

import (
	"github.com/garyburd/redigo/redis"
	"log"
	"sync"
	"time"
)

type LogParserPool struct {
	sync.Mutex
	*redis.Pool
	*Setting
	checklist     map[string]string
	exitChannel   chan int
	logParserList map[string]*LogParser
}

func (m *LogParserPool) Run() {
	redisCon := func() (redis.Conn, error) {
		c, err := redis.Dial("tcp", m.redisServer)
		if err != nil {
			return nil, err
		}
		return c, err
	}
	m.Pool = redis.NewPool(redisCon, 3)
	go m.syncLogTopics()
}

func (m *LogParserPool) Stop() {
	close(m.exitChannel)
	m.Pool.Close()
	m.Lock()
	defer m.Unlock()
	for k := range m.logParserList {
		m.logParserList[k].Stop()
	}
}

func (m *LogParserPool) syncLogTopics() {
	ticker := time.Tick(time.Second * 600)
	m.getLogTopics()
	for {
		select {
		case <-ticker:
			m.getLogTopics()
			m.checkLogParsers()
		case <-m.exitChannel:
			return
		}
	}
}

func (m *LogParserPool) getLogTopics() {
	con := m.Get()
	defer con.Close()
	topics, err := redis.Strings(con.Do("SMEMBERS", "logtopics"))
	if err != nil {
		log.Println("fail to get topics")
		return
	}
	m.Lock()
	defer m.Unlock()
	for _, topic := range topics {
		m.checklist[topic] = topic
		if _, ok := m.logParserList[topic]; !ok {
			w := &LogParser{
				Setting:     m.Setting,
				logTopic:    topic,
				exitChannel: make(chan int),
				msgChannel:  make(chan Record),
			}
			if err := w.Run(); err != nil {
				log.Println(topic, err)
				continue
			}
			m.logParserList[topic] = w
		}
	}
}

func (m *LogParserPool) checkLogParsers() {
	m.Lock()
	defer m.Unlock()
	for k := range m.logParserList {
		if _, ok := m.checklist[k]; !ok {
			m.logParserList[k].Stop()
			delete(m.logParserList, k)
		}
	}
}
