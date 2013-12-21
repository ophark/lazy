package main

import (
	"github.com/garyburd/redigo/redis"
	"log"
	"sync"
	"time"
)

type WebLogParserPool struct {
	sync.Mutex
	*redis.Pool
	maxInFlight           int
	lookupdList           []string
	checklist             map[string]string
	logChannel            string
	elasticSearchServer   string
	elasticSearchPort     string
	elasticSearchIndex    string
	elasticSearchIndexTTL string
	exitChannel           chan int
	webLogParserList      map[string]*WebLogParser
}

func (m *WebLogParserPool) Run() {
	go m.syncWebLogs()
}

func (m *WebLogParserPool) Stop() {
	close(m.exitChannel)
	m.Lock()
	defer m.Unlock()
	for k := range m.webLogParserList {
		m.webLogParserList[k].Stop()
	}
}

func (m *WebLogParserPool) syncWebLogs() {
	ticker := time.Tick(time.Second * 600)
	m.getWebLogs()
	for {
		select {
		case <-ticker:
			m.getWebLogs()
			m.CheckWebParser()
		case <-m.exitChannel:
			return
		}
	}
}

func (m *WebLogParserPool) getWebLogs() {
	con := m.Get()
	defer con.Close()
	topics, err := redis.Strings(con.Do("SMEMBERS", "weblogtopics"))
	if err != nil {
		log.Println("fail to get topics")
		return
	}
	m.Lock()
	defer m.Unlock()
	m.checklist = make(map[string]string)
	for _, topic := range topics {
		m.checklist[topic] = topic
		if _, ok := m.webLogParserList[topic]; !ok {
			w := &WebLogParser{
				Pool:                  m.Pool,
				webLogTopic:           topic,
				webLogChannel:         m.logChannel,
				elasticSearchServer:   m.elasticSearchServer,
				elasticSearchPort:     m.elasticSearchPort,
				elasticSearchIndex:    m.elasticSearchIndex,
				elasticSearchIndexTTL: m.elasticSearchIndexTTL,
				maxInFlight:           m.maxInFlight,
				lookupdList:           m.lookupdList,
				exitChannel:           make(chan int),
				msgChannel:            make(chan Record),
			}
			if err := w.Run(); err != nil {
				log.Println(topic, err)
				continue
			}
			m.webLogParserList[topic] = w
		}
	}
}

func (m *WebLogParserPool) CheckWebParser() {
	m.Lock()
	defer m.Unlock()
	for k := range m.webLogParserList {
		if _, ok := m.checklist[k]; !ok {
			m.webLogParserList[k].Stop()
			delete(m.webLogParserList, k)
		}
	}
}
