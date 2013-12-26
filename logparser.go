package main

import (
	"encoding/json"
	"github.com/bitly/go-nsq"
	"github.com/garyburd/redigo/redis"
	"github.com/mattbaird/elastigo/api"
	"github.com/mattbaird/elastigo/core"
	"log"
	"sync"
	"time"
)

type LogParser struct {
	sync.Mutex
	*redis.Pool
	maxInFlight           int
	lookupdList           []string
	elasticSearchServer   string
	elasticSearchPort     string
	elasticSearchIndex    string
	elasticSearchIndexTTL string
	reader                *nsq.Reader
	logFormat             *LogFormat
	logTopic              string
	logChannel            string
	exitChannel           chan int
	msgChannel            chan Record
}

func (m *LogParser) Run() error {
	m.getLogFormat()
	go m.elasticSearchBuildIndex()
	var err error
	m.reader, err = nsq.NewReader(m.logTopic, m.logChannel)
	if err != nil {
		log.Println(m.logTopic, err)
		return err
	}
	m.reader.SetMaxInFlight(m.maxInFlight)
	for i := 0; i < m.maxInFlight; i++ {
		m.reader.AddHandler(m)
	}
	for _, addr := range m.lookupdList {
		err := m.reader.ConnectToLookupd(addr)
		if err != nil {
			return err
		}
	}
	go m.syncLogFormat()
	return err
}

func (m *LogParser) Stop() {
	m.reader.Stop()
	close(m.exitChannel)
}

func (m *LogParser) HandleMessage(msg *nsq.Message) error {
	m.Lock()
	defer m.Unlock()
	rst := generateLogTokens(msg.Body)
	message, err := m.logFormat.Parser(rst)
	if err != nil {
		log.Println(err)
		return nil
	}
	record := Record{
		errChannel: make(chan error),
		body:       message,
		ttl:        m.elasticSearchIndexTTL,
		logType:    m.logTopic,
	}
	m.msgChannel <- record
	return <-record.errChannel
}

func (m *LogParser) getLogFormat() {
	con := m.Get()
	defer con.Close()
	body, e := con.Do("GET", "logformat:"+m.logTopic)
	if e != nil {
		return
	}
	m.Lock()
	defer m.Unlock()
	var logFormat LogFormat
	if err := json.Unmarshal(body.([]byte), &logFormat); err == nil {
		m.logFormat = &logFormat
	}

}

func (m *LogParser) syncLogFormat() {
	ticker := time.Tick(time.Second * 600)
	for {
		select {
		case <-ticker:
			m.getLogFormat()
		case <-m.exitChannel:
			return
		}
	}
}

func (m *LogParser) elasticSearchBuildIndex() {
	api.Domain = m.elasticSearchServer
	api.Port = m.elasticSearchPort
	indexor := core.NewBulkIndexorErrors(10, 60)
	done := make(chan bool)
	indexor.Run(done)
	var err error
	for {
		select {
		case errBuf := <-indexor.ErrorChannel:
			log.Println(errBuf.Err)
		case r := <-m.msgChannel:
			err = indexor.Index(m.elasticSearchIndex, r.logType, "", r.ttl, nil, r.body)
			r.errChannel <- err
		case <-m.exitChannel:
			break
		}
	}
	done <- true
}
