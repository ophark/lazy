package main

import (
	"encoding/json"
	"github.com/bitly/go-nsq"
	"github.com/garyburd/redigo/redis"
	"github.com/mattbaird/elastigo/api"
	"github.com/mattbaird/elastigo/core"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"
)

type WebLogParser struct {
	*redis.Pool
	reader                *nsq.Reader
	maxInFlight           int
	lookupdList           []string
	elasticSearchServer   string
	elasticSearchPort     string
	elasticSearchIndex    string
	elasticSearchIndexTTL string
	webLogFormat          *LogFormat
	webLogTopic           string
	webLogChannel         string
	exitChannel           chan int
	msgChannel            chan Record
	sync.Mutex
}

func (m *WebLogParser) Run() error {
	m.getLogFormat()
	go m.elasticSearchBuildIndex()
	var err error
	m.reader, err = nsq.NewReader(m.webLogTopic, m.webLogChannel)
	if err != nil {
		log.Println(m.webLogTopic, err)
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

func (m *WebLogParser) Stop() {
	m.reader.Stop()
	close(m.exitChannel)
}

func (m *WebLogParser) HandleMessage(msg *nsq.Message) error {
	m.Lock()
	defer m.Unlock()
	rst := parserWebLog(msg.Body)
	if len(m.webLogFormat.Names) != len(rst) {
		log.Println("format error:", m.webLogFormat.Names, rst)
	} else {
		message := make(map[string]interface{})
		for i := 0; i < len(m.webLogFormat.Names); i++ {
			key := m.webLogFormat.Names[i]
			if value, ok := m.webLogFormat.Values[key]; ok {
				switch value {
				case "int":
					t, err := strconv.ParseInt(string(rst[i]), 10, 32)
					if err != nil {
						log.Println("log token err:", rst[i], " do not match ", key)
						return nil
					}
					message[key] = t
				case "strings":
					k := strings.Split(key, " ")
					v := strings.Split(string(rst[i]), " ")
					if len(k) != len(v) {
						log.Println("log token err", k, v)
						return nil
					}
					for l := 0; l < len(k); l++ {
						message[k[l]] = v[l]
					}
				case "float":
					t, err := strconv.ParseFloat(string(rst[i]), 64)
					if err != nil {
						log.Println("log token err:", rst[i], " do not match ", key)
						return nil
					}
					message[key] = t
				default:
					message[key] = string(rst[i])
				}
			}
		}
		record := Record{
			errChannel: make(chan error),
			body:       message,
			logType:    m.webLogTopic,
		}
		m.msgChannel <- record
		return <-record.errChannel
	}
	return nil
}

func (m *WebLogParser) getLogFormat() {
	con := m.Get()
	defer con.Close()
	body, e := con.Do("GET", "weblogformat:"+m.webLogTopic)
	if e != nil {
		return
	}
	m.Lock()
	defer m.Unlock()
	var logFormat LogFormat
	if err := json.Unmarshal(body.([]byte), &logFormat); err == nil {
		m.webLogFormat = &logFormat
	}

}

func (m *WebLogParser) syncLogFormat() {
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

func (m *WebLogParser) elasticSearchBuildIndex() {
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
			err = indexor.Index(m.elasticSearchIndex, r.logType, "", m.elasticSearchIndexTTL, nil, r.body)
			r.errChannel <- err
		case <-m.exitChannel:
			break
		}
	}
	done <- true
}

func parserWebLog(buf []byte) [][]byte {
	var tokens [][]byte
	token := make([]byte, 0)
	var lastChar byte
	for _, v := range buf {
		switch v {
		case byte(' '):
			fallthrough
		case byte('['):
			fallthrough
		case byte(']'):
			fallthrough
		case byte('"'):
			if len(token) > 0 {
				if token[len(token)-1] == byte('\\') {
					token = append(token, v)
					continue
				}
				if lastChar == byte('"') {
					if v != byte('"') {
						token = append(token, v)
						continue
					}
				}
				if lastChar == byte('[') {
					if v != byte(']') {
						token = append(token, v)
						continue
					}
				}
				tokens = append(tokens, token)
				token = make([]byte, 0)
			} else {
				if lastChar == byte('"') {
					if v == byte('"') {
						tokens = append(tokens, token)
						token = make([]byte, 0)
					}
				}
				if lastChar == byte('[') {
					if v == byte(']') {
						tokens = append(tokens, token)
						token = make([]byte, 0)
					}
				}
			}
			lastChar = v
		default:
			token = append(token, v)
		}
	}
	return tokens
}
