package main

import (
	"encoding/json"
	"fmt"
	"github.com/hashicorp/consul/api"
	"log"
	"sync"
	"time"
)

type LogParserPool struct {
	sync.Mutex
	*Setting
	checklist     map[string]string
	exitChannel   chan int
	logParserList map[string]*LogParser
}

func (m *LogParserPool) Stop() {
	close(m.exitChannel)
	m.Lock()
	defer m.Unlock()
	for k := range m.logParserList {
		m.logParserList[k].Stop()
	}
}

func (m *LogParserPool) Run() {
	ticker := time.Tick(time.Second * 600)
	err := m.getLogTopics()
	if err != nil {
		log.Println(err)
	}
	for {
		select {
		case <-ticker:
			err = m.getLogTopics()
			if err != nil {
				log.Println(err)
			}
			m.checkLogParsers()
		case <-m.exitChannel:
			return
		}
	}
}

//"%s/topics"
func (m *LogParserPool) getLogTopics() error {
	config := api.DefaultConfig()
	config.Address = m.ConsulAddress
	config.Datacenter = m.Datacenter
	config.Token = m.Token
	client, err := api.NewClient(config)
	if err != nil {
		return err
	}
	kv := client.KV()
	topicsKey := fmt.Sprintf("%s/topics", m.ConsulKey)
	pairs, _, err := kv.List(topicsKey, nil)
	if err != nil {
		return err
	}
	size := len(topicsKey) + 1
	m.Lock()
	defer m.Unlock()
	for _, value := range pairs {
		topic := value.Key[size:]
		m.checklist[topic] = topic
		if _, ok := m.logParserList[topic]; !ok {
			var logSetting LogSetting
			err = json.Unmarshal(value.Value, &logSetting)
			if err != nil {
				return err
			}
			w := &LogParser{
				Setting:     m.Setting,
				logTopic:    topic,
				regexMap:    make(map[string][]*RegexpSetting),
				exitChannel: make(chan int),
				msgChannel:  make(chan ElasticRecord),
				logSetting:  &logSetting,
			}
			if err := w.Run(); err != nil {
				log.Println(topic, err)
				continue
			}
			m.logParserList[topic] = w
		}
	}
	return nil
}

func (m *LogParserPool) checkLogParsers() {
	m.Lock()
	defer m.Unlock()
	for k := range m.logParserList {
		if _, ok := m.checklist[k]; !ok {
			m.logParserList[k].Stop()
			log.Println("remove:", k)
			delete(m.logParserList, k)
		}
	}
}
