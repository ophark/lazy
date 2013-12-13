package main

import (
	"github.com/bitly/go-nsq"
	"github.com/garyburd/redigo/redis"
	"github.com/jbrukh/bayesian"
	"github.com/jeromer/syslogparser/rfc3164"
	"github.com/mattbaird/elastigo/api"
	"github.com/mattbaird/elastigo/core"
	"log"
	"regexp"
	"strings"
	"sync"
	"time"
)

// Analyzer define a syslog analyzer node
type Analyzer struct {
	*redis.Pool
	*nsq.Writer
	c                   *bayesian.Classifier
	trainTopic          string
	trainChannel        string
	elasticSearchServer string
	elasticSearchPort   string
	elasticSearchIndex  string
	msgChannel          chan Record
	regexMap            map[string][]*regexp.Regexp
	sync.Mutex
}

// Record is used to pass data to elasticsearch
type Record struct {
	logType    string
	body       map[string]interface{}
	errChannel chan error
}

// HandleMessage is nsq reader's handle
func (m *Analyzer) HandleMessage(msg *nsq.Message) error {
	p := rfc3164.NewParser(msg.Body)
	if err := p.Parse(); err != nil {
		log.Println(err, string(msg.Body))
		return nil
	}
	message := p.Dump()
	words := m.parseLog(message["content"].(string))
	tag := message["tag"].(string)
	if len(tag) == 0 {
		message["tag"] = "misc"
	}
	con := m.Get()
	defer con.Close()
	con.Do("SADD", "logtags", message["tag"])
	record := Record{
		errChannel: make(chan error),
		logType:    "normal",
	}
	m.Lock()
	_, likely, strict := m.c.LogScores(words)
	rg, ok := m.regexMap[tag]
	m.Unlock()
	if strict && likely > 0 {
		record.logType = "error"
	}
	if !strict {
		m.Publish(m.trainTopic, msg.Body)
		log.Println("failed bayes", string(msg.Body))
	}
	if ok {
		for _, r := range rg {
			if r.MatchString(message["content"].(string)) {
				record.logType = "passregexp"
				break
			}
		}
	}
	record.body = message
	if record.logType != "passregexp" {
		log.Println("regexp check failed", string(msg.Body))
	}
	m.msgChannel <- record
	return <-record.errChannel
}

func (m *Analyzer) elasticSearchBuildIndex() {
	api.Domain = m.elasticSearchServer
	api.Port = m.elasticSearchPort
	indexor := core.NewBulkIndexorErrors(10, 60)
	done := make(chan bool)
	indexor.Run(done)
	defer close(indexor.ErrorChannel)
	go func() {
		for errBuf := range indexor.ErrorChannel {
			log.Println(errBuf.Err)
		}
	}()
	count := 0
	var err error
	for r := range m.msgChannel {
		err = indexor.Index(m.elasticSearchIndex, r.logType, "", "", nil, r.body)
		r.errChannel <- err
		count ++
		if count > 20 {
			done <- true
			indexor.Run(done)
			count = 0
		}
	}
}
func (m *Analyzer) parseLog(msg string) []string {
	re := regexp.MustCompile("\\(|\\)|{|}|/")
	t := strings.Split(re.ReplaceAllString(msg, " "), " ")
	var tokens []string
	for _, v := range t {
		tokens = append(tokens, strings.ToLower(v))
	}
	return tokens
}

func (m *Analyzer) syncBayes() {
	ticker := time.Tick(time.Second * 600)
	for {
		<-ticker
		m.getBayes()
	}
}

func (m *Analyzer) syncRegexp() {
	ticker := time.Tick(time.Second * 600)
	for {
		<-ticker
		m.getRegexp()
	}
}

func (m *Analyzer) getBayes() {
	var NormalLog = bayesian.Class("NormalLog")
	var ErrorLog = bayesian.Class("ErrorLog")
	con := m.Get()
	defer con.Close()
	n, _ := redis.Strings(con.Do("SMEMBERS", "NormalLog"))
	e, _ := redis.Strings(con.Do("SMEMBERS", "ErrorLog"))
	m.Lock()
	defer m.Unlock()
	m.c = bayesian.NewClassifier(NormalLog, ErrorLog)
	m.c.Learn(n, NormalLog)
	m.c.Learn(e, ErrorLog)
}

func (m *Analyzer) getRegexp() {
	con := m.Get()
	defer con.Close()
	t, e := redis.Strings(con.Do("SMEMBERS", "logtags"))
	if e != nil {
		return
	}
	for _, value := range t {
		r, _ := redis.Strings(con.Do("SMEMBERS", "tag:"+value))
		var rg []*regexp.Regexp
		for _, v := range r {
			x, e := regexp.CompilePOSIX(v)
			if e != nil {
				log.Println(r, e)
				continue
			}
			rg = append(rg, x)
		}
		m.Lock()
		m.regexMap[value] = rg
		m.Unlock()
	}
}

/*
type Learner struct {
	*redis.Pool
	probably.Sketch
	learnChannel chan []string
}

func (m *Learner) HandleMessage(msg *nsq.Message) error {
	return nil
}

func (m *Learner) Learn() {
	chaos := bayesian.Class("Chaos")
	sequence := bayesian.Class("Sequence")
	c := bayesian.NewClassifier(chaos, sequence)
	ticker := time.Tick(time.Second * 30)
	con := m.Get()
	defer con.Close()
	turn := 0
	for {
		select {
		case l := <-m.learnChannel:
		}
		turn++
	}
}
*/
