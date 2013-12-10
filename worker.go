package main

import (
	"github.com/bitly/go-nsq"
	"github.com/dustin/go-probably"
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

type Analyzer struct {
	*redis.Pool
	*nsq.Writer
	*probably.Sketch
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

type Record struct {
	logType    string
	body       map[string]interface{}
	errChannel chan error
}

func (m *Analyzer) HandleMessage(msg *nsq.Message) error {
	p := rfc3164.NewParser(msg.Body)
	if err := p.Parse(); err != nil {
		log.Println(err, string(msg.Body))
		return nil
	}
	message := p.Dump()
	words := m.parseLog(message["content"].(string))
	if len(message["tag"].(string)) == 0 {
		message["tag"] = "misc"
	}
	tag := message["tag"].(string)
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
	stat := false
	if ok {
		for _, r := range rg {
			if r.MatchString(message["content"].(string)) {
				stat = true
				break
			}
		}
	}
	record.body = message
	if !stat {
		log.Println("do nothing with", string(msg.Body))
	}
	m.msgChannel <- record
	return <-record.errChannel
}

func (m *Analyzer) elasticSearchBuildIndex() {
	api.Domain = m.elasticSearchServer
	api.Port = m.elasticSearchPort
	for r := range m.msgChannel {
		_, err := core.Index(true, m.elasticSearchIndex, r.logType, "", r.body)
		r.errChannel <- err
	}
}
func (m *Analyzer) parseLog(msg string) []string {
	re := regexp.MustCompile("\\(|\\)|{|}|/")
	t := strings.Split(re.ReplaceAllString(msg, " "), " ")
	var tokens []string
	for _, v := range t {
		if len(v) == 1 {
			continue
		}
		tokens = append(tokens, strings.ToLower(v))
		log.Println(v, m.ConservativeIncrement(strings.ToLower(v)))
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
