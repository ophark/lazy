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
	reader                *nsq.Reader
	writer                *nsq.Writer
	classifiers           []string
	c                     *bayesian.Classifier
	analyzerTopic         string
	analyzerChannel       string
	maxInFlight           int
	lookupdList           []string
	trainTopic            string
	trainChannel          string
	elasticSearchServer   string
	elasticSearchPort     string
	elasticSearchIndex    string
	elasticSearchIndexTTL string
	msgChannel            chan Record
	exitChannel           chan int
	regexMap              map[string][]*regexp.Regexp
	sync.Mutex
}

func (m *Analyzer) Run() error {
	m.getRegexp()
	m.getBayes()
	go m.elasticSearchBuildIndex()
	var err error
	m.reader, err = nsq.NewReader(m.analyzerTopic, m.analyzerChannel)
	if err != nil {
		return err
	}
	m.reader.SetMaxInFlight(m.maxInFlight)
	for i := 0; i < m.maxInFlight; i++ {
		m.reader.AddHandler(m)
	}
	for _, addr := range m.lookupdList {
		err = m.reader.ConnectToLookupd(addr)
		if err != nil {
			return err
		}
	}
	go m.syncRegexp()
	go m.syncBayes()
	return err
}

func (m *Analyzer) Stop() {
	m.reader.Stop()
	m.writer.Stop()
	close(m.exitChannel)
	m.Pool.Close()
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
		tag = "misc"
	} else {
		tag = strings.Replace(tag, ".", "", -1)
	}
	record := Record{
		errChannel: make(chan error),
		ttl:        m.elasticSearchIndexTTL,
		logType:    "chaos",
	}
	m.Lock()
	_, likely, strict := m.c.LogScores(words)
	rg, ok := m.regexMap[tag]
	if strict {
		record.logType = m.classifiers[likely]
	} else {
		m.writer.Publish(m.trainTopic, msg.Body)
	}
	m.Unlock()
	if ok {
		for _, r := range rg {
			if r.MatchString(message["content"].(string)) {
				if record.logType == "chaos" {
					record.logType = "regexp"
				} else {
					record.logType += "_regexp"
				}
				break
			}
		}
	}
	record.body = message
	m.msgChannel <- record
	return <-record.errChannel
}

func (m *Analyzer) elasticSearchBuildIndex() {
	api.Domain = m.elasticSearchServer
	api.Port = m.elasticSearchPort
	indexor := core.NewBulkIndexorErrors(10, 60)
	done := make(chan bool)
	indexor.Run(done)
	var err error
	con := m.Get()
	defer con.Close()
	for {
		select {
		case errBuf := <-indexor.ErrorChannel:
			log.Println(errBuf.Err)
		case r := <-m.msgChannel:
			err = indexor.Index(m.elasticSearchIndex, r.logType, "", r.ttl, nil, r.body)
			con.Do("SADD", "logtags", r.body["tag"])
			r.errChannel <- err
		case <-m.exitChannel:
			break
		}
	}
	done <- true
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
		select {
		case <-ticker:
			m.getBayes()
		case <-m.exitChannel:
			return
		}
	}
}

func (m *Analyzer) syncRegexp() {
	ticker := time.Tick(time.Second * 600)
	for {
		select {
		case <-ticker:
			m.getRegexp()
		case <-m.exitChannel:
			return
		}
	}
}

func (m *Analyzer) getBayes() {
	con := m.Get()
	defer con.Close()
	classifiers, err := redis.Strings(con.Do("SMEMBERS", "classifiers"))
	if err != nil {
		return
	}
	if len(classifiers) < 2 {
		log.Println("classifiers is less than 2")
		return
	}
	var classifierList []bayesian.Class
	for _, c := range classifiers {
		var name = bayesian.Class(c)
		classifierList = append(classifierList, name)
	}
	m.Lock()
	defer m.Unlock()
	m.classifiers = classifiers
	m.c = bayesian.NewClassifier(classifierList...)
	for _, c := range classifierList {
		words, err := redis.Strings(con.Do("SMEMBERS", "classifier:"+string(c)))
		if err != nil {
			return
		}
		m.c.Learn(words, c)
	}
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
