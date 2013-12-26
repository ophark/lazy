package main

import (
	"flag"
	"github.com/bitly/go-nsq"
	"github.com/garyburd/redigo/redis"
	"log"
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"strings"
	"syscall"
)

var (
	confFile = flag.String("conf", "lazy.json", "lazy config file")
)

func main() {
	flag.Parse()
	c, err := ReadConfig(*confFile)
	if err != nil {
		log.Fatal("config parse error", err)
	}

	lookupdAddresses, _ := c["lookupd_addresses"]
	nsqdAddr, _ := c["nsqd_addr"]
	maxinflight, _ := c["maxinflight"]
	analyzerChannel, _ := c["analyzer_channel"]
	analyzerTopic, _ := c["analyzer_topic"]
	trainTopic, _ := c["train_topic"]
	redisServer, _ := c["redis_server"]
	elasticSearchServer, _ := c["elasticsearch_host"]
	elasticSearchPort, _ := c["elasticsearch_port"]
	elasticSearchIndex, _ := c["elasticsearch_syslog_index"]
	elasticSearchIndexTTL, _ := c["elasticsearch_syslog_index_ttl"]

	redisCon := func() (redis.Conn, error) {
		c, err := redis.Dial("tcp", redisServer)
		if err != nil {
			return nil, err
		}
		return c, err
	}
	max, _ := strconv.ParseInt(maxinflight, 10, 32)
	analyzer := &Analyzer{
		Pool:                  redis.NewPool(redisCon, 3),
		writer:                nsq.NewWriter(nsqdAddr),
		maxInFlight:           int(max),
		lookupdList:           strings.Split(lookupdAddresses, ","),
		analyzerTopic:         analyzerTopic,
		analyzerChannel:       analyzerChannel,
		trainTopic:            trainTopic,
		msgChannel:            make(chan Record),
		exitChannel:           make(chan int),
		regexMap:              make(map[string][]*regexp.Regexp),
		elasticSearchServer:   elasticSearchServer,
		elasticSearchPort:     elasticSearchPort,
		elasticSearchIndex:    elasticSearchIndex,
		elasticSearchIndexTTL: elasticSearchIndexTTL,
	}
	if err := analyzer.Run(); err != nil {
		log.Fatal(err)
	}
	termchan := make(chan os.Signal, 1)
	signal.Notify(termchan, syscall.SIGINT, syscall.SIGTERM)
	<-termchan
	analyzer.Stop()
}
