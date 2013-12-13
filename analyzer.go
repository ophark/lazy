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
	logChannel, _ := c["log_channel"]
	logTopic, _ := c["log_topic"]
	trainTopic, _ := c["train_topic"]
	redisServer, _ := c["redis_server"]
	elasticSearchServer, _ := c["elasticsearch_host"]
	elasticSearchPort, _ := c["elasticsearch_port"]
	elasticSearchIndex, _ := c["elasticsearch_index"]

	redisCon := func() (redis.Conn, error) {
		c, err := redis.Dial("tcp", redisServer)
		if err != nil {
			return nil, err
		}
		return c, err
	}
	redisPool := redis.NewPool(redisCon, 3)
	defer redisPool.Close()

	analyzer := &Analyzer{
		Pool:                redisPool,
		Writer:              nsq.NewWriter(nsqdAddr),
		trainTopic:          trainTopic,
		msgChannel:          make(chan Record),
		regexMap:            make(map[string][]*regexp.Regexp),
		elasticSearchServer: elasticSearchServer,
		elasticSearchPort:   elasticSearchPort,
		elasticSearchIndex:  elasticSearchIndex,
	}
	analyzer.getBayes()
	analyzer.getRegexp()
	go analyzer.syncRegexp()
	go analyzer.syncBayes()
	go analyzer.elasticSearchBuildIndex()
	r, err := nsq.NewReader(logTopic, logChannel)
	if err != nil {
		log.Fatal(err)
	}
	max, _ := strconv.ParseInt(maxinflight, 10, 32)
	r.SetMaxInFlight(int(max))
	for i := 0; i < int(max); i++ {
		r.AddHandler(analyzer)
	}
	lookupdlist := strings.Split(lookupdAddresses, ",")
	for _, addr := range lookupdlist {
		log.Printf("lookupd addr %s", addr)
		err := r.ConnectToLookupd(addr)
		if err != nil {
			log.Fatal(err)
		}
	}

	termchan := make(chan os.Signal, 1)
	signal.Notify(termchan, syscall.SIGINT, syscall.SIGTERM)
	<-termchan
	r.Stop()
	analyzer.Close()
	analyzer.Stop()
}
