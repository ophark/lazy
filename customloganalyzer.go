package main

import (
	"flag"
	"github.com/garyburd/redigo/redis"
	"log"
	"os"
	"os/signal"
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
	maxinflight, _ := c["maxinflight"]
	logChannel, _ := c["log_channel"]
	redisServer, _ := c["redis_server"]
	elasticSearchServer, _ := c["elasticsearch_host"]
	elasticSearchPort, _ := c["elasticsearch_port"]
	elasticSearchIndex, _ := c["elasticsearch_log_index"]
	elasticSearchIndexTTL, _ := c["elasticsearch_log_index_ttl"]

	redisCon := func() (redis.Conn, error) {
		c, err := redis.Dial("tcp", redisServer)
		if err != nil {
			return nil, err
		}
		return c, err
	}
	max, _ := strconv.ParseInt(maxinflight, 10, 32)
	tasks := &LogParserPool{
		Pool:                  redis.NewPool(redisCon, 3),
		logChannel:            logChannel,
		lookupdList:           strings.Split(lookupdAddresses, ","),
		maxInFlight:           int(max),
		elasticSearchServer:   elasticSearchServer,
		elasticSearchPort:     elasticSearchPort,
		elasticSearchIndex:    elasticSearchIndex,
		elasticSearchIndexTTL: elasticSearchIndexTTL,
		exitChannel:           make(chan int),
		logParserList:         make(map[string]*LogParser),
	}
	tasks.Run()
	termchan := make(chan os.Signal, 1)
	signal.Notify(termchan, syscall.SIGINT, syscall.SIGTERM)
	<-termchan
	tasks.Stop()
}