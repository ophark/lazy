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
	webLogChannel, _ := c["weblog_channel"]
	redisServer, _ := c["redis_server"]
	elasticSearchServer, _ := c["elasticsearch_host"]
	elasticSearchPort, _ := c["elasticsearch_port"]
	elasticSearchIndex, _ := c["elasticsearch_weblog_index"]

	redisCon := func() (redis.Conn, error) {
		c, err := redis.Dial("tcp", redisServer)
		if err != nil {
			return nil, err
		}
		return c, err
	}
	redisPool := redis.NewPool(redisCon, 3)
	defer redisPool.Close()
	max, _ := strconv.ParseInt(maxinflight, 10, 32)
	tasks := &WebLogParserPool{
		Pool:                redisPool,
		logChannel:          webLogChannel,
		lookupdList:         strings.Split(lookupdAddresses, ","),
		maxInFlight:         int(max),
		elasticSearchServer: elasticSearchServer,
		elasticSearchPort:   elasticSearchPort,
		elasticSearchIndex:  elasticSearchIndex,
		exitChannel:         make(chan int),
		webLogParserList:    make(map[string]*WebLogParser),
	}
	tasks.Run()
	termchan := make(chan os.Signal, 1)
	signal.Notify(termchan, syscall.SIGINT, syscall.SIGTERM)
	<-termchan
	tasks.Stop()
}
