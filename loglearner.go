package main

import (
	"flag"
	"github.com/bitly/go-nsq"
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
	trainTopic, _ := c["train_topic"]
	trainChannel, _ := c["train_channel"]
	redisServer, _ := c["redis_server"]

	redisCon := func() (redis.Conn, error) {
		c, err := redis.Dial("tcp", redisServer)
		if err != nil {
			return nil, err
		}
		return c, err
	}
	redisPool := redis.NewPool(redisCon, 3)
	defer redisPool.Close()

	learner := &Learner{
		Pool:         redisPool,
		learnChannel: make(chan *LogMessage),
	}

	r, err := nsq.NewReader(trainTopic, trainChannel)
	if err != nil {
		log.Fatal(err)
	}
	max, _ := strconv.ParseInt(maxinflight, 10, 32)
	r.SetMaxInFlight(int(max))
	for i := 0; i < int(max); i++ {
		r.AddHandler(learner)
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
	learner.Close()
}
