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
	"time"
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
	logTopics, _ := c["log_topics"]
	logChannel, _ := c["log_channel"]
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
	weblogs := make(map[string]*WebLog)
	readers := make(map[string]*nsq.Reader)
	ticker := time.Tick(time.Second * 600)
	checkticker := time.Tick(time.Second * 3600)
	con := redisPool.Get()
	defer con.Close()
	termchan := make(chan os.Signal, 1)
	signal.Notify(termchan, syscall.SIGINT, syscall.SIGTERM)
	for {
		select {
		case <-checkticker:
			topics, err := redis.Strings(con.Do("SMEMBERS", logTopics))
			if err != nil {
				log.Println("fail to get logtopics")
				continue
			}
			check := make(map[string]string)
			for _, topic := range topics {
				check[topic] = topic
			}
			for k := range readers {
				if _, ok := check[k]; ok {
					continue
				} else {
					readers[k].Stop()
					weblogs[k].Close()
					delete(readers, k)
					delete(weblogs, k)
					close(weblogs[k].exitChannel)
				}
			}
		case <-ticker:
			topics, err := redis.Strings(con.Do("SMEMBERS", logTopics))
			if err != nil {
				log.Println("fail to get topics")
			}
			for _, topic := range topics {
				if _, ok := readers[topic]; ok {
					continue
				}
				weblog := &WebLog{
					Pool:                redisPool,
					msgChannel:          make(chan Record),
					elasticSearchServer: elasticSearchServer,
					elasticSearchPort:   elasticSearchPort,
					elasticSearchIndex:  elasticSearchIndex,
					topic:               topic,
					exitChannel:         make(chan int),
				}
				weblog.getLogFormat()
				go weblog.syncLogFormat()
				go weblog.elasticSearchBuildIndex()
				r, err := nsq.NewReader(topic, logChannel)
				if err != nil {
					log.Fatal(err)
				}
				max, _ := strconv.ParseInt(maxinflight, 10, 32)
				r.SetMaxInFlight(int(max))
				for i := 0; i < int(max); i++ {
					r.AddHandler(weblog)
				}
				lookupdlist := strings.Split(lookupdAddresses, ",")
				for _, addr := range lookupdlist {
					log.Printf("lookupd addr %s", addr)
					err := r.ConnectToLookupd(addr)
					if err != nil {
						log.Fatal(err)
					}
				}
				weblogs[topic] = weblog
				readers[topic] = r
			}
		case <-termchan:
			break
		}
	}
	for _, r := range readers {
		r.Stop()
	}
	for _, weblog := range weblogs {
		weblog.Close()
	}
}
