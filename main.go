package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
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
	var logParserPool *LogParserPool
	logParserPool = &LogParserPool{
		Setting:       c,
		exitChannel:   make(chan int),
		checklist:     make(map[string]string),
		logParserList: make(map[string]*LogParser),
	}
	logParserPool.Run()
	termchan := make(chan os.Signal, 1)
	signal.Notify(termchan, syscall.SIGINT, syscall.SIGTERM)
	<-termchan
	logParserPool.Stop()
}
