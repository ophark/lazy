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
	s, err := ReadConfig(*confFile)
	if err != nil {
		log.Fatal("config parse error", err)
	}
	logParserPool := &LogParserPool{
		Setting: s,
	}
	logParserPool.Run()
	termchan := make(chan os.Signal, 1)
	signal.Notify(termchan, syscall.SIGINT, syscall.SIGTERM)
	<-termchan
	logParserPool.Stop()
}
