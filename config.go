package main

import (
	"encoding/json"
	"github.com/goinggo/mapstructure"
	"io/ioutil"
	"os"
)

// Config is metrictools config struct
type Setting struct {
	nsqdAddress       string   `jpath:"nsqd_addr"`
	lookupdAddresses  []string `jpath:"lookupd_addresses"`
	analyzerTopic     string   `jpath:"analyzer_topic"`
	analyzerChannel   string   `jpath:"analyzer_channel"`
	trainTopic        string   `jpath:"train_topic"`
	redisServer       string   `jpath:"redis_server"`
	elasticSearchHost string   `jpath:"elasticsearch_host"`
	elasticSearchPort string   `jpath:"elasticsearch_port"`
	maxInFlight       int      `jpath:"maxinflight"`
	listenAddress     string   `jpath:"listen_address"`
	sessionName       string   `jpath:"session_name"`
	modes             []string `jpath:"modes"`
}

// ReadConfig used to read json to config
func ReadConfig(file string) (*Setting, error) {
	configFile, err := os.Open(file)
	config, err := ioutil.ReadAll(configFile)
	if err != nil {
		return nil, err
	}
	configFile.Close()
	docMap := make(map[string]interface{})
	if err := json.Unmarshal(config, &docMap); err != nil {
		return nil, err
	}
	setting := &Setting{}
	err = mapstructure.DecodePath(docMap, setting)
	return setting, err
}
