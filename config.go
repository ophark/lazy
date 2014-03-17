package main

import (
	"encoding/json"
	"github.com/goinggo/mapstructure"
	"io/ioutil"
	"os"
)

// Config is metrictools config struct
type Setting struct {
	NsqdAddress       string   `jpath:"nsqd_addr"`
	LookupdAddresses  []string `jpath:"lookupd_addresses"`
	TrainTopic        string   `jpath:"train_topic"`
	RedisServer       string   `jpath:"redis_server"`
	ElasticSearchHost string   `jpath:"elasticsearch_host"`
	ElasticSearchPort string   `jpath:"elasticsearch_port"`
	MaxInFlight       int      `jpath:"maxinflight"`
	ListenAddress     string   `jpath:"listen_address"`
	SessionName       string   `jpath:"session_name"`
	Modes             []string `jpath:"modes"`
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
