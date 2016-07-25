package main

import (
	"encoding/json"
	"io/ioutil"
	"os"
)

// Config is metrictools config struct
type Setting struct {
	NsqdAddress       string   `json:"nsqd_addr"`
	LookupdAddresses  []string `json:"lookupd_addresses"`
	TrainTopic        string   `json:"train_topic"`
	ElasticSearchHost string   `json:"elasticsearch_host"`
	ElasticSearchPort string   `json:"elasticsearch_port"`
	MaxInFlight       int      `json:"maxinflight"`
	ConsulAddress     string   `json:"consul_address"`
	Datacenter        string   `json:"datacenter"`
	Token             string   `json:"consul_token"`
	ConsulKey         string   `json:"consul_key"`
}

// ReadConfig used to read json to config
func ReadConfig(file string) (*Setting, error) {
	configFile, err := os.Open(file)
	config, err := ioutil.ReadAll(configFile)
	if err != nil {
		return nil, err
	}
	configFile.Close()
	setting := &Setting{}
	if err := json.Unmarshal(config, setting); err != nil {
		return nil, err
	}
	return setting, err
}
