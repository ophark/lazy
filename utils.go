package main

import (
	"encoding/json"
	"io/ioutil"
	"os"
)

// Config is metrictools config struct
type Config map[string]string

// ReadConfig used to read json to config
func ReadConfig(file string) (Config, error) {
	var setting Config
	configFile, err := os.Open(file)
	config, err := ioutil.ReadAll(configFile)
	if err != nil {
		return nil, err
	}
	configFile.Close()
	if err := json.Unmarshal(config, &setting); err != nil {
		return nil, err
	}
	return setting, nil
}

type LogFormat struct {
	Names []string `json: "names"`
	Values map[string]string `json: "values"`
}

// Record is used to pass data to elasticsearch
type Record struct {
	logType    string
	body       map[string]interface{}
	errChannel chan error
}
