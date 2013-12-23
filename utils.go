package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
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
	Names  []string          `json: "names"`
	Values map[string]string `json: "values"`
}

func (m *LogFormat) Parser(msgs []string) (map[string]interface{}, error) {
	message := make(map[string]interface{})
	if len(m.Names) != len(msgs) {
		return message, fmt.Errorf("log format error: %s %s", m.Names, msgs)
	}
	for i := 0; i < len(m.Names); i++ {
		key := m.Names[i]
		if value, ok := m.Values[key]; ok {
			switch value {
			case "int":
				t, err := strconv.ParseInt(msgs[i], 10, 32)
				if err != nil {
					return message, fmt.Errorf("data format err: %s %s", msgs[i], key)
				}
				message[key] = t
			case "strings":
				k := strings.Split(key, " ")
				v := strings.Split(string(msgs[i]), " ")
				if len(k) != len(v) {
					return message, fmt.Errorf("log fromat error: %s %s", k, v)
				}
				for l := 0; l < len(k); l++ {
					message[k[l]] = v[l]
				}
			case "float":
				t, err := strconv.ParseFloat(msgs[i], 64)
				if err != nil {
					return message, fmt.Errorf("data format err: %s %s", msgs[i], key)
				}
				message[key] = t
			default:
				message[key] = string(msgs[i])
			}
		}
	}
	return message, nil
}

// Record is used to pass data to elasticsearch
type Record struct {
	logType    string
	ttl        string
	body       map[string]interface{}
	errChannel chan error
}

func generateLogTokens(buf []byte) []string {
	var tokens []string
	token := make([]byte, 0)
	var lastChar byte
	for _, v := range buf {
		switch v {
		case byte(' '):
			fallthrough
		case byte('['):
			fallthrough
		case byte(']'):
			fallthrough
		case byte('"'):
			if len(token) > 0 {
				if token[len(token)-1] == byte('\\') {
					token = append(token, v)
					continue
				}
				if lastChar == byte('"') {
					if v != byte('"') {
						token = append(token, v)
						continue
					}
				}
				if lastChar == byte('[') {
					if v != byte(']') {
						token = append(token, v)
						continue
					}
				}
				tokens = append(tokens, string(token))
				token = make([]byte, 0)
			} else {
				if lastChar == byte('"') {
					if v == byte('"') {
						tokens = append(tokens, string(token))
						token = make([]byte, 0)
					}
				}
				if lastChar == byte('[') {
					if v == byte(']') {
						tokens = append(tokens, string(token))
						token = make([]byte, 0)
					}
				}
			}
			lastChar = v
		default:
			token = append(token, v)
		}
	}
	return tokens
}
