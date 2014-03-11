package main

import (
	"fmt"
	"github.com/jeromer/syslogparser/rfc3164"
	"strconv"
	"strings"
)

// LogSetting define log format setting

type LogSetting struct {
	logType            string            `json:"log_type"`
	splitRegexp        string            `json:"split_regexp,omitempty"`
	elasticSearchIndex string            `json:"index"`
	indexTTL           string            `json:"index_ttl"`
	tokens             []string          `json:"tokens,omitempty"`
	tokenFormat        map[string]string `json:"token_format,omitempty"`
	addtionCheck       []string          `json:"addtion_check,omitempty"`
}

func (l *LogSetting) Parser(msg []byte) (map[string]interface{}, error) {
	data := make(map[string]interface{})
	var err error
	if l.logType == "rfc3164" {
		p := rfc3164.NewParser(msg)
		if err = p.Parse(); err != nil {
			return data, err
		}
		data = p.Dump()
	} else {
		data, err = l.wildFormat(generateLogTokens(msg))
	}
	return data, err
}

func (l *LogSetting) wildFormat(msgTokens []string) (map[string]interface{}, error) {
	data := make(map[string]interface{})
	if len(l.tokens) != len(msgTokens) {
		return data, fmt.Errorf("log format error: %s %s", l.tokens, msgTokens)
	}
	for i, token := range l.tokens {
		tk := msgTokens[i]
		if format, ok := l.tokenFormat[token]; ok {
			switch format {
			case "int":
				t, err := strconv.ParseInt(tk, 10, 32)
				if err != nil {
					return data, fmt.Errorf("data format err: %s %s", tk, format)
				}
				data[token] = t
			case "strings":
				k := strings.Split(token, " ")
				v := strings.Split(string(tk), " ")
				if len(k) != len(v) {
					return data, fmt.Errorf("log fromat error: %s %s", k, v)
				}
				for l := 0; l < len(k); l++ {
					data[k[l]] = v[l]
				}
			case "float":
				t, err := strconv.ParseFloat(tk, 64)
				if err != nil {
					return data, fmt.Errorf("data format err: %s %s", tk, format)
				}
				data[token] = t
			default:
				data[token] = tk
			}
		}
	}
	return data, nil
}
