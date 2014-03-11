package main

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
