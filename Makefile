# Copyright 2012, guxianje. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

MAKEFLAGS = -s
all:
	go build loganalyzer.go analyzer.go utils.go
	go build learner.go worker.go utils.go
	go build webui.go bayes.go utils.go logtag.go
	go build weblog.go worker.go utils.go

analyzer:
	go build loganalyzer.go analyzer.go utils.go

web:
	go build webui.go bayes.go utils.go logtag.go

learner:
	go build learner.go worker.go utils.go 

weblog:
	go build weblog.go weblogparser.go weblogparserpool.go utils.go

fmt:
	go fmt

lint:
	golint *.go

clean:
	go clean
