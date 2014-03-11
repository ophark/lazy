# Copyright 2012, guxianje. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

MAKEFLAGS = -s
all:
	go build

webui:
	go build webui.go classifier.go utils.go logtag.go logformat.go

learner:
	go build bayeslearner.go learner.go utils.go

fmt:
	go fmt

lint:
	golint *.go

clean:
	go clean
