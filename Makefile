.PHONY: build

OUTPUT_FILE ?= kafka-util

build:
	go build -o $(OUTPUT_FILE) cmd/main.go
