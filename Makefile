.PHONY: build dist clean

OUTPUT_FILE ?= kafka-util

build:
	go build -o $(OUTPUT_FILE) cmd/main.go

dist:
	GOOS=darwin GOARCH=amd64 go build -o dist/$(OUTPUT_FILE)-darwin-amd64 cmd/main.go
	GOOS=darwin GOARCH=arm64 go build -o dist/$(OUTPUT_FILE)-darwin-arm64 cmd/main.go
	GOOS=linux GOARCH=amd64 go build -o dist/$(OUTPUT_FILE)-linux-amd64 cmd/main.go
	GOOS=linux GOARCH=arm64 go build -o dist/$(OUTPUT_FILE)-linux-arm64 cmd/main.go
	GOOS=windows GOARCH=amd64 go build -o dist/$(OUTPUT_FILE)-windows-amd64 cmd/main.go
	GOOS=windows GOARCH=arm64 go build -o dist/$(OUTPUT_FILE)-windows-arm64 cmd/main.go

clean:
	rm -f $(OUTPUT_FILE)
	rm -rf dist/
