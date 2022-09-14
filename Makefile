.PHONY: $(MAKECMDGOALS)

build: webapi cron

mkbin:
	@mkdir -p bin/

gentypes:
	go generate ./webapi/types/types.go

webapi: mkbin gentypes
	go build -o bin/dealer-webapi ./webapi

cron: mkbin gentypes
	go build -o bin/dealer-cron ./cron
