.PHONY: $(MAKECMDGOALS)

build: webapi cron

mkbin:
	@mkdir -p bin/

webapi: mkbin
	go build -o bin/spade-webapi ./webapi

cron: mkbin gentypes
	go build -o bin/spade-cron ./cron

gentypes: genfiltypes

genfiltypes:
	go generate ./internal/filtypes/types.go
