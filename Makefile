.PHONY: $(MAKECMDGOALS)

build: webapi cron

mkbin:
	@mkdir -p bin/

webapi: mkbin genapitypes
	go build -o bin/dealer-webapi ./webapi

cron: mkbin gentypes
	go build -o bin/dealer-cron ./cron

gentypes: genapitypes genfiltypes

genapitypes:
	go generate ./webapi/types/types.go

genfiltypes:
	go generate ./cron/filtypes/types.go
