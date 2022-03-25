.PHONY: $(MAKECMDGOALS)

build: webapi cron

mkbin:
	@mkdir -p bin/

webapi: mkbin
	go build -o bin/dealer-webapi ./webapi

cron: mkbin
	go build -o bin/dealer-cron ./cron
