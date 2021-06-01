SHELL := /bin/bash
PWD := $(shell pwd)

default: build

all:

deps:
	go mod tidy

build: deps
	GOOS=linux go build -o bin/node tp2.aba.distros.fi.uba.ar
.PHONY: build

docker-image:
	docker build -f ./Dockerfile -t "tp2node:latest" .
.PHONY: docker-image

docker-compose-up: docker-image
	docker-compose -f docker-compose-dev.yaml up -d --build
.PHONY: docker-compose-up

docker-compose-stop:
	docker-compose -f docker-compose-dev.yaml stop -t 1
.PHONY: docker-compose-stop

docker-compose-down: docker-compose-stop
	docker-compose -f docker-compose-dev.yaml down
.PHONY: docker-compose-down

docker-compose-logs:
	docker-compose -f docker-compose-dev.yaml logs -f
.PHONY: docker-compose-logs
