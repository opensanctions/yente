
all:
	make index
	make api

run: build
	docker-compose run --rm app bash

build:
	docker-compose build --pull

services:
	docker-compose up --remove-orphans -d index

api: build services
	docker-compose up --remove-orphans app
