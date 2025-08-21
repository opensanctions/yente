.PHONY: build all shell stop services api test typecheck check docs

all:
	make api

build:
	docker build -t ghcr.io/opensanctions/yente:latest .

shell: build
	docker compose run --rm app bash

stop:
	docker compose down

services:
	docker compose -f docker-compose.yml up --remove-orphans -d index

api: build services
	docker compose up --remove-orphans app

test:
	pytest --cov-report html --cov-report term --cov=yente -v tests

typecheck:
	mypy --strict yente

check: typecheck test

docs:
	mkdocs build -c -d site
