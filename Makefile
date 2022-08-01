
all:
	make api

build:
	docker build -t ghcr.io/opensanctions/yente:latest .

shell: build
	docker-compose run --rm app bash

services:
	docker-compose up --remove-orphans -d index

api: build services
	docker-compose up --remove-orphans app

test:
	pytest -v tests/unit

integration:
	pytest -v tests/integration

typecheck:
	mypy --strict yente

check: typecheck integration test