.PHONY: build all shell stop services api test typecheck check docs lock

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

# Regenerate the PEP 751 lockfile (pylock.toml) inside the Dockerfile's `lock`
# stage so the resolved wheels match the python_image we ship. pip lock is
# experimental in pip 26.x.
lock:
	docker build --platform=linux/amd64 --target=lock -t yente-lock .
	docker run --rm --platform=linux/amd64 -v "$(CURDIR):/work" -w /work yente-lock \
		pip lock . -o pylock.toml --quiet
