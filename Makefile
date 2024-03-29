ROOT_DIR 	:= $(shell dirname $(realpath $(firstword $(MAKEFILE_LIST))))

format:
	bin/format-code.sh

lint:
	mypy eventbus/ tests/
	isort eventbus/ tests/ --check-only
	flake8 eventbus/ tests/
	black --check eventbus tests

build:
	docker build -t eventbus3 .

test:
	EVENTBUS_CONFIG=configs/test.yml pytest

start-producer:
	uvicorn --reload --reload-dir ./eventbus --app-dir ./eventbus app_producer:app
