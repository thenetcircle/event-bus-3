ROOT_DIR 	:= $(shell dirname $(realpath $(firstword $(MAKEFILE_LIST))))

format:
	bin/format-code.sh

lint:
	mypy eventbus/ tests/
	isort eventbus/ tests/ --check-only
	flake8 eventbus/ tests/
	black --check eventbus tests

start-http-app:
	uvicorn --reload --reload-dir ./eventbus --app-dir ./eventbus http_app:app