install:
	pip install -U pipenv
	pipenv install --dev
	pipenv install -e .

local-pytest-image:
	docker build -t pcic/pycds-local-pytest -f docker/local-pytest/Dockerfile .

local-pytest-run:
	py3clean .
	docker run --rm -it -v $(shell pwd):/codebase pcic/pycds-local-pytest

test-db-image:
	docker build -t pcic/pycds-test-db -f docker/test-db/Dockerfile .
