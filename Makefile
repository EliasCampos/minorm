VENV := $(shell echo $${VIRTUAL_ENV-.venv})

all: init lint test

init:
	$(VENV)/bin/pip install tox

test: init
	$(VENV)/bin/tox

lint: init
	$(VENV)/bin/tox -e lint