VENV := $(shell echo $${VIRTUAL_ENV-.venv})

all: init lint types test

init:
	$(VENV)/bin/pip install tox

test: init
	$(VENV)/bin/tox

lint: init
	$(VENV)/bin/tox -e lint

types: init
	$(VENV)/bin/tox -e types
