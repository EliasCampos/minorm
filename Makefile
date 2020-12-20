VENV := $(shell echo $${VIRTUAL_ENV-.venv})

all: init test

init:
	$(VENV)/bin/pip install tox

test: init
	$(VENV)/bin/tox