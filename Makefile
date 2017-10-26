init:
	pip install -r requirements.txt -r requirements-dev.txt

test:
	export PYTHONPATH=$(shell pwd)/lib; py.test -m local -rs -vvv tests

test-live:
	export PYTHONPATH=$(shell pwd)/lib; py.test -m live -rs -vvv tests

.PHONY: init test
