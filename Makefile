init:
	pip install -r requirements.txt -r requirements-dev.txt

test:
	export PYTHONPATH=$(shell pwd)/lib; py.test -m local -s -vvv tests

test-live:
	export PYTHONPATH=$(shell pwd)/lib; py.test -m live -s -vvv tests

.PHONY: init test
