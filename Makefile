.PHONY: test docs

COLLECT_COVERAGE ?= 1
INCLUDE_BRANCH_COVERAGE ?= 0

test: flake8 pylint test_lib test_examples

flake8:
	flake8 nameko test

pylint:
	pylint --rcfile=pylintrc nameko -E

test_lib:
	@if [ $(COLLECT_COVERAGE) -ne 0 ]; then\
		BRANCH=$(INCLUDE_BRANCH_COVERAGE) py.test test --strict --timeout 30 --cov --cov-config=$(CURDIR)/.coveragerc;\
	else\
		py.test test --strict --timeout 30;\
	fi

test_examples:
	@if [ $(COLLECT_COVERAGE) -ne 0 ]; then\
		BRANCH=$(INCLUDE_BRANCH_COVERAGE) py.test docs/examples/test --strict --timeout 30 --cov=docs/examples --cov-config=$(CURDIR)/.coveragerc;\
	else\
		py.test docs/examples/test --strict --timeout 30;\
	fi
	py.test docs/examples/testing

test_docs: docs spelling #linkcheck

docs:
	sphinx-build -n -b html -d docs/build/doctrees docs docs/build/html

spelling:
	sphinx-build -b spelling -d docs/build/doctrees docs docs/build/spelling

linkcheck:
	sphinx-build -W -b linkcheck -d docs/build/doctrees docs docs/build/linkcheck
