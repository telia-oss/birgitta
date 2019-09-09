COV_MIN = 100 # Gradually increase this as we add more tests
TAG = latest

SRC_DIR = $(shell pwd)
DIST_DIR = $(SRC_DIR)/dist
BIRGITTA_TESTS = $(SRC_DIR)/ngadss

package: clean
	python setup.py sdist bdist_wheel

clean:
	cd $(SRC_DIR)
	find . -name '__pycache__' -exec rm -rf {} +
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +
	rm -rf dist/
	rm -rf *.egg-info
	rm -rf htmlcov/
	rm -rf spark-warehouse/
	rm -rf .pytest_cache/
	rm -rf .coverage*
	rm -rf tmp/*

configure:
	pip install -r requirements.txt
	pip install -r requirements_dev.txt

lint:
	flake8

jupyter:
	jupyter notebook scratchpad/notebooks/default.ipynb

test:
	# Fork safety disabled to avoid fork() crash
	# objc[67570]: +[__NSPlaceholderDate initialize] may have
	# been in progress in another thread when fork() was called.
	OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES \
	pytest \
		--cov=birgitta \
		--cov-report html \
		--cov-report term-missing \
		--cov-fail-under $(COV_MIN) \
		$(BIRGITTA_TESTS)

.PHONY: build clean configure lint test
