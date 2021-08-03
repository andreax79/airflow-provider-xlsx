SHELL=/bin/bash -e

help:
	@echo "- make clean        Clean"
	@echo "- make tag          Create version tag"
	@echo "- make test         Run tests"
	@echo "- make coverage     Run tests coverage"
	@echo "- make lint         Run lint"

lint:
	python3 setup.py flake8

tag:
	@grep -q "## $$(cat airflow_xlsx/VERSION)" changelog.txt || (echo "Missing changelog !!! Update changelog.txt"; exit 1)
	@git tag -a "v$$(cat airflow_xlsx/VERSION)" -m "version v$$(cat airflow_xlsx/VERSION)"

build: clean
	python3 setup.py bdist_wheel
	python3 setup.py sdist bdist_wheel

clean:
	-rm -rf build dist
	-rm -rf *.egg-info

doc:
	cd docs; $(MAKE) html

test:
	@./scripts/tests.sh

coverage:
	@./scripts/coverage.sh
