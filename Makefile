## Lint your code using pylint
.PHONY: yamllint
yamllint:
	python -m yamllint --version
	python -m yamllint .
## Run tests using pytest
.PHONY: test
test:
	python -m pytest --version
	python -m pytest tests
## Format your code using black
.PHONY: format
format:
	python -m black --version
	python -m black .
## Check linting with flake8
.PHONY: lint
lint:
	python -m flake8 --version
	python -m flake8 .
.PHONY: check
check:
	pre-commit run --all-files
## Run ci part
.PHONY: ci
ci: check lint test
