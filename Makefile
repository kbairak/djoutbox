.PHONY: lint test

lint:
	ruff check src/
	ruff format --check src/
	mypy src/djoutbox/ --no-incremental

test:
	pytest --cov --cov-report=term-missing -v

.PHONY: format
format:
	ruff check --fix src/
	ruff format src/
