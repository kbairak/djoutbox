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

migrate:
	uv run examples/manage.py runserver

runserver:
	uv run examples/manage.py runserver

relay:
	uv run examples/relay.py

worker:
	uv run examples/worker.py

docs-serve:
	uv run mkdocs serve

docs-build:
	uv run mkdocs build --strict
