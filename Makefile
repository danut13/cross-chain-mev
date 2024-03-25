.PHONY: code
code: check format lint sort

.PHONY: check
check:
	mypy src

.PHONY: lint
lint:
	flake8 src

.PHONY: sort
sort:
	isort --force-single-line-imports src

.PHONY: format
format:
	yapf --in-place --recursive src

.PHONY: init
init:
	if [ ! -d data ]; then \
		mkdir data; \
	fi
	if [ ! -d .venv ]; then \
		python3 -m venv .venv; \
	fi
	. .venv/bin/activate
	pip install --upgrade pip
	pip install -r requirements.txt

.PHONY: exec
exec:
	@. .venv/bin/activate
	python3 -m src

.PHONY: view
view:
	@. .venv/bin/activate
	python3 -m src.data_ops --blocks

.PHONY: delete
delete:
	@if [ -z "$(start)" ] || [ -z "$(end)" ]; then \
		echo "Error: Both 'start' and 'end' parameters are required."; \
		exit 1; \
	fi
	@. .venv/bin/activate
	python3 -m src.data_ops --delete $(start) $(end)
