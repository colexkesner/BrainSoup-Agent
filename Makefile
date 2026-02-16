.PHONY: install run test

install:
	python -m pip install -r requirements.txt

run:
	python -m src.run_pipeline --config config/config.yaml

test:
	pytest -q
