.PHONY: up down test lint generate

up:
	docker-compose up -d

down:
	docker-compose down -v

test:
	pytest tests/ -v

lint:
	ruff check src/ tests/

generate:
	python -m src.producer.event_generator --events-per-second 10 --duration 60
