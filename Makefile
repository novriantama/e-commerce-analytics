ifneq (,$(wildcard .env))
    include .env
    export
endif

.PHONY: download-data build up down logs clean help dbt-init dbt-run dbt-clean

# Default target
help:
	@echo "Available commands:"
	@echo "  make download-data - Install Kaggle CLI and download + unzip the Brazilian E-Commerce dataset"
	@echo "  make build         - Build custom docker images (Go Kafka Producer, etc.)"
	@echo "  make up            - Start all services in the background"
	@echo "  make down          - Stop and remove all services"
	@echo "  make logs          - Tail container logs"
	@echo "  make clean         - Stop services, remove volumes, delete raw downloaded dataset files and dbt env"
	@echo "  make dbt-init      - Create local virtual environment and install dbt-clickhouse"
	@echo "  make dbt-run       - Execute dbt models (compile and build fact_orders in ClickHouse)"
	@echo "  make dbt-clean     - Remove dbt target, log and package directories"

# 1. Download and extract Kaggle dataset
download-data:
	@echo "=== Installing Kaggle CLI ==="
	pip install --user kaggle || pip install kaggle || pip3 install kaggle
	@echo "=== Downloading Dataset ==="
	mkdir -p data
	kaggle datasets download -d olistbr/brazilian-ecommerce -p data
	@echo "=== Unzipping Dataset ==="
	unzip -o data/brazilian-ecommerce.zip -d data
	@echo "=== Dataset Ready ==="
	ls -lh data

# 2. Build Docker images
build:
	@echo "=== Building Docker Images ==="
	docker compose build

# 3. Start all docker services
up:
	@echo "=== Starting Services ==="
	docker compose up -d
	@echo "=== Services Running ==="
	docker compose ps

# 4. Stop all docker services
down:
	@echo "=== Stopping Services ==="
	docker compose down

# 5. Tail container logs
logs:
	docker compose logs -f

# 6. Clean up docker environment, downloaded data, and dbt env
clean: dbt-clean
	@echo "=== Cleaning Up Docker Volumes and Files ==="
	docker compose down -v
	rm -rf data .venv-dbt
	@echo "=== Cleanup Complete ==="

# 7. Initialize dbt virtual environment and dependencies
dbt-init:
	@echo "=== Setting up dbt virtual environment ==="
	python3 -m venv .venv-dbt
	.venv-dbt/bin/pip install --upgrade pip
	.venv-dbt/bin/pip install dbt-clickhouse
	@echo "=== dbt Ready ==="

# 8. Run dbt models
dbt-run:
	@echo "=== Running dbt Models ==="
	cd dbt_ecommerce && ../.venv-dbt/bin/dbt run --profiles-dir .

# 9. Clean dbt target output
dbt-clean:
	@echo "=== Cleaning dbt targets ==="
	rm -rf dbt_ecommerce/target dbt_ecommerce/dbt_packages dbt_ecommerce/logs
