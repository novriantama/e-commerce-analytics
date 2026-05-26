.PHONY: download-data build up down logs clean help

# Default target
help:
	@echo "Available commands:"
	@echo "  make download-data - Install Kaggle CLI and download + unzip the Brazilian E-Commerce dataset"
	@echo "  make build         - Build custom docker images (Go Kafka Producer, etc.)"
	@echo "  make up            - Start all services in the background"
	@echo "  make down          - Stop and remove all services"
	@echo "  make logs          - Tail container logs"
	@echo "  make clean         - Stop services, remove volumes, and delete raw downloaded dataset files"

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

# 6. Clean up docker environment and downloaded data
clean:
	@echo "=== Cleaning Up Docker Volumes and Files ==="
	docker compose down -v
	rm -rf data
	@echo "=== Cleanup Complete ==="
