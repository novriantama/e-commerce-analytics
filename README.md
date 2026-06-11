# E-Commerce Analytics Platform (Batch & Real-time Kafka-ClickHouse-dbt Pipeline)

This project implements a production-grade, highly resilient end-to-end data analytics platform utilizing both **batch ingestion** and **near-real-time streaming processing** for the Brazilian E-Commerce dataset. 

---

## 🏗️ Architecture Overview

The platform uses a modern data stack architecture to ingest, store, model, and visualize transaction data:

```mermaid
graph TD
    %% Source files
    CSV_B[Batch CSVs<br/>customers, geolocation, products, sellers, category] -->|Airflow Batch DAG| MINIO_B[(MinIO Data Lake<br/>bronze/)]
    CSV_S[Streaming CSVs<br/>orders, items, payments, reviews] -->|Go Simulator| KAFKA_B[Kafka KRaft Cluster<br/>kafka1, kafka2, kafka3]
    
    %% Kafka to Lake (Orchestrated Streaming Consumer)
    KAFKA_B -->|Airflow Streaming DAG| MINIO_S[(MinIO Data Lake<br/>bronze/)]
    KAFKA_B -->|Kafka DLQ Router| KAFKA_DLQ[DLQ Topics<br/>orders_dlq, etc.]
    
    %% Real-time loading
    KAFKA_B -->|ClickHouse Kafka Engine| CH_Q[(ClickHouse Queue Tables)]
    CH_Q -->|Materialized Views| CH_T[(ClickHouse ReplacingMergeTree)]
    
    %% Batch loading
    MINIO_B -->|ClickHouse S3 Function| CH_T
    
    %% Modeling
    CH_T -->|dbt Models| CH_A[(ClickHouse Analytical Database<br/>fact_orders)]
    
    %% Visualization
    CH_A --> SUPERSET(Superset Dashboards)
```

### Key Components:
1. **Event Simulator (Golang)**: Reads historical transactional CSV datasets, indexes them in memory, sorts orders chronologically, and streams events (orders, items, payments, reviews) to Kafka topics at a rate of 20,000 orders/hour.
2. **Message Broker (Kafka)**: A 3-broker KRaft cluster with replication factor = 3 and partition count = 3 ensuring high availability.
3. **Data Lake (MinIO)**: Serves as the Bronze storage layer archiving raw data partitioned by date in Parquet format.
4. **OLAP Warehouse (ClickHouse)**: Optimized database processing streaming records using internal Kafka Engine tables and Materialized Views.
5. **Orchestrator (Apache Airflow)**: Runs batch ingestion tasks and micro-batched streaming consumers that periodically flush messages to MinIO and isolate invalid payloads into Dead Letter Queue (DLQ) topics.
6. **Data Modeling (dbt)**: Builds a wide, denormalized table (`analytical.fact_orders`) optimized for columnar scanning.
7. **Business Intelligence (Apache Superset)**: Connects to ClickHouse to visualize metrics.

---

## 🛠️ Tech Stack
*   **Language**: Go (for high-throughput simulator), Python (Airflow & dbt)
*   **Orchestration**: Apache Airflow
*   **Streaming**: Apache Kafka (KRaft mode)
*   **Data Lake**: MinIO Object Storage
*   **Data Warehouse**: ClickHouse (OLAP)
*   **Data Modeling**: dbt (Data Build Tool)
*   **Visualization**: Apache Superset
*   **Infrastructure**: Docker, Docker Compose

---

## 🚀 How to Run the Project

### Prerequisites
Make sure you have the following installed:
*   [Docker & Docker Compose](https://docs.docker.com/get-docker/)
*   Python 3.10+ (for local dbt execution)
*   Kaggle CLI configured (requires `~/.kaggle/kaggle.json` key for automatic dataset downloads)

---

### Step-by-Step Execution

#### 1. Setup Environment
Clone the repository, copy the template `.env.example` file to `.env`, and customize it if needed:
```bash
cp .env.example .env
```

#### 2. Download the Dataset
Download and unzip the Kaggle Brazilian E-Commerce dataset using the automated Makefile target:
```bash
make download-data
```

#### 3. Build and Start Docker Services
Build the custom Docker images (Golang Event Simulator, custom Airflow image) and spin up all containers:
```bash
make build
make up
```

#### 4. Run the Batch Ingestion DAG
1. Navigate to the Airflow UI at `http://localhost:8085` (credentials: `admin` / `admin_pass`).
2. Trigger the `batch_ingestion_dag` DAG manually. This will:
   * Read static dimension datasets (customers, products, sellers, geolocation) from CSV.
   * Clean and save them as Parquet files in MinIO.
   * Bulk insert the records into ClickHouse using the native `s3` table function.

#### 5. Verify the Streaming Pipeline
*   The **Golang Event Producer** starts automatically on container launch and begins emitting chronological order events to Kafka.
*   **ClickHouse** ingests this data in real time via Kafka Engine queues and populates tables.
*   The Airflow **`streaming_consumer_dag`** DAG runs every 5 minutes to consume offsets, save Parquet archives to MinIO, and route any parsing failures to the Dead Letter Queue (e.g. `orders_dlq`).

#### 6. Run dbt to Build the Analytical Fact Table
To transform the raw staging schemas into the denormalized ClickHouse warehouse schema:
1. Initialize the local Python virtual environment and install `dbt-clickhouse`:
   ```bash
   make dbt-init
   ```
2. Execute the dbt models:
   ```bash
   make dbt-run
   ```
   *This compiles the SQL staging views and builds the wide table `analytical.fact_orders` using the ClickHouse `MergeTree` engine, partitioned by month and ordered by key dimensions.*

#### 7. Verify the Analytical Output
Connect to ClickHouse using DBeaver (Port `8124` for HTTP, credentials: `clickhouse_admin` / `clickhouse_admin_pass`). Run:
```sql
-- Query the wide fact table
SELECT * FROM analytical.fact_orders LIMIT 5;

-- Check total record count
SELECT count() FROM analytical.fact_orders;
```

#### 8. Connect Apache Superset to ClickHouse
1. Open Superset in your browser at `http://localhost:8089` (credentials: `admin` / `admin_pass`).
2. Go to **Settings** (top right) -> **Database Connections**.
3. Click **+ Database** to add a new connection.
4. Select **Other** from the dropdown menu (do not select ClickHouse directly from the list, as the default ClickHouse driver has compatibility issues with SQLAlchemy 2.0).
5. In the **SQLAlchemy URI** field, enter:
   ```
   clickhousedb://clickhouse_admin:clickhouse_admin_pass@clickhouse:8123/analytical
   ```
6. Click **Test Connection**. It should display a "Connection looks good!" notification.
7. Click **Connect** to save the connection.

---

## 📊 BI Dashboard & Key Insights

After successfully connecting Apache Superset to the ClickHouse analytical database, we built and generated the following **E-Commerce Analytics Dashboard** summarizing key metrics and performance indicators:

![E-Commerce BI Dashboard](dashboard.png)

### 📈 Key Performance Indicators (KPIs)
* **Total Revenue**: **$3.3M** (Aggregate value of all completed sales)
* **Total Orders**: **20.8k** (Volume of transactions processed)
* **Average Order Value (AOV)**: **$136.21** (Average ticket size per order)
* **Average Customer Rating**: **4.06 / 5.00** (Overall review rating index)

### 💡 Core Analytical Insights

1. **Payment Preferences**:
   * **Credit Card** is by far the most dominant payment method, followed by **Boleto** (cash-in/bank transfer ticket).
   * Vouchers and debit cards represent a very small fraction of the transaction volume.

2. **Top Performing Product Categories**:
   * **`bed_bath_table`** represents the leading revenue generator.
   * Other major categories include **`furniture_decor`**, **`housewares`**, and **`watches_gifts`**.

3. **Logistics & Delivery SLAs**:
   * **SP (São Paulo)** generates the highest order concentration (exceeding 8k orders) and also the largest raw number of delayed orders.
   * However, actual delivery duration (typically 10-20 days) is consistently **much lower** than the estimated delivery duration (typically 20-40 days) across all states. This indicates a highly conservative SLA estimation buffer strategy by the logistics department.

---

## 🧹 Cleanup
To stop all services, delete volumes, and remove the raw downloaded files:
```bash
make clean
```

---

## 📁 Repository Directory Structure
*   [`dags/`] - Apache Airflow batch and streaming DAGs.
*   [`dbt_ecommerce/`] - dbt project containing sources, staging views, and the marts model.
*   [`docker/`] - Custom Dockerfiles and SQL schema initializations.
*   [`jobs/producer/`] - Golang Kafka Producer (Event Simulator) source code.
*   [`Makefile`] - Automated command targets.
*   [`docker-compose.yml`] - Multi-container setup.
