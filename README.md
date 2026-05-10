# FinTech Fraud Detection Pipeline

A real-time fraud detection pipeline built on a **Lambda architecture**, combining Apache Kafka, Spark Structured Streaming, Apache Airflow, PostgreSQL, and Grafana - fully containerised with Docker.

---

## Architecture

```
Transaction Producer
        │
        ▼
   Apache Kafka
        │
        ▼
Spark Structured Streaming ──► PostgreSQL ──► Grafana Dashboard
                                    │
                                    ▼
                            Apache Airflow (every 6h)
                                    │
                          ┌─────────┴──────────┐
                          ▼                    ▼
                   Parquet Files         Reconciliation
                (Data Warehouse)            Report (CSV)
```

---

## Tech Stack

| Component | Technology |
|---|---|
| Message Broker | Apache Kafka 3.7.0 (KRaft) |
| Stream Processing | Apache Spark 4.0.1 |
| Orchestration | Apache Airflow 2.8.1 |
| Database | PostgreSQL 14 |
| Monitoring | Grafana |
| Containerisation | Docker Compose |

---

## Fraud Detection Rules

Two rules are applied per transaction using stateful processing via `applyInPandasWithState`:

- **High Value** - transaction amount exceeds $5,000
- **Impossible Location** - same user transacts in a different location within 10 minutes of their previous transaction

Each transaction is flagged as either `NORMAL` or `FRAUD`.

---

## Prerequisites

- Docker Desktop (6GB+ memory allocated)
- At least 10GB free disk space

---

## Getting Started

**1. Build Docker images**
```bash
docker compose build
```

**2. Start all services**
```bash
docker compose up -d
```

**3. Initialise Airflow**
```bash
docker exec -it airflow-webserver airflow db migrate

docker exec -it airflow-webserver airflow users create \
  --username admin \
  --password admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com
```

## Service URLs

| Service | URL |
|---|---|
| Spark Master UI | http://localhost:8080 |
| Spark Worker UI | http://localhost:8081 |
| Airflow UI | http://localhost:8082 |
| Kafka UI | http://localhost:8085 |
| Grafana | http://localhost:3000 |

---

## Airflow DAGs

| DAG | Schedule | Purpose |
|---|---|---|
| `spark_streaming_pipeline` | Manual trigger | Submits and monitors the Spark streaming job |
| `etl_pipeline` | Every 6 hours | Exports validated transactions to Parquet and generates reconciliation report |

---

## ETL Outputs

Each 6-hour Airflow run produces two files:

- `dags/data_warehouse/validated_transactions_<timestamp>.parquet` — validated (non-fraud) transactions
- `dags/reports/reconciliation_report_<timestamp>.csv` — total ingress vs validated count
- `dags/reports/fraud_by_category_<timestamp>.csv` — fraud attempts broken down by merchant category

---

## Monitoring

Grafana dashboard available at `http://localhost:3000` (default credentials: `admin` / `admin`).

Panels include:
- Transaction throughput over time
- Total transactions processed
- Fraud attempts by merchant category
- Transaction validation breakdown (NORMAL vs FRAUD)
- Total ingress vs validated amount
