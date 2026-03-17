# 🏗️ Multi-Source Lakehouse Pipeline — Project Guidelines

## 📌 Overview

This project aims to build an **end-to-end Data Engineering platform** using a **Lakehouse architecture** with **Delta Lake and Apache Spark**, following the **Medallion pattern (Bronze, Silver, Gold)**.

The platform ingests data from multiple sources:

- Public APIs
- Relational database (PostgreSQL)
- CSV files simulating SaaS exports (e.g., Salesforce)
- Simulated streaming data

All data is standardized and stored in a **single Data Lake**, with strong emphasis on:

- Data Quality
- Reproducibility
- Scalability
- Observability
- Clean architecture

---

# 🎯 Objectives

- Demonstrate real-world Data Engineering skills
- Handle multi-source ingestion
- Apply Medallion architecture
- Use Delta Lake as the unified storage format
- Implement a custom Data Quality framework
- Build a production-like, extensible codebase

---

# 🧱 Architecture Decisions

## 1. Lakehouse Architecture
- Pattern: Medallion (Landing → Bronze → Silver → Gold)
- Storage: Local Data Lake (Delta format)
- Future-ready for Azure / Databricks

## 2. Processing Engine
- Apache Spark (PySpark)
- Delta Lake for all processed layers

## 3. Source Systems
- PostgreSQL (transactional simulation)
- Public APIs (external integrations)
- CSV files (SaaS exports simulation)
- Streaming (file-based simulation)

## 4. Orchestration
- Apache Airflow

## 5. Data Quality
- Custom framework implemented in PySpark
- Results persisted in Delta tables

---

# 🗂️ Project Structure
data-lakehouse/
│
├── configs/
├── docs/
├── infra/
├── storage/
│ ├── landing/
│ ├── bronze/
│ ├── silver/
│ ├── gold/
│ └── checkpoints/
│
├── src/lakehouse/
│ ├── common/
│ ├── config/
│ ├── spark/
│ ├── ingestion/
│ ├── bronze/
│ ├── silver/
│ ├── gold/
│ ├── quality/
│ ├── metadata/
│ └── orchestration/
│
├── dags/
├── scripts/
├── tests/
└── notebooks/



---

# ⚖️ Core Mandates (Codebase Rules)

## M1 — No business logic in notebooks
- All logic must be in Python modules
- Notebooks are for exploration only

## M2 — Pipelines must be reproducible
- No hidden state
- Deterministic outputs
- Explicit parameters

## M3 — All datasets must include metadata
Each dataset must include (when applicable):

- `source_system`
- `ingestion_timestamp`
- `processing_timestamp`
- `batch_id`
- `load_date`
- `record_hash`
- `pipeline_run_id`

## M4 — Data Quality is mandatory
- No Silver/Gold dataset without validation
- Fail fast or mark dataset accordingly

## M5 — Configuration must be externalized
- No hardcoded paths or secrets
- Use `.env`, YAML or config modules

## M6 — Single Responsibility Principle
- Each function must do one thing well
- Avoid monolithic scripts

## M7 — Predictable naming
Paths must follow:
/{layer}/{source}/{entity}/



## M8 — Logging is required
Each job must log:

- start/end
- input/output volumes
- errors
- execution time

## M9 — Minimum test coverage required
- Unit tests for transformations
- Tests for data quality rules
- Integration tests for pipelines

## M10 — Databricks-ready design
- Avoid local-only assumptions
- Abstract storage paths
- Keep Spark-compatible structure

---

# 🧩 Medallion Layer Rules

## Landing Layer
- Raw, unmodified data
- Store original format (JSON, CSV, etc.)
- Versioned by ingestion time

## Bronze Layer
- Minimal transformation
- Schema standardization
- Add metadata columns
- No business logic

## Silver Layer
- Clean and validated data
- Deduplication
- Type casting
- Data quality enforcement
- Domain normalization

## Gold Layer
- Business-ready datasets
- Aggregations and KPIs
- Fact and dimension modeling

---

# 📏 Data Quality Framework

## Categories

### 1. Structural
- Schema validation
- Required columns
- Data types

### 2. Content
- Not null
- Unique keys
- Valid ranges
- Accepted values

### 3. Relational
- Foreign key integrity
- Cross-table validation

### 4. Temporal / Volume
- Data freshness
- Volume anomalies
- Drift detection

---

## Quality Results Table

All validations must be persisted in:
silver.monitoring_quality_results



### Schema

- `pipeline_run_id`
- `dataset_name`
- `layer`
- `rule_name`
- `rule_type`
- `status`
- `failed_records`
- `total_records`
- `failure_rate`
- `execution_timestamp`

---

# 📛 Naming Conventions

## Files
snake_case.py



Examples:
- `db_to_bronze.py`
- `silver_customers.py`

## Functions
verb_object()



Examples:
- `read_csv()`
- `validate_not_null()`
- `write_delta_table()`

## Tables
bronze.crm_accounts
silver.sales_orders
gold.customer_360



---

# 🔄 Data Flow
Sources
↓
Landing (raw)
↓
Bronze (standardized)
↓
Silver (clean + validated)
↓
Gold (analytics)



---

# 🚀 Implementation Phases

## Phase 0 — Project Setup
- Repository structure
- Config files
- Docker setup
- Documentation

## Phase 1 — Environment
- Spark + Delta setup
- PostgreSQL setup
- Logging + config modules

## Phase 2 — Data Sources
- Generate fake PostgreSQL data
- Generate CSV exports
- Integrate API
- Simulate streaming

## Phase 3 — Landing
- Store raw data from all sources

## Phase 4 — Bronze
- Convert all sources to Delta
- Add metadata

## Phase 5 — Data Quality Framework
- Implement validation engine
- Persist results

## Phase 6 — Silver
- Clean and validate datasets
- Deduplicate and standardize

## Phase 7 — Gold
- Build business datasets
- KPIs and aggregations

## Phase 8 — Orchestration
- Airflow DAGs
- Dependency management

## Phase 9 — Testing
- Unit + integration tests

## Phase 10 — Documentation
- Architecture
- Decisions (ADR)
- Execution guide

---

# 🧠 Domain Recommendation

Recommended domain:

**Sales / CRM / Customer 360**

### Entities
- customers
- orders
- products
- accounts
- opportunities
- leads
- events

---

# 📦 First Milestone

Minimum deliverable:

- PostgreSQL with fake data
- CSV exports
- 1 API integrated
- Data in Landing
- Conversion to Bronze (Delta)
- Metadata columns applied

---

# 📚 Engineering Principles

- Favor clarity over cleverness
- Prefer explicit over implicit
- Design for reprocessing
- Build for observability
- Keep layers decoupled
- Treat data as a product

---

# 🔮 Future Enhancements

- dbt for transformation layer
- Kafka for real streaming
- MinIO for object storage
- Power BI / dashboards
- Migration to Azure / Databricks
- Data catalog and lineage tracking

---

# 🧾 Final Notes

This project is designed to:

- Simulate real-world data platform challenges
- Showcase engineering maturity
- Be extensible and production-ready

Every design choice should be explainable in an interview.

---