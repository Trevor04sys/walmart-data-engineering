# Walmart Data Engineering Project

An end-to-end data engineering project built using **Apache Airflow,
dbt, Azure Databricks, and TigerData Cloud**. This project demonstrates
data ingestion, incremental transformations, dimensional modeling, data
quality testing, workflow orchestration, and Spark query optimization.

## Project Overview

This project implements an end-to-end data pipeline using Walmart sales
data. It follows the **Medallion Architecture** to transform raw source
data into structured, analytics-ready datasets.

The source data consists of six CSV files containing customers,
employees, orders, order items, products, and stores. A Python script
loads these datasets into TigerData Cloud, a PostgreSQL database.

Apache Airflow orchestrates the pipeline, while Azure Databricks handles
data ingestion and processing. dbt manages SQL transformations,
snapshots, dimensional modeling, and data quality tests.

The project includes incremental processing, Change Data Capture (CDC),
Slowly Changing Dimensions (SCD Type 2), and Delta Lake experiments.

## Key Features

-   End-to-end data pipeline using Apache Airflow, dbt, and Azure
    Databricks.
-   PostgreSQL source database hosted on TigerData Cloud.
-   Python-based CSV ingestion using `psycopg2`.
-   Databricks ingestion and CDC processing.
-   Medallion Architecture with Bronze, Silver, and Gold layers.
-   Incremental data transformations using dbt.
-   Business-oriented data modeling using `obt_b`.
-   SCD Type 2 dimensional modeling using dbt snapshots.
-   Fact table modeling with historical dimension lookups.
-   Automated dbt data quality testing.
-   Delta Lake time travel and Change Data Feed experiments.
-   Spark query plan analysis and performance optimization.
-   Airflow task failure and DAG success email notifications.
-   Docker-based deployment.
-   Version control using Git and GitHub.

## Architecture

``` text
                 Walmart CSV Dataset
                         |
                         v
                 Python Data Loader
                    (psycopg2)
                         |
                         v
                 TigerData Cloud
                   (PostgreSQL)
                         |
                         v
                  Apache Airflow
                  (Orchestration)
                         |
                         v
              Databricks Ingestion
                  (CDC Processing)
                         |
                         v
                  Bronze Layer
                         |
                         v
               Silver Technical
              (Incremental Models)
                         |
                         v
               Silver Business
                     (obt_b)
                         |
                         v
                   Gold Layer
                    /      \\
                   v        v
              Dimensions  fact_orders
                (SCD2)        |
                   \\         /
                    v       v
                  Data Quality
                     Tests
```

Apache Airflow manages task dependencies and execution order. Databricks
performs ingestion and data processing, while dbt manages
transformations, snapshots, and testing.

## Technology Stack

  Technology         Purpose
  ------------------ ---------------------------------------------
  Python             CSV ingestion and data loading
  psycopg2           PostgreSQL database connectivity
  TigerData Cloud    Managed PostgreSQL source database
  Apache Airflow 3   Workflow orchestration
  Docker Compose     Containerized deployment
  Azure Databricks   Data ingestion and processing
  Databricks SDK     Databricks integration
  Delta Lake         Data storage, time travel, and CDC
  dbt                SQL transformations, snapshots, and testing
  PySpark            Distributed data processing
  Git                Version control
  GitHub             Source code hosting

## Data Source and Ingestion

The project uses six Walmart CSV datasets:

-   Customers
-   Employees
-   Orders
-   Order Items
-   Products
-   Stores

A Python script, `load_walmart_data.py`, uses `psycopg2` to load the CSV
files into the `raw` schema of a PostgreSQL database hosted on TigerData
Cloud.

The source database was initially hosted on Ghost. The data was
subsequently migrated to TigerData, and the restored tables were
validated against the original row counts.

The migration included all six tables, with a total of **42,796 rows**.

The Python loader is intended for initial data loading. Subsequent
pipeline ingestion and processing are handled by Databricks.

## Data Pipeline

The pipeline follows a sequence of ingestion, transformation, modeling,
and validation tasks.

1.  **Source data:** Walmart CSV files provide the initial datasets.
2.  **Data loading:** Python loads the CSV files into the TigerData
    PostgreSQL database.
3.  **Data ingestion:** Databricks ingests source data and processes CDC
    changes.
4.  **Source freshness:** Checks the freshness of source data.
5.  **Silver technical layer:** Applies incremental transformations to
    the source tables.
6.  **Data quality:** Runs dbt tests to validate transformed data.
7.  **Silver business layer:** Builds `obt_b`, a business-oriented table
    at the order-item grain.
8.  **Ephemeral models:** Provides intermediate transformations for
    downstream models.
9.  **Snapshots:** Captures historical changes using dbt snapshots.
10. **Gold dimensions:** Builds dimensional models using SCD Type 2.
11. **Gold fact:** Builds `fact_orders` with historical dimension
    lookups.
12. **Final validation:** Runs data quality tests on the resulting
    models.

Airflow orchestrates the pipeline and manages task dependencies and
execution order.

## Data Modeling

The project follows the Medallion Architecture, separating data
processing into Bronze, Silver, and Gold layers.

### Bronze Layer

The Bronze layer stores ingested source data in Databricks. It serves as
the initial layer for downstream transformations.

### Silver Layer

The Silver layer contains technical and business-oriented models.

#### Silver Technical

Contains incremental dbt models for the source entities:

-   Customers
-   Employees
-   Orders
-   Order Items
-   Products
-   Stores

These models apply incremental processing to prepare the source data for
downstream transformations.

#### Silver Business

The Silver Business layer contains the `obt_b` model.

`obt_b` combines relevant source entities into a business-oriented
table. Its intended grain is **one row per order item**. It provides a
consolidated dataset for downstream dimensional and fact modeling.

### Gold Layer

The Gold layer contains dimensional models and a fact table designed for
analytical use.

#### Dimensions

-   `dim_customers`
-   `dim_employees`
-   `dim_orders`
-   `dim_products`
-   `dim_stores`

The dimensions use dbt snapshots to maintain historical records using
SCD Type 2. Historical versions are maintained with validity periods,
allowing fact records to reference the appropriate dimension version.

#### Fact Table

`fact_orders` stores order-item-level records and uses temporal joins to
retrieve the appropriate historical dimension records.

The model connects fact records to their corresponding dimensions using
the relevant validity periods.

## Data Quality

Data quality is implemented using dbt tests to validate the correctness
and consistency of the transformed data.

The project includes tests for:

-   Fact table grain and uniqueness.
-   Dimension validity and overlapping historical records.
-   Exactly one current customer dimension record.
-   Temporal consistency between facts and dimensions.
-   Order total reconciliation.
-   Business table grain.

These tests help ensure that the dimensional models and fact table
maintain the expected structure and relationships.

## Delta Lake Experiments

The project includes practical experiments with Delta Lake to explore
its data management capabilities.

The experiments cover:

-   Time travel to query previous table versions.
-   Change Data Feed (CDF) to inspect data changes.
-   Schema evolution using automatic schema merging.
-   Delta table history analysis.
-   File layout and optimization experiments.

CDF was enabled on the customers technical table for the experiment.

## Spark Optimization

Spark optimization experiments were performed using Databricks query
plans and query profiles.

The experiments covered:

-   Column pruning
-   Predicate pushdown
-   Join strategies
-   Broadcast joins
-   Sort-merge joins
-   Aggregation plans
-   Delta table details
-   Query profile comparisons

The experiments and observations are documented in
[`walmart_project/spark-optimization.md`](walmart_project/spark-optimization.md).

## Airflow Monitoring

Apache Airflow is used to orchestrate and monitor pipeline execution.

The project includes:

-   Task retries
-   Task failure email notifications
-   DAG success email notifications
-   Task execution status and logs

Email notifications are configured through Airflow connections.

The `orchestrate` DAG coordinates the pipeline's ingestion and
transformation tasks.

## Project Structure

``` text
walmart-data-engineering/
│
├── config/
│   └── airflow.cfg
│
├── dags/
│   └── orchestrate.py
│
├── walmart_dataset/
│   ├── data/
│   │   ├── customers.csv
│   │   ├── employees.csv
│   │   ├── orders.csv
│   │   ├── order_items.csv
│   │   ├── products.csv
│   │   └── stores.csv
│   │
│   └── ddl/
│       └── walmart_schema.sql
│
├── walmart_project/
│   ├── models/
│   │   ├── source/
│   │   ├── silver_technical/
│   │   ├── silver_business/
│   │   └── gold/
│   │       ├── ephemeral/
│   │       └── fact/
│   │
│   ├── snapshots/
│   ├── tests/
│   ├── macros/
│   ├── analyses/
│   ├── seeds/
│   │
│   ├── dbt_project.yml
│   ├── packages.yml
│   ├── profiles.yml
│   └── spark-optimization.md
│
├── load_walmart_data.py
├── Dockerfile
├── docker-compose.yaml
├── requirements.txt
├── .gitignore
└── README.md
```

Local environment files and database credentials are excluded from
version control.

## Prerequisites

-   Docker and Docker Compose
-   Python
-   An Azure Databricks workspace
-   A Databricks SQL warehouse
-   A Databricks access token
-   A TigerData Cloud PostgreSQL database
-   Git

## Setup

### 1. Clone the Repository

``` bash
git clone https://github.com/Trevor04sys/walmart-data-engineering.git
cd walmart-data-engineering
```

### 2. Configure Environment Variables

Create a local `.env` file for environment-specific configuration.

Configure the required Airflow and Databricks settings. Add the
TigerData PostgreSQL connection string as `DATABASE_URL` if you intend
to run the initial CSV loading script.

Do not commit `.env` or expose credentials in the repository.

### 3. Configure dbt

Configure the Databricks connection in:

``` text
walmart_project/profiles.yml
```

Use environment variables for credentials rather than committing
secrets.

Ensure that the configured Databricks environment has access to the
required catalogs, schemas, and tables.

### 4. Load the Source Data

If you are setting up the project with an empty source database, run:

``` bash
python load_walmart_data.py
```

The script loads the CSV files into the corresponding tables in the
`raw` schema.

This is an initial-load operation. Do not rerun it against an already
populated database without preparing the tables first.

### 5. Start Airflow

From the project root, run:

``` bash
docker compose up -d --build
```

Check the running containers:

``` bash
docker compose ps
```

### 6. Access Airflow

Open the Airflow web interface using the port configured in
`docker-compose.yaml`.

Enable and trigger the `orchestrate` DAG to execute the pipeline.

The DAG coordinates the ingestion and transformation workflow.

## CI/CD

GitHub Actions was explored for automated dbt validation and testing.
However, CI/CD is not currently configured in this project.

Pipeline execution and validation are performed through the Airflow and
Databricks environment.

## Security

-   Credentials and access tokens must not be committed.
-   Local environment files are excluded using `.gitignore`.
-   Databricks connection settings should use environment variables.
-   Database credentials should be managed securely.
-   Production credentials should be stored using an appropriate secrets
    management solution.

## Project Status

The end-to-end pipeline has been executed successfully.

The project includes source data migration to TigerData, Databricks
ingestion, dbt transformations, dimensional modeling, data quality
tests, and Airflow orchestration.

Additional enhancements, such as automated CI/CD, can be considered
separately.

## Author

**Raj Pramalick**

Data Engineering \| Azure Databricks \| dbt \| Apache Airflow \| PySpark
