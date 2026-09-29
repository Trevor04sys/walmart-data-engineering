\# Walmart Data Engineering Project



An end-to-end data engineering project built using \*\*Apache Airflow, dbt, and Azure Databricks\*\*. The project demonstrates data ingestion, incremental transformations, dimensional modeling, data quality testing, orchestration, and Spark query optimization.



\## Project Overview



This project implements a data pipeline for Walmart's sales data. It uses a medallion architecture to transform raw data into structured, analytics-ready datasets.



The pipeline is orchestrated using Apache Airflow, while dbt manages SQL transformations, snapshots, and data quality tests.



\### Key Features



\- Automated pipeline orchestration using Apache Airflow.

\- Data ingestion and CDC processing using the Databricks SDK.

\- Incremental data transformations using dbt.

\- Medallion architecture with Silver and Gold layers.

\- Slowly Changing Dimensions (SCD Type 2).

\- Fact table modeling with historical dimension lookups.

\- Automated dbt tests and data quality validation.

\- Delta Lake time travel and Change Data Feed experiments.

\- Spark query plan analysis and performance optimization.

\- Airflow task failure and DAG success email notifications.

\- Docker-based deployment.



\## Architecture



```text

&#x20;                 Apache Airflow

&#x20;                       |

&#x20;                       v

&#x20;             Data Ingestion (CDC)

&#x20;                       |

&#x20;                       v

&#x20;              Azure Databricks

&#x20;                       |

&#x20;                       v

&#x20;                Bronze Layer

&#x20;                       |

&#x20;                       v

&#x20;             Silver Technical

&#x20;                       |

&#x20;                       v

&#x20;             Silver Business

&#x20;                   (obt\_b)

&#x20;                       |

&#x20;                       v

&#x20;               Gold Layer

&#x20;                /        \\

&#x20;               v          v

&#x20;         Dimensions    Fact Table

&#x20;         (SCD Type 2)  (fact\_orders)

&#x20;               \\          /

&#x20;                v        v

&#x20;              Data Quality

&#x20;                  Tests

```



Airflow orchestrates the pipeline, while dbt manages the transformations and testing within Databricks.



\## Technology Stack



| Technology | Purpose |

|---|---|

| Python | Pipeline orchestration and data processing |

| Apache Airflow 3 | Workflow orchestration |

| Docker Compose | Containerized deployment |

| Azure Databricks | Data processing and storage |

| Delta Lake | Reliable data storage, time travel, and CDC |

| dbt | SQL transformations, snapshots, and testing |

| PySpark | Distributed data processing |

| Git | Version control |

| GitHub Actions | Planned CI/CD |



\## Data Pipeline



The pipeline follows a sequence of ingestion, transformation, modeling, and validation tasks.



1\. \*\*Data ingestion:\*\* Ingests source data and processes CDC changes using the Databricks SDK.

2\. \*\*Source freshness:\*\* Checks the freshness of source data.

3\. \*\*Silver technical layer:\*\* Applies incremental transformations to the source tables.

4\. \*\*Data quality:\*\* Runs dbt tests to validate transformed data.

5\. \*\*Silver business layer:\*\* Builds `obt\_b`, a business-oriented table at the order-item grain.

6\. \*\*Ephemeral models:\*\* Provides intermediate transformations for downstream models.

7\. \*\*Snapshots:\*\* Captures historical changes using dbt snapshots.

8\. \*\*Gold dimensions:\*\* Builds dimensional models using SCD Type 2.

9\. \*\*Gold fact:\*\* Builds `fact\_orders` with historical dimension lookups.

10\. \*\*Final validation:\*\* Runs data quality tests on the resulting models.



Airflow manages task dependencies and execution order.



\## Data Modeling



The project uses a medallion architecture.



\### Silver Layer



\*\*Silver Technical\*\*



Contains incremental dbt models for the source entities:



\- Customers

\- Employees

\- Orders

\- Order Items

\- Products

\- Stores



\*\*Silver Business\*\*



The `obt\_b` model combines relevant source entities into a business-oriented table.



Its intended grain is one row per order item.



\### Gold Layer



The Gold layer contains dimensional models and a fact table.



\*\*Dimensions\*\*



\- `dim\_customers`

\- `dim\_employees`

\- `dim\_orders`

\- `dim\_products`

\- `dim\_stores`



The dimensions use dbt snapshots to maintain historical records with SCD Type 2.



\*\*Fact Table\*\*



`fact\_orders` stores order-item-level records and uses temporal joins to retrieve the appropriate historical dimension records.



\## Data Quality



Data quality is implemented using dbt tests.



The project includes tests for:



\- Fact table grain and uniqueness.

\- Dimension validity and overlapping historical records.

\- Exactly one current customer dimension record.

\- Temporal consistency between facts and dimensions.

\- Order total reconciliation.

\- Business table grain.



These tests help validate the correctness and consistency of the transformed data.



\## Delta Lake Experiments



The project includes practical experiments with Delta Lake:



\- Time travel to query previous table versions.

\- Change Data Feed (CDF) to inspect data changes.

\- Schema evolution using automatic schema merging.

\- Delta table history analysis.

\- File layout and optimization experiments.



CDF was enabled on the customers technical table for the experiment.



\## Spark Optimization



Spark optimization experiments were performed using Databricks query plans and query profiles.



The experiments covered:



\- Column pruning

\- Predicate pushdown

\- Join strategies

\- Broadcast joins

\- Sort-merge joins

\- Aggregation plans

\- Delta table details

\- Query profile comparisons



The experiments and observations are documented here:



\[`walmart\_project/spark-optimization.md`](walmart\_project/spark-optimization.md)



\## Airflow Monitoring



Airflow is used to monitor pipeline execution.



The project includes:



\- Task retries

\- Task failure email notifications

\- DAG success email notifications

\- Task execution status and logs



Email notifications are configured through Airflow connections.



\## Project Structure



```text

walmart-data-engineering/

│

├── config/

│   └── airflow.cfg

│

├── dags/

│   └── orchestrate.py

│

├── walmart\_project/

│   ├── models/

│   │   ├── source/

│   │   ├── silver\_technical/

│   │   ├── silver\_business/

│   │   └── gold/

│   │       ├── ephemeral/

│   │       └── fact/

│   │

│   ├── snapshots/

│   ├── tests/

│   ├── macros/

│   ├── analyses/

│   ├── seeds/

│   ├── dbt\_project.yml

│   ├── packages.yml

│   ├── profiles.yml

│   └── spark-optimization.md

│

├── Dockerfile

├── docker-compose.yaml

├── requirements.txt

├── .gitignore

└── README.md

```



The local `profiles.yml` contains environment-specific connection settings and is excluded from version control.



\## Prerequisites



\- Docker and Docker Compose

\- Python

\- An Azure Databricks workspace

\- A Databricks SQL warehouse

\- A Databricks access token



\## Setup



\### 1. Clone the repository



```bash

git clone https://github.com/<your-username>/walmart-data-engineering.git

cd walmart-data-engineering

```



\### 2. Configure environment variables



Create a `.env` file for your local configuration.



Add the required Airflow and Databricks settings. Keep credentials and tokens out of version control.



\### 3. Configure dbt



Configure the Databricks connection in:



```text

walmart\_project/profiles.yml

```



Use environment variables for credentials rather than committing secrets.



\### 4. Start Airflow



From the project root, run:



```bash

docker compose up -d --build

```



Check the running containers:



```bash

docker compose ps

```



\### 5. Access Airflow



Open the Airflow web interface using the port configured in `docker-compose.yaml`.



Enable and trigger the `orchestrate` DAG to execute the pipeline.



\## CI/CD



GitHub Actions integration is planned to automate project validation and testing.



The intended workflow includes:



\- dbt project validation

\- SQL and configuration checks

\- Automated testing

\- Pipeline-related quality checks



\## Security



\- Credentials and access tokens must not be committed.

\- Local environment files are excluded using `.gitignore`.

\- Databricks connection settings should use environment variables.

\- Production credentials should be managed securely.



\## Author



\*\*Raj Pramalick\*\*



Data Engineering | Azure Databricks | dbt | Apache Airflow | PySpark

