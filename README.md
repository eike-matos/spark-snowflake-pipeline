# spark-snowflake-pipeline

End-to-end data engineering pipeline that ingests, cleans, loads, and models large-scale social media sentiment data. Built around **PySpark** and **Snowflake**, orchestrated by **Apache Airflow**, modeled with **dbt**, with infrastructure provisioned via **Terraform** and every processing step fully containerized with **Docker**.

---

## Overview

The pipeline ingests the [Sentiment140](https://www.kaggle.com/datasets/kazanova/sentiment140) dataset (1.6M labeled tweets) and moves it through a fully automated flow, triggered by a single Airflow DAG:
Kaggle dataset (raw CSV, ~1.6M tweets)
→ PySpark (containerized) — cleans and transforms the raw data
→ Snowflake staging — TWEETS_CLEAN table
→ dbt (containerized) — staging → mart models
→ Snowflake analytics tables
Orchestrated end-to-end by: Airflow (DockerOperator + PythonOperator)
Infrastructure provisioned by: Terraform (Snowflake warehouse, database, role)
Every processing step runs in its own Docker container

A single DAG run (`social_pipeline_dag`) executes all three stages in sequence — Spark cleaning, Snowflake load, and dbt transformation — with no manual steps in between.

---

## Tech stack

- **PySpark** — large-scale data cleaning and transformation (`apache/spark:3.5.3-python3` image)
- **Snowflake** — data warehouse, authenticated via key-pair (JWT), not password
- **dbt** — analytical data modeling (staging → marts), containerized
- **Apache Airflow** — pipeline orchestration (`LocalExecutor`, Postgres metadata DB)
- **Terraform** — infrastructure as code for Snowflake (warehouse, database, role, grants)
- **Docker / Docker Compose** — containerization for every service and processing step
- **Python 3.11**

---

## Project structure

spark-snowflake-pipeline/
├── terraform/
│ ├── main.tf # provider config (key-pair auth)
│ ├── variables.tf
│ ├── resources.tf # warehouse, database, role, grants
│ ├── outputs.tf
│ └── .snowflake_keys/ # RSA key pair (gitignored)
├── spark/
│ ├── jobs/
│ │ └── clean_raw_data.py # PySpark cleaning job
│ └── Dockerfile
├── dbt_project/
│ ├── models/
│ │ ├── staging/
│ │ │ ├── stg_tweets.sql
│ │ │ └── \_staging**sources.yml
│ │ └── marts/
│ │ ├── mart_sentiment_by_day.sql
│ │ ├── mart_top_users.sql
│ │ └── \_marts**models.yml
│ ├── dbt_project.yml
│ ├── Dockerfile
│ └── profiles.yml # gitignored — real credentials
├── airflow/
│ ├── dags/
│ │ └── social_pipeline_dag.py # orchestrates Spark → Snowflake → dbt
│ ├── Dockerfile
│ └── requirements.txt
├── docker-compose.yml # Airflow (Postgres + webserver + scheduler)
├── data/ # raw + staged data (gitignored)
├── .env / .env.example
├── .gitignore
└── README.md

---

## Requirements

- **Docker Desktop** installed and running
- **Terraform** CLI installed
- **Python 3.11+**
- A **Snowflake** account with permissions to create warehouses/databases/roles
- The [Sentiment140](https://www.kaggle.com/datasets/kazanova/sentiment140) dataset downloaded to `data/`

---

## Setup

### 1. Infrastructure (Terraform)

Snowflake authentication uses **key-pair (JWT) auth**, not password — required for service/automation users as of Snowflake's 2026 auth deprecation milestones.

```bash
cd terraform
mkdir -p .snowflake_keys
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out .snowflake_keys/snowflake_key.p8 -nocrypt
openssl rsa -in .snowflake_keys/snowflake_key.p8 -pubout -out .snowflake_keys/snowflake_key.pub
```

Register the public key on your Snowflake user (`ALTER USER <user> SET RSA_PUBLIC_KEY='...'`), then create `terraform.tfvars` (gitignored) with your organization/account/user, and:

```bash
terraform init
terraform plan
terraform apply
```

This provisions `SPARK_PIPELINE_WH`, `SPARK_PIPELINE_DB`, and `SPARK_PIPELINE_ROLE`.

### 2. Airflow (Docker Compose)

```bash
echo "AIRFLOW_UID=$(id -u)" > .env
echo "HOST_PROJECT_DIR=$(pwd)" >> .env

docker compose up airflow-init
docker compose up -d
```

Access the UI at `http://localhost:8080` (`admin` / `admin`).

### 3. Spark job image

```bash
cd spark
docker build -t spark-cleaning-job .
```

### 4. dbt

```bash
pip install dbt-snowflake
cp ~/.dbt/profiles.yml dbt_project/profiles.yml   # container-facing copy, gitignored

cd dbt_project
docker build -t dbt-transform-job .
```

### 5. Run the pipeline

In the Airflow UI, trigger `social_pipeline_dag`. It runs three tasks in sequence: `clean_tweets_with_spark` → `load_tweets_to_snowflake` → `run_dbt_models`.

---

## Architecture notes & challenges solved

A few non-obvious problems came up building this, worth documenting:

- **Snowflake key-pair authentication**: Snowflake deprecated password auth for service/automation users through 2026. Both Terraform and dbt authenticate via RSA key-pair (JWT), not username/password.
- **Docker-outside-of-Docker (DooD)**: Airflow runs inside a container but needs to launch sibling containers (Spark, dbt) on the host's Docker daemon. This required mounting `/var/run/docker.sock` into the Airflow containers and passing host-absolute paths (`HOST_PROJECT_DIR`) for volume mounts — a container path is meaningless to the host Docker daemon.
- **pandas datetime → Snowflake type mismatch**: `write_pandas` didn't reliably infer `TIMESTAMP_NTZ` from a `datetime64` column, silently writing epoch nanoseconds as `NUMBER(38,0)` instead. Fixed by explicitly creating the table with the correct type and converting the column to an ISO-formatted string before load, letting Snowflake's `COPY INTO` handle the conversion unambiguously.
- **Third-party image churn**: the original plan to containerize Spark with `bitnami/spark` broke because Bitnami moved its public images to a legacy/paid tier in 2025–2026. Switched to the officially maintained `apache/spark` image instead.

---

## Branching workflow

feature → dev → main

- **`main`** — stable, production-ready code
- **`dev`** — integration branch for completed features
- **`users/<username>/<feature-name>`** — individual feature branches

Feature branches are created from `dev`. Once a feature is complete, it's merged into `dev` via Pull Request, and immediately after, `dev` is merged into `main` — keeping both branches always in sync.

---

## Commit conventions

This project follows [Conventional Commits](https://www.conventionalcommits.org/):
<type>: <short description>

| Type       | Use case                                                 |
| ---------- | -------------------------------------------------------- |
| `feat`     | A new feature (a DAG, a dbt model, a Terraform resource) |
| `fix`      | A bug fix                                                |
| `chore`    | Maintenance tasks (deps, config, gitignore, etc.)        |
| `docs`     | Documentation changes only                               |
| `refactor` | Code change that doesn't add a feature or fix a bug      |
| `test`     | Adding or updating tests                                 |

---

## Roadmap / status

- [x] Project scaffold
- [x] Terraform — Snowflake infrastructure
- [x] Docker Compose — Airflow base setup
- [x] PySpark — standalone cleaning job
- [x] Spark containerization
- [x] Airflow DAG — full pipeline orchestration
- [x] dbt — staging → marts models
- [x] Full pipeline automation (Spark → Snowflake → dbt via Airflow)
- [x] Documentation

---

## Data source

[Sentiment140](https://www.kaggle.com/datasets/kazanova/sentiment140) — 1.6M tweets labeled by sentiment (Stanford University). Not committed to the repository; download it to `data/` before running the pipeline.
