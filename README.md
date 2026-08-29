# spark-snowflake-pipeline

End-to-end data engineering pipeline processing large-scale social media data (tweets), built around **PySpark** and **Snowflake**, with **dbt** for analytical modeling and **Apache Airflow** for orchestration. Infrastructure (Snowflake resources) is provisioned via **Terraform**, and all services run in **Docker**.

---

## Overview

This project ingests a large raw dataset (Sentiment140 — 1.6M labeled tweets) and moves it through a full pipeline:
Kaggle dataset (raw)
→ PySpark (cleaning, transformation)
→ Snowflake staging
→ dbt (staging → intermediate → marts)
→ Snowflake analytics models
Orchestrated by: Airflow
Infrastructure provisioned by: Terraform (Snowflake resources only)
Everything containerized with: Docker

The goal is to demonstrate a realistic, production-shaped data pipeline: infrastructure as code, heavy-data processing, orchestration, and analytical modeling working together.

---

## Tech stack

- **PySpark** — large-scale data cleaning and transformation
- **Snowflake** — data warehouse
- **dbt** — analytical data modeling (SQL)
- **Apache Airflow** — pipeline orchestration
- **Terraform** — infrastructure as code (Snowflake warehouse, database, role)
- **Docker** — containerization for all services
- **Python 3.11+**

---

## Project structure

spark-snowflake-pipeline/
├── terraform/ # Snowflake infrastructure as code
│ ├── main.tf
│ ├── variables.tf
│ └── outputs.tf
├── spark/
│ ├── jobs/ # PySpark transformation jobs
│ └── Dockerfile
├── dbt_project/
│ ├── models/
│ │ ├── staging/
│ │ ├── intermediate/
│ │ └── marts/
│ └── dbt_project.yml
├── airflow/
│ ├── dags/ # Airflow DAGs orchestrating the pipeline
│ └── Dockerfile
├── docker-compose.yml # spins up Airflow + Spark (+ Postgres for Airflow metadata)
├── data/ # raw dataset (gitignored)
├── .gitignore
└── README.md

---

## Requirements

- **Docker** installed and running
- **Terraform** CLI installed
- **Python 3.11+**
- A **Snowflake** account with permissions to create warehouses/databases/roles

---

## Setup

_(to be completed as the project is built)_

```bash
git clone https://github.com/eike-matos/spark-snowflake-pipeline.git
cd spark-snowflake-pipeline
```

More detailed setup instructions (Terraform apply, Docker Compose, Airflow credentials, etc.) will be added as each part of the pipeline is implemented.

---

## Branching workflow

This project follows a simplified environment-branch workflow:
feature → dev → main

- **`main`** — stable, production-ready code
- **`dev`** — integration branch for completed features
- **`users/<username>/<feature-name>`** — individual feature branches

Feature branches are created from `dev`. Once a feature is complete, it's merged into `dev` via Pull Request, and immediately after, `dev` is merged into `main` — keeping both branches always in sync.

---

## Commit conventions

This project follows [Conventional Commits](https://www.conventionalcommits.org/):
<type>: <short description>

Common types used in this project:

| Type       | Use case                                            |
| ---------- | --------------------------------------------------- |
| `feat`     | A new feature (e.g. a new DAG, a new dbt model)     |
| `fix`      | A bug fix                                           |
| `chore`    | Maintenance tasks (deps, config, gitignore, etc.)   |
| `docs`     | Documentation changes only                          |
| `refactor` | Code change that doesn't add a feature or fix a bug |
| `test`     | Adding or updating tests                            |

Examples:
feat: add Terraform config for Snowflake warehouse
fix: correct database role permissions
chore: update gitignore
docs: add README for terraform module
refactor: simplify PySpark cleaning job

---

## Roadmap / status

- [x] Project scaffold
- [ ] Terraform — Snowflake infrastructure
- [ ] Docker Compose — Airflow base setup
- [ ] PySpark — standalone cleaning job
- [ ] Spark containerization
- [ ] Airflow DAG — full pipeline orchestration
- [ ] dbt — staging → marts models
- [ ] Tests + final documentation

---

## Data source

[Sentiment140](https://www.kaggle.com/datasets/kazanova/sentiment140) — 1.6M tweets labeled by sentiment. Not committed to the repository; download instructions will be added in `data/README.md`.
