FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir dbt-snowflake

COPY . /app

ENTRYPOINT ["dbt"]
CMD ["run"]
