import os
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

HOST_PROJECT_DIR = os.environ["HOST_PROJECT_DIR"]

default_args = {
    "owner": "eike-matos",
    "retries": 1,
}

with DAG(
    dag_id="social_pipeline_dag",
    description="Cleans Sentiment140 with PySpark, loads it into Snowflake, and runs dbt models",
    default_args=default_args,
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["spark", "snowflake", "dbt"],
) as dag:

    clean_tweets = DockerOperator(
        task_id="clean_tweets_with_spark",
        image="spark-cleaning-job",
        api_version="auto",
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        mount_tmp_dir=False,
        mounts=[
            Mount(
                source=f"{HOST_PROJECT_DIR}/data",
                target="/app/data",
                type="bind",
            ),
        ],
    )

    def load_to_snowflake(**context):
        import glob
        import pandas as pd
        import snowflake.connector
        from snowflake.connector.pandas_tools import write_pandas

        parquet_files = glob.glob("/opt/airflow/data/staging/tweets_clean/*.parquet")
        df = pd.concat([pd.read_parquet(f) for f in parquet_files], ignore_index=True)

        df["created_at"] = pd.to_datetime(df["created_at"]).dt.strftime("%Y-%m-%d %H:%M:%S")

        conn = snowflake.connector.connect(
            account="TTRNRTZ-BUB61223",
            user="EIKEMATOS",
            private_key_file="/opt/airflow/keys/snowflake_key.p8",
            authenticator="SNOWFLAKE_JWT",
            warehouse="SPARK_PIPELINE_WH",
            database="SPARK_PIPELINE_DB",
            role="ACCOUNTADMIN",
        )

        success, nchunks, nrows, _ = write_pandas(
            conn=conn,
            df=df,
            table_name="TWEETS_CLEAN",
            schema="PUBLIC",
            auto_create_table=False,
            overwrite=True,
        )

        print(f"Loaded {nrows} rows into Snowflake in {nchunks} chunks. Success: {success}")
        conn.close()

    load_tweets = PythonOperator(
        task_id="load_tweets_to_snowflake",
        python_callable=load_to_snowflake,
    )

    run_dbt_models = DockerOperator(
        task_id="run_dbt_models",
        image="dbt-transform-job",
        api_version="auto",
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        mount_tmp_dir=False,
        command="run",
        mounts=[
            Mount(
                source=f"{HOST_PROJECT_DIR}/dbt_project",
                target="/app",
                type="bind",
            ),
            Mount(
                source=f"{HOST_PROJECT_DIR}/dbt_project/profiles.yml",
                target="/root/.dbt/profiles.yml",
                type="bind",
            ),
            Mount(
                source=f"{HOST_PROJECT_DIR}/terraform/.snowflake_keys",
                target="/keys",
                type="bind",
                read_only=True,
            ),
        ],
    )

    clean_tweets >> load_tweets >> run_dbt_models
