FROM apache/spark:3.5.3-python3

USER root

WORKDIR /app

COPY jobs/ /app/jobs/

ENTRYPOINT ["/opt/spark/bin/spark-submit"]
CMD ["/app/jobs/clean_raw_data.py"]
