FROM apache/airflow:2.9.2

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends default-jdk && \
    apt-get autoremove -yqq --purge && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER airflow

RUN pip install --no-cache-dir pyspark==3.4.1 requests
