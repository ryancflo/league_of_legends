ARG AIRFLOW_BASE_IMAGE="apache/airflow:2.2.4-python3.8"
FROM ${AIRFLOW_BASE_IMAGE}

ENV AIRFLOW_HOME=/opt/airflow
WORKDIR $AIRFLOW_HOME
# COPY requirements.txt

USER root
RUN chmod -R 777 /opt/airflow

USER airflow
RUN pip install dbt-core \ 
                dbt-snowflake \ 
                apache-airflow-providers-microsoft-azure==3.7.0 \
                apache-airflow-providers-snowflake\ 
                riotwatcher \
                pandas


