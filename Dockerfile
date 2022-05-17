ARG AIRFLOW_BASE_IMAGE="apache/airflow:2.2.4-python3.8"
FROM ${AIRFLOW_BASE_IMAGE}

# COPY requirements.txt .
USER airflow
RUN pip install dbt \ 
                apache-airflow-providers-microsoft-azure==3.7.0 \
                apache-airflow-providers-snowflake[slack]\ 
                riotwatcher \
                pandas