ARG AIRFLOW_BASE_IMAGE="apache/airflow:2.2.4-python3.8"
FROM ${AIRFLOW_BASE_IMAGE}

ENV AIRFLOW_HOME=/opt/airflow

# COPY requirements.txt 

USER airflow
RUN pip install dbt-core \ 
                dbt-snowflake \ 
                apache-airflow-providers-microsoft-azure==3.7.0 \
                apache-airflow-providers-snowflake\ 
                riotwatcher \
                pandas
RUN chown newuser
WORKDIR $AIRFLOW_HOME