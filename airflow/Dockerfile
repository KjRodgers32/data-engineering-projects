FROM apache/airflow:2.9.1

USER root
RUN apt-get update -qq && apt-get install wget -qqq

COPY requirements.txt .
COPY credentials .

USER airflow

RUN pip install --no-cache-dir apache-airflow==${AIRFLOW_VERSION} -r requirements.txt