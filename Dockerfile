FROM apache/airflow:2.10.5

WORKDIR /opt/airflow

COPY requirements/requirements_docker.txt /opt/airflow

RUN pip install --no-cache-dir -r requirements_docker.txt