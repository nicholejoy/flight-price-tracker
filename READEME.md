### Flight Price Tracker

## Unit Tests
pip install -r requirements/requirements_dev.txt
pytest tests/unit/test_data_pipeline.py

## Dag Validaiton Tests
pip install -r requirements/requirements_dev.txt
pytest tests/dag_validations/test_flight_price_tracker_dag.py

## Local Deplopyment
Can be used for developemnt testing purposes
Deployment configuraiton largely based on official airflow docker compose file. See https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html for more info

Create .env file in root directory containing:
ELASTIC_PASSWORD=[anything]
FLIGHT_DATA_API_KEY=[API key]
EMAIL_PASSWORD=[application email app password]
EMAIL_ADDRESS_FROM=[application email]
EMAIL_ADDRESS_USER=[application email]
EMAIL_RECIPIENT=[email to which notificaiton should be sent]
docker compose from: https://airflow.apache.org/docs/apache-airflow/2.10.5/docker-compose.yaml
docker compose up airflow-init

Requests cana be made to http://localhost:8080/api/v1/dags/flight_price_tracker/dagRuns
e.g.
url = 'http://localhost:8080/api/v1/dags/flight_price_tracker/dagRuns'

data = {
    "conf": {},
    "dag_run_id": "example_run__" + datetime.now().strftime("%Y%m%d%H%M%S"),
}
response = requests.post(url, json=data, auth=('airflow', 'airflow'))

## Integration Tests
Set up local test environment

Execute integration tests from host
docker compose exec airflow-scheduler  pytest tests/integration/test_price_tracker_dag.py