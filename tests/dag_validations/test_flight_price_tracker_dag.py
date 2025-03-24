from airflow.models import DagBag
from dags.flight_price_tracker.flight_price_tracker_dag import flight_price_dag


def test_dag_loading():
    dag_bag = DagBag(dag_folder="dags/flight_price_tracker")
    assert len(dag_bag.import_errors) == 0, f"Import errors: {dag_bag.import_errors}"
    dag = dag_bag.get_dag("flight_price_tracker")
    assert dag is not None


def test_task_cirularity():
    fetch_data_task = flight_price_dag.get_task("fetch_data")
    prepare_price_alerts_task = flight_price_dag.get_task("prepare_price_alerts")
    index_data_task = flight_price_dag.get_task("index_data")
    should_continue_task = flight_price_dag.get_task("should_continue")
    generate_email_content = flight_price_dag.get_task("generate_email_content")
    send_email = flight_price_dag.get_task("send_email")
    downstream_tasks = [
        prepare_price_alerts_task,
        index_data_task,
        should_continue_task,
        generate_email_content,
        send_email,
    ]
    for task in downstream_tasks:
        assert task not in fetch_data_task.downstream_task_ids
