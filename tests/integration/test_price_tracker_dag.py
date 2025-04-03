from datetime import datetime, timezone
from unittest.mock import patch, Mock, MagicMock

import pytest
import requests
from airflow.models import DagBag
from airflow.hooks.base import BaseHook
from airflow.utils.state import State
from airflow.models import DagModel
from airflow.utils.session import create_session

from elasticsearch import Elasticsearch, helpers
from elasticsearch.exceptions import ConnectionError
from dags.flight_price_tracker.config import ELASTIC_PASSWORD, INDEX, ELASTIC_CONN_NAME


conn = BaseHook.get_connection(ELASTIC_CONN_NAME)
es = Elasticsearch([conn.host], basic_auth=("elastic", ELASTIC_PASSWORD))
DAG_ID = "flight_price_tracker"
dag = DagBag(include_examples=False).get_dag(DAG_ID)

TEST_LOCATION = "TEST"
INPUT_TEST_ITEM = {
    "data": {
        "everywhereDestination": {
            "results": [
                {
                    "id": "location-29475373",
                    "type": "LOCATION",
                    "content": {
                        "location": {
                            "id": "29475373",
                            "skyCode": TEST_LOCATION,
                            "name": TEST_LOCATION,
                            "type": TEST_LOCATION,
                        },
                        "flightQuotes": {
                            "cheapest": {
                                "price": "$34",
                                "rawPrice": 34.0,
                                "direct": True,
                            },
                            "direct": {
                                "price": "$34",
                                "rawPrice": 34.0,
                                "direct": True,
                            },
                        },
                        "image": {
                            "url": "https://content.skyscnr.com/d22c8f93938b699739ba5e9d682019af/GettyImages-474603829.jpg"
                        },
                        "flightRoutes": {"directFlightsAvailable": True},
                    },
                    "skyId": TEST_LOCATION,
                }
            ]
        }
    }
}
INDEXED_TEST_ITEM = {
    "sky_id": TEST_LOCATION,
    "location": TEST_LOCATION,
    "cheapest_price": 34.0,
}


def verify_data_indexed(expected_indexed_item: dict, expected_number_of_items: int):
    items_to_fetch = 50
    query = {
        "query": {"term": {"location.keyword": TEST_LOCATION}},
        "size": items_to_fetch,
    }
    es_data = es.search(index=INDEX, body=query)
    matches = es_data.get("hits", {}).get("hits", [])
    assert (
        len(matches) == expected_number_of_items
    ), "Incorrect number of elements indexed"
    latest_match = max(matches, key=lambda match: match["_source"]["timestamp"])
    datestamp = datetime.fromisoformat(latest_match["_source"]["timestamp"]).date()
    assert (
        datestamp == datetime.now(timezone.utc).date()
    ), "incorrrect timestamp indexed"
    del latest_match["_source"]["timestamp"]
    assert (
        latest_match["_source"] == expected_indexed_item
    ), "flight data not indexed correctly added"


def cleanup_index():
    es.delete_by_query(
        index=INDEX, body={"query": {"term": {"location.keyword": TEST_LOCATION}}}
    )


def pause_dag(dag_id: str):
    with create_session() as session:
        dag = session.query(DagModel).filter(DagModel.dag_id == dag_id).first()
        if dag:
            dag.is_paused = True
            session.commit()


@pytest.fixture(scope="session", autouse=True)
def setup_once():
    pause_dag(DAG_ID)
    cleanup_index()
    yield


@pytest.fixture(scope="function", autouse=True)
def cleanup_elasticsearch_after_test():
    yield
    cleanup_index()


def mock_requests_get(url, *args, **kwargs):
    if url == "https://sky-scanner3.p.rapidapi.com/flights/search-roundtrip":
        return Mock(status_code=200, json=lambda: INPUT_TEST_ITEM)
    return requests.get(url, *args, **kwargs, timeout=10)


def test_dag_execution_no_notification(cleanup_elasticsearch_after_test):
    with patch("requests.get", side_effect=mock_requests_get), patch(
        "airflow.operators.email.EmailOperator.execute", new=MagicMock()
    ) as mock_email_execute:
        mock_email_execute.__name__ = "execute"
        mock_email_execute.return_value = None
        dag.test()
    es.indices.refresh(index=INDEX)
    assert (
        mock_email_execute.call_count == 0
    ), "Email notification was triggered without a price drop"
    verify_data_indexed(INDEXED_TEST_ITEM, 1)


def test_dag_execution_send_notification(cleanup_elasticsearch_after_test):
    n_items_indexed = 30
    if not es.indices.exists(index=INDEX):
        es.indices.create(index=INDEX)
    docs = [
        {
            "sky_id": TEST_LOCATION,
            "location": TEST_LOCATION,
            "cheapest_price": 100.0,
            "timestamp": "2021-01-16T19:25:46.590256",
        }
        for x in range(n_items_indexed)
    ]
    actions = [{"_op_type": "index", "_index": INDEX, "_source": doc} for doc in docs]
    helpers.bulk(es, actions)
    es.indices.refresh(index=INDEX)
    with patch("requests.get", side_effect=mock_requests_get), patch(
        "airflow.operators.email.EmailOperator.execute", new=MagicMock()
    ) as mock_email_execute:
        mock_email_execute.__name__ = "execute"
        mock_email_execute.return_value = None
        dag.test()
    es.indices.refresh(index=INDEX)
    assert (
        mock_email_execute.call_count == 1
    ), "Email notificaiton was not triggered by the lower price"
    verify_data_indexed(INDEXED_TEST_ITEM, n_items_indexed + 1)


def test_dag_failed_to_fetch_data():
    with patch("requests.get") as mock_get:

        def side_effect(url, *args, **kwargs):
            if url == "https://sky-scanner3.p.rapidapi.com/flights/search-roundtrip":
                raise requests.exceptions.RequestException("Simulated failure")
            return requests.get(url, *args, **kwargs)

        mock_get.side_effect = side_effect
        dag.test()
    assert dag.get_task("fetch_data").get_task_instances()[-1].state == State.FAILED


def test_dag_failed_to_index():
    with patch(
        "elasticsearch.helpers.bulk",
        side_effect=ConnectionError("Simulated bulk failure"),
    ):
        dag.test()
    assert dag.get_task("index_data").get_task_instances()[-1].state == State.FAILED
