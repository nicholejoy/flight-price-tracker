import sys
import os
import logging
from elasticsearch import Elasticsearch, helpers
from airflow.hooks.base import BaseHook

from airflow.exceptions import AirflowException


sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from config import ELASTIC_PASSWORD, INDEX, MIN_COUNT, ELASTIC_CONN_NAME

logger = logging.getLogger("airflow.task")


class ElasticsearchConnection:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            instance = super().__new__(cls)
            conn = BaseHook.get_connection(ELASTIC_CONN_NAME)
            instance.es = Elasticsearch(
                [conn.host], basic_auth=("elastic", ELASTIC_PASSWORD)
            )
            cls._instance = instance
        return cls._instance.es

    @classmethod
    def create_elasticsearch_index(cls):
        es = cls()
        mapping = {
            "mappings": {
                "properties": {
                    "sky_id": {
                        "type": "text",
                        "fields": {"keyword": {"type": "keyword", "ignore_above": 256}},
                    },
                    "location": {
                        "type": "text",
                        "fields": {"keyword": {"type": "keyword", "ignore_above": 256}},
                    },
                    "cheapest_price": {"type": "float"},
                    "timestamp": {"type": "date", "format": "date_optional_time"},
                }
            }
        }
        try:
            if not es.indices.exists(index=INDEX):
                es.indices.create(index=INDEX, body=mapping)
        except Exception as e:
            raise AirflowException(f"Error creating index '{INDEX}': {e}") from e

    @classmethod
    def delete_elasticsearch_index(cls, index_name):
        es = cls()
        try:
            if es.indices.exists(index=index_name):
                es.indices.delete(index=index_name)
        except Exception as e:
            raise AirflowException(f"Error deleting index '{index_name}': {e}") from e

    @classmethod
    def index_data(cls, **kwargs):
        es = cls()
        if not es.indices.exists(index=INDEX):
            es.indices.create(index=INDEX)
        docs = kwargs["ti"].xcom_pull(task_ids="prepare_price_alerts", key="all_rows")
        actions = [
            {"_op_type": "index", "_index": INDEX, "_source": doc} for doc in docs
        ]
        helpers.bulk(es, actions)

    @classmethod
    def get_location_price_statistics(cls):
        """Fetches average and stadard deviation of prices per location."""
        es = cls()
        query = {
            "size": 0,
            "aggs": {
                "by_location": {
                    "terms": {"field": "location.keyword", "size": 1000},
                    "aggs": {
                        "average_price": {"avg": {"field": "cheapest_price"}},
                        "price_std_deviation": {
                            "extended_stats": {"field": "cheapest_price"}
                        },
                    },
                }
            },
        }
        response = es.search(index=INDEX, body=query)
        if "aggregations" not in response:
            logger.info("Error: No aggregations found in response.")
            return None
        location_price_statistics = {
            bucket["key"]: (
                bucket["average_price"]["value"],
                bucket["price_std_deviation"]["std_deviation"],
            )
            for bucket in response["aggregations"]["by_location"]["buckets"]
            if bucket["doc_count"] >= MIN_COUNT
        }
        return location_price_statistics
