import json
from unittest.mock import patch, MagicMock, call
import pytest
from dags.flight_price_tracker.data_pipeline import fetch_data, prepare_price_alerts


def load_json(file_path):
    with open(file_path, "r") as f:
        return json.load(f)


@pytest.fixture
def mock_requests_get():
    mock_response = MagicMock()
    yield mock_response


def test_fetch_data_failure(mock_requests_get):
    mock_requests_get.status_code = 500
    mock_requests_get.json.return_value = {}
    with patch("requests.get", return_value=mock_requests_get):
        with pytest.raises(Exception):
            fetch_data()


def test_fetch_data_empty_response(mock_requests_get):
    mock_requests_get.status_code = 200
    mock_requests_get.json.return_value = {
        "data": {"everywhereDestination": {"results": []}}
    }

    mock_ti = MagicMock()
    kwargs = {"ti": mock_ti}
    with patch("requests.get", return_value=mock_requests_get):
        fetch_data(**kwargs)
    mock_ti.xcom_push.assert_called_with(key="fetched_data", value=[])


def test_prepare_price_alerts_prices_dropped():
    mock_ti = MagicMock()
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response_json = load_json(
        "tests/unit/test_data/test_prepare_price_alerts_input.json"
    )
    mock_response.json.return_value = mock_response_json
    location_price_statistics = {
        "Denmark": (90, 10),
        "Belgium": (100, 10),
        "Albania": (57, 0),
        "Austria": (44, 0),
        "Bulgaria": (47, 0),
        "Croatia": (57, 0),
        "Cyprus": (80, 0),
        "Czechia": (95, 0),
        "Egypt": (260, 0),
        "Estonia": (190, 0),
    }

    mock_ti.xcom_pull.return_value = mock_response.json()["data"][
        "everywhereDestination"
    ]["results"]

    kwargs = {"ti": mock_ti}
    prepare_price_alerts(location_price_statistics, **kwargs)

    data = load_json("tests/unit/test_data/test_prepare_price_alerts_expected.json")
    expected_calls = [call(**call_data) for call_data in data["expected_calls"]]

    mock_ti.xcom_push.assert_has_calls(expected_calls)


def test_prepare_price_alerts_no_price_drops():
    mock_ti = MagicMock()
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response_json = load_json(
        "tests/unit/test_data/test_prepare_price_alerts_input.json"
    )
    mock_response.json.return_value = mock_response_json
    location_price_statistics = {}
    mock_ti.xcom_pull.return_value = mock_response.json()["data"][
        "everywhereDestination"
    ]["results"]

    kwargs = {"ti": mock_ti}
    prepare_price_alerts(location_price_statistics, **kwargs)

    expected_call = load_json(
        "tests/unit/test_data/test_prepare_price_alerts_expected_no_price_drops.json"
    )

    mock_ti.xcom_push.assert_called_once_with(**expected_call)
