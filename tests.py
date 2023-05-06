from datetime import datetime
from unittest.mock import Mock, patch

import pytest

import db
from lambda_function import get_table_list

# ENV environment should be set to 'test'


@pytest.fixture
def file_table_list():
    return b"table1;date_column1\ntable2;date_column2"


def test_get_table_list_ok(file_table_list):
    with patch("boto3.client") as mock_s3_client:
        mock_response = {"Body": Mock(read=Mock(return_value=file_table_list))}
        mock_s3_client.return_value.get_object.return_value = mock_response

        tables = get_table_list()
        mock_s3_client.assert_called_once()
        assert isinstance(tables, dict)
        assert tables
        assert tables["table1"] == "date_column1"
        assert tables["table2"] == "date_column2"


def test_get_table_list_exception():
    with patch("boto3.client") as mock_s3_client:
        mock_s3_client.return_value.get_object.side_effect = Exception("mocked error")

        tables = get_table_list()
        mock_s3_client.assert_called_once()
        assert isinstance(tables, dict)
        assert not tables


@pytest.mark.parametrize("text", [b"", b"\n", b"\n\n", b" "])
def test_get_table_list_file_not_found(text):
    with patch("boto3.client") as mock_s3_client:
        mock_response = {"Body": Mock(read=Mock(return_value=text))}
        mock_s3_client.return_value.get_object.return_value = mock_response

        tables = get_table_list()
        mock_s3_client.assert_called_once()
        assert isinstance(tables, dict)
        assert not tables


def test_get_data_with_empty_results():
    with patch("db.connect") as mock_connect:
        mock_cursor = mock_connect.return_value.cursor.return_value
        mock_cursor.description = [("id",), ("name",), ("date",)]
        mock_cursor.fetchall.return_value = []
        columns, data = db.get_data("my_table", "date", datetime(2021, 1, 1))
        assert columns == ["id", "name", "date"]
        assert data == []


def test_get_data_with_results():
    with patch("db.connect") as mock_connect:
        mock_cursor = mock_connect.return_value.cursor.return_value
        mock_cursor.description = [("id",), ("name",), ("date",)]
        mock_cursor.fetchall.return_value = [(1, "John", datetime(2020, 12, 31))]
        columns, data = db.get_data("my_table", "date", datetime(2021, 1, 1))
        assert columns == ["id", "name", "date"]
        assert data == [(1, "John", datetime(2020, 12, 31))]


def test_get_data_with_db_connection_error():
    with patch("db.connect", side_effect=Exception("Connection error")):
        columns, data = db.get_data("my_table", "date", datetime(2021, 1, 1))
        assert columns == []
        assert data == []


def test_get_data_with_query_error():
    with patch("db.connect") as mock_connect:
        mock_cursor = mock_connect.return_value.cursor.return_value
        mock_cursor.execute.side_effect = Exception("Query error")
        columns, data = db.get_data("my_table", "date", datetime(2021, 1, 1))
        assert columns == []
        assert data == []
