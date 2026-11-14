from datetime import date
from unittest.mock import MagicMock

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import requests
import responses
from google.api_core.exceptions import NotFound
from google.cloud import bigquery

from nyc_mobility.config import Settings
from nyc_mobility.ingest import taxi
from nyc_mobility.ingest.taxi import (
    delete_month_rows,
    download_parquet,
    load_month,
    prepare_month_table,
    taxi_parquet_url,
)


def _write_parquet(path, tz=None):
    ts = pa.timestamp("us", tz=tz)
    table = pa.table(
        {
            "VendorID": pa.array([1, 2, 1], pa.int64()),
            "tpep_pickup_datetime": pa.array(
                [date(2024, 1, 1)] * 3, pa.date32()
            ).cast(pa.timestamp("ns")).cast(ts),
            "tpep_dropoff_datetime": pa.array(
                [date(2024, 1, 2)] * 3, pa.date32()
            ).cast(pa.timestamp("ns")).cast(ts),
            "fare_amount": pa.array([5.0, 7.5, 9.0], pa.float64()),
        }
    )
    pq.write_table(table, path)
    return path


def test_taxi_parquet_url():
    assert taxi_parquet_url(date(2024, 3, 1)).endswith("yellow_tripdata_2024-03.parquet")


@responses.activate
def test_download_parquet_streams_to_disk(tmp_path):
    url = taxi_parquet_url(date(2024, 3, 1))
    responses.add(responses.GET, url, body=b"PAR1-payload", status=200)
    dest = tmp_path / "f.parquet"
    assert download_parquet(url, dest) == dest
    assert dest.read_bytes() == b"PAR1-payload"
    download_parquet(url, dest)
    assert len(responses.calls) == 1


@responses.activate
def test_download_parquet_raises_on_http_error(tmp_path):
    url = taxi_parquet_url(date(2024, 3, 1))
    responses.add(responses.GET, url, status=403)
    with pytest.raises(requests.HTTPError):
        download_parquet(url, tmp_path / "f.parquet")


def test_prepare_month_table_adds_metadata(tmp_path):
    path = _write_parquet(tmp_path / "a.parquet")
    table = prepare_month_table(path, date(2024, 1, 1))
    assert table.num_rows == 3
    assert table.column("source_month").to_pylist() == [date(2024, 1, 1)] * 3
    assert table.schema.field("source_month").type == pa.date32()
    assert table.schema.field("loaded_at").type == pa.timestamp("us")
    assert len(set(table.column("loaded_at").to_pylist())) == 1
    assert table.schema.field("tpep_pickup_datetime").type == pa.timestamp("us")


def test_prepare_month_table_converts_tz_to_naive(tmp_path):
    path = _write_parquet(tmp_path / "a.parquet", tz="UTC")
    table = prepare_month_table(path, date(2024, 1, 1))
    assert table.schema.field("tpep_pickup_datetime").type == pa.timestamp("us")
    assert table.schema.field("tpep_dropoff_datetime").type.tz is None


def test_delete_month_rows_uses_parameter_and_waits():
    client = MagicMock(spec=bigquery.Client)
    delete_month_rows(client, "p.raw_tlc.yellow_trips", date(2024, 1, 1))
    sql = client.query.call_args.args[0]
    assert "DELETE FROM `p.raw_tlc.yellow_trips`" in sql
    assert "source_month = @month" in sql
    params = client.query.call_args.kwargs["job_config"].query_parameters
    assert params[0].name == "month" and params[0].type_ == "DATE"
    assert params[0].value == date(2024, 1, 1)
    client.query.return_value.result.assert_called_once()


def test_delete_month_rows_tolerates_missing_table():
    client = MagicMock(spec=bigquery.Client)
    client.query.return_value.result.side_effect = NotFound("nope")
    delete_month_rows(client, "p.raw_tlc.yellow_trips", date(2024, 1, 1))


def test_load_month_deletes_existing_month_rows_before_append(tmp_path, mocker):
    src = _write_parquet(tmp_path / "src.parquet")
    mocker.patch.object(taxi, "download_parquet", return_value=src)
    client = MagicMock(spec=bigquery.Client)
    manager = MagicMock()
    manager.attach_mock(client.query, "query")
    manager.attach_mock(client.load_table_from_file, "load")

    rows = load_month(
        client, Settings(project_id="p", location="US"), date(2024, 1, 1), tmp_path
    )

    assert rows == 3
    assert [c[0] for c in manager.mock_calls if c[0] in ("query", "load")] == [
        "query",
        "load",
    ]
    assert client.query.call_count == 1
    assert client.load_table_from_file.call_count == 1
    args = client.load_table_from_file.call_args
    assert args.args[1] == "p.raw_tlc.yellow_trips"
    cfg = args.kwargs["job_config"]
    assert cfg.write_disposition == "WRITE_APPEND"
    assert cfg.source_format == "PARQUET"
    assert list(cfg.schema_update_options) == ["ALLOW_FIELD_ADDITION"]
    client.load_table_from_file.return_value.result.assert_called_once()
    prepared = tmp_path / "yellow_tripdata_2024-01.prepared.parquet"
    assert pq.read_table(prepared).num_rows == 3
