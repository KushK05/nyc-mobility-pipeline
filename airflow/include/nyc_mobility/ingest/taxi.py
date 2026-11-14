"""Idempotent monthly loader for NYC TLC yellow taxi Parquet files."""

from datetime import date, datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import requests
from google.api_core.exceptions import NotFound
from google.cloud import bigquery

from nyc_mobility.config import Settings

TAXI_URL_TEMPLATE = (
    "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{yyyy_mm}.parquet"
)
CHUNK_SIZE = 8 * 1024 * 1024
TIMESTAMP_COLUMNS = ("tpep_pickup_datetime", "tpep_dropoff_datetime")


def taxi_parquet_url(month: date) -> str:
    return TAXI_URL_TEMPLATE.format(yyyy_mm=month.strftime("%Y-%m"))


def download_parquet(
    url: str, dest: Path, session: requests.Session | None = None
) -> Path:
    if dest.exists() and dest.stat().st_size > 0:
        return dest
    dest.parent.mkdir(parents=True, exist_ok=True)
    part = dest.with_name(dest.name + ".part")
    http = session or requests
    with http.get(url, stream=True, timeout=60) as response:
        response.raise_for_status()
        with part.open("wb") as fh:
            for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
                fh.write(chunk)
    part.replace(dest)
    return dest


def _to_naive_us(column: pa.ChunkedArray) -> pa.ChunkedArray:
    if column.type.tz is not None:
        # Convert to UTC wall time, then drop the timezone.
        column = column.cast(pa.timestamp(column.type.unit, tz="UTC"))
        column = pc.cast(column, pa.timestamp(column.type.unit))
    return column.cast(pa.timestamp("us"))


def prepare_month_table(path: Path, month: date) -> pa.Table:
    table = pq.read_table(path)
    for name in TIMESTAMP_COLUMNS:
        index = table.schema.get_field_index(name)
        table = table.set_column(
            index, pa.field(name, pa.timestamp("us")), _to_naive_us(table.column(name))
        )
    loaded_at = datetime.now(timezone.utc).replace(tzinfo=None)
    return table.append_column(
        pa.field("source_month", pa.date32()),
        pa.array([month] * table.num_rows, pa.date32()),
    ).append_column(
        pa.field("loaded_at", pa.timestamp("us")),
        pa.array([loaded_at] * table.num_rows, pa.timestamp("us")),
    )


def delete_month_rows(client: bigquery.Client, table_id: str, month: date) -> None:
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("month", "DATE", month)]
    )
    sql = f"DELETE FROM `{table_id}` WHERE source_month = @month"
    try:
        client.query(sql, job_config=job_config).result()
    except NotFound:
        pass  # first load: the table does not exist yet


def load_month(
    client: bigquery.Client, settings: Settings, month: date, workdir: Path
) -> int:
    yyyy_mm = month.strftime("%Y-%m")
    table_id = f"{settings.project_id}.{settings.raw_tlc_dataset}.yellow_trips"
    source = download_parquet(
        taxi_parquet_url(month), workdir / f"yellow_tripdata_{yyyy_mm}.parquet"
    )
    table = prepare_month_table(source, month)
    prepared = workdir / f"yellow_tripdata_{yyyy_mm}.prepared.parquet"
    pq.write_table(table, prepared)

    delete_month_rows(client, table_id, month)
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
    )
    with prepared.open("rb") as fh:
        client.load_table_from_file(fh, table_id, job_config=job_config).result()
    return table.num_rows
