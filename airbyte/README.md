# Airbyte path (optional)

The default weather ingestion is the Python loader
(`airflow/include/nyc_mobility/ingest/weather.py`). This directory holds an
optional Airbyte alternative.

## Use it

1. In Airbyte, open Connector Builder and import
   `connector_builder_open_meteo.yaml`. Adjust the `start_datetime` and
   `end_datetime` window if needed.
2. Publish the connector, then create a connection with a BigQuery destination
   writing to the dataset `raw_weather`.
3. The stream lands as `raw_weather.open_meteo_archive`. Each record is one
   monthly API response, so the `hourly` arrays are unflattened.
4. Set `weather_source: airbyte` in dbt. The staging model flattens the arrays
   into hourly rows.

The manifest could not be validated offline. Test the stream in the Connector
Builder before relying on it.
