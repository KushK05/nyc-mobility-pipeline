import re
from pathlib import Path

MODELS = Path(__file__).resolve().parents[1] / "dbt" / "nyc_mobility" / "models"
INTERMEDIATE = MODELS / "intermediate"


def _text(name: str) -> str:
    return re.sub(r"\s+", " ", (INTERMEDIATE / name).read_text().lower())


def test_validated_model_contains_every_rule():
    text = _text("int_trips_validated.sql")
    literals = [
        "'negative_fare'",
        "'zero_distance_high_fare'",
        "'dropoff_before_pickup'",
        "'duration_over_6h'",
        "'pickup_outside_source_month'",
    ]
    positions = [text.find(lit) for lit in literals]
    assert all(p >= 0 for p in positions), positions
    assert positions == sorted(positions)


def test_deduped_uses_surrogate_key():
    text = _text("int_trips_deduped.sql")
    assert "generate_surrogate_key" in text
    for col in ["vendor_id", "pickup_datetime", "dropoff_datetime", "pu_location_id", "do_location_id", "fare_amount"]:
        assert f"'{col}'" in text


def test_with_weather_joins_on_hour():
    assert "datetime_trunc(pickup_datetime, hour)" in _text("int_trips_with_weather.sql")


MARTS = MODELS / "marts"


def test_fct_trips_config_is_partitioned_and_filtered():
    text = re.sub(r"\s+", " ", (MARTS / "fct_trips.sql").read_text().lower())
    assert "insert_overwrite" in text
    assert "require_partition_filter=true" in text.replace(" ", "")
    assert "cluster_by=['pu_location_id','do_location_id']" in text.replace(" ", "")
    assert "lookback_days" in text
    assert "rejection_reason" not in text


def test_marts_yml_has_required_tests():
    import yaml

    doc = yaml.safe_load((MARTS / "_marts.yml").read_text())
    models = {m["name"]: m for m in doc["models"]}
    cols = {c["name"]: c for c in models["fct_trips"]["columns"]}

    def kinds(col):
        return [t if isinstance(t, str) else next(iter(t)) for t in cols[col].get("tests", [])]

    def cfg(col, kind):
        return next(t[kind] for t in cols[col]["tests"] if isinstance(t, dict) and kind in t)

    assert {"unique", "not_null"} <= set(kinds("trip_id"))
    for col in ["pu_location_id", "do_location_id"]:
        rel = cfg(col, "relationships")
        assert rel["to"] == "ref('dim_zone')" and rel["field"] == "location_id"
    assert cfg("payment_type", "accepted_values")["values"] == [1, 2, 3, 4, 5, 6]
    assert cfg("fare_amount", "dbt_utils.accepted_range")["min_value"] == 0
    assert cfg("trip_duration_min", "dbt_utils.accepted_range")["max_value"] == 360
    for name in ["fct_trips", "agg_hourly_zone_demand", "agg_daily_weather_impact"]:
        assert all(c.get("description") for c in models[name]["columns"]), name


def test_fct_trips_tests_have_partition_where():
    import yaml

    doc = yaml.safe_load((MARTS / "_marts.yml").read_text())
    fct = next(m for m in doc["models"] if m["name"] == "fct_trips")
    count = 0
    for col in fct["columns"]:
        for t in col.get("tests", []):
            assert isinstance(t, dict), (col["name"], t)
            body = next(iter(t.values())) or {}
            where = body["config"]["where"]
            assert "pickup_date between" in where and "run_month_start" in where
            count += 1
    assert count == 7


def test_quarantine_trips_is_incremental_insert_overwrite():
    text = (MARTS / "quarantine_trips.sql").read_text()
    assert "insert_overwrite" in text
    assert "require_partition_filter" not in text
