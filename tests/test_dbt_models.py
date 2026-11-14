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
