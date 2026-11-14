"""Column catalog of the dbt marts, mirrored from dbt/nyc_mobility/models/marts/_marts.yml."""

MART_SCHEMA: dict[str, list[tuple[str, str, str]]] = {
    "fct_trips": [
        ("trip_id", "STRING", "Surrogate key over vendor, pickup, dropoff, locations and fare."),
        ("vendor_id", "INT64", "TLC vendor code."),
        ("pickup_datetime", "DATETIME", "Timestamp when the meter was engaged."),
        ("dropoff_datetime", "DATETIME", "Timestamp when the meter was disengaged."),
        ("pickup_date", "DATE", "Calendar date of the pickup (partition column)."),
        ("pickup_hour", "INT64", "Hour of day of the pickup, 0 to 23."),
        ("pu_location_id", "INT64", "Pickup taxi zone id, joins to dim_zone.location_id."),
        ("do_location_id", "INT64", "Dropoff taxi zone id, joins to dim_zone.location_id."),
        ("passenger_count", "INT64", "Number of passengers reported by the driver."),
        ("trip_distance", "FLOAT64", "Trip distance in miles."),
        ("trip_duration_min", "FLOAT64", "Trip duration in minutes."),
        (
            "payment_type",
            "INT64",
            "Payment type code (1 card, 2 cash, 3 no charge, 4 dispute, 5 unknown, 6 voided).",
        ),
        ("fare_amount", "FLOAT64", "Metered time-and-distance fare in USD."),
        ("tip_amount", "FLOAT64", "Tip in USD (card payments only)."),
        ("tolls_amount", "FLOAT64", "Total tolls paid in USD."),
        (
            "total_amount",
            "FLOAT64",
            "Total amount charged to the passenger in USD, excluding cash tips.",
        ),
        ("congestion_surcharge", "FLOAT64", "Congestion surcharge in USD."),
        ("airport_fee", "FLOAT64", "Airport pickup fee in USD."),
        ("temperature_2m", "FLOAT64", "Air temperature at 2 m in Celsius during the pickup hour."),
        ("rain", "FLOAT64", "Rainfall in mm during the pickup hour."),
        ("snowfall", "FLOAT64", "Snowfall in cm during the pickup hour."),
        ("wind_speed_10m", "FLOAT64", "Wind speed at 10 m during the pickup hour."),
        ("is_rainy", "BOOL", "True when it rained during the pickup hour."),
        ("source_month", "DATE", "First day of the source file month the trip was loaded from."),
    ],
    "dim_zone": [
        ("location_id", "INT64", "TLC taxi zone id."),
        ("borough", "STRING", "Borough name."),
        ("zone", "STRING", "Zone name."),
        ("service_zone", "STRING", "TLC service zone."),
        ("is_airport", "BOOL", "True for airport zones (name contains Airport, or ids 1, 132, 138)."),
    ],
    "dim_date": [
        ("date_day", "DATE", "Calendar date."),
        ("year", "INT64", "Calendar year."),
        ("month", "INT64", "Calendar month number, 1 to 12."),
        ("day_of_week", "INT64", "Day of week, 1 = Sunday through 7 = Saturday."),
        ("day_name", "STRING", "Full weekday name."),
        ("is_weekend", "BOOL", "True on Saturday and Sunday."),
        ("is_holiday", "BOOL", "True on US federal holidays."),
        ("holiday_name", "STRING", "Holiday name, NULL on non-holidays."),
    ],
    "agg_hourly_zone_demand": [
        ("pickup_date", "DATE", "Calendar date of the pickup (partition column)."),
        ("pickup_hour", "INT64", "Hour of day of the pickup, 0 to 23."),
        ("pu_location_id", "INT64", "Pickup taxi zone id."),
        ("borough", "STRING", "Borough of the pickup zone."),
        ("zone", "STRING", "Name of the pickup zone."),
        ("trips", "INT64", "Number of trips in the zone and hour."),
        ("avg_fare", "FLOAT64", "Average metered fare in USD."),
        ("avg_duration_min", "FLOAT64", "Average trip duration in minutes."),
        ("avg_tip_pct", "FLOAT64", "Average tip as a percent of the fare."),
        ("is_weekend", "BOOL", "True when pickup_date is a Saturday or Sunday."),
        ("is_holiday", "BOOL", "True when pickup_date is a US federal holiday."),
    ],
    "agg_daily_weather_impact": [
        ("pickup_date", "DATE", "Calendar date of the pickup."),
        ("is_rainy", "BOOL", "True when it rained during the pickup hour."),
        ("trips", "INT64", "Number of trips."),
        ("avg_duration_min", "FLOAT64", "Average trip duration in minutes."),
        ("avg_fare", "FLOAT64", "Average metered fare in USD."),
        ("total_revenue", "FLOAT64", "Sum of total_amount in USD."),
        ("borough", "STRING", "Borough of the pickup zone."),
    ],
}

TABLE_DESCRIPTIONS: dict[str, str] = {
    "fct_trips": (
        "One row per valid taxi trip with hourly weather at pickup, partitioned by pickup_date "
        "(a pickup_date filter is required)."
    ),
    "dim_zone": "NYC taxi zones with borough and airport flag.",
    "dim_date": "Calendar dimension for 2024 with weekend and US holiday flags.",
    "agg_hourly_zone_demand": (
        "Trips, fares and tips per pickup zone and hour, with weekend and holiday flags."
    ),
    "agg_daily_weather_impact": (
        "Daily trip volume, duration, fare and revenue split by rain and pickup borough."
    ),
}


def render_schema(project_id: str) -> str:
    """Render the marts catalog as plain text for the LLM system prompt."""
    sections = []
    for table, columns in MART_SCHEMA.items():
        lines = [f"`{project_id}.marts.{table}`: {TABLE_DESCRIPTIONS[table]}"]
        lines += [f"  - {name} {type_}: {description}" for name, type_, description in columns]
        sections.append("\n".join(lines))
    return "\n\n".join(sections)
