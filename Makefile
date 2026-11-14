.PHONY: venv venv-airflow test dbt-parse dbt-build airflow-start chatbot eval cost-compare

MONTH ?= 2024-01-01
DBT_DIR := dbt/nyc_mobility

venv:
	uv venv --python 3.12 .venv
	uv pip install --python .venv/bin/python -e ".[dev,dbt,chatbot]"

# Optional and heavy: only needed to run the DAG tests against a real Airflow install.
venv-airflow: venv
	uv pip install --python .venv/bin/python -e ".[airflow]"

test:
	.venv/bin/pytest -q

dbt-parse:
	cd $(DBT_DIR) && ../../.venv/bin/dbt deps --profiles-dir . && ../../.venv/bin/dbt parse --profiles-dir .

dbt-build:
	cd $(DBT_DIR) && ../../.venv/bin/dbt deps --profiles-dir . && ../../.venv/bin/dbt seed --profiles-dir . && ../../.venv/bin/dbt build --profiles-dir . --vars '{"run_month_start": "$(MONTH)"}'

airflow-start:
	cd airflow && astro dev start

chatbot:
	.venv/bin/streamlit run chatbot/app.py

eval:
	.venv/bin/python eval/run_eval.py

cost-compare:
	.venv/bin/python eval/partition_cost_comparison.py
