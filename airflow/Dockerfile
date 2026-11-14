# Astro Runtime 3.3-12 ships Airflow 3.3.2 (newest 3.x Runtime on 2026-11-14; no
# Runtime with Airflow 3.4 exists yet). The python-3.12 variant matches the local
# test venv. The DAG keeps fallbacks for Task SDK APIs that differ between 3.3 and 3.4.
FROM astrocrpublic.azurecr.io/runtime:3.3-12-python-3.12

# dbt project and chatbot folder are bind-mounted by docker-compose.override.yml.
ENV DBT_PROJECT_DIR=/usr/local/airflow/include/dbt \
    GOOGLE_APPLICATION_CREDENTIALS=/usr/local/airflow/include/gcp-key.json \
    CHATBOT_METADATA_PATH=/usr/local/airflow/include/chatbot/metadata.json
