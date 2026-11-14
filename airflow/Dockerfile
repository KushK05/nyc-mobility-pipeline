FROM astrocrpublic.azurecr.io/runtime:3.0-latest

# dbt project and chatbot folder are bind-mounted by docker-compose.override.yml.
ENV DBT_PROJECT_DIR=/usr/local/airflow/include/dbt \
    GOOGLE_APPLICATION_CREDENTIALS=/usr/local/airflow/include/gcp-key.json \
    CHATBOT_METADATA_PATH=/usr/local/airflow/include/chatbot/metadata.json
