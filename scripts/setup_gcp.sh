#!/usr/bin/env bash
# Idempotent BigQuery dataset setup. Set CREATE_SA=1 to also create the service account.
set -euo pipefail

: "${GCP_PROJECT_ID:?GCP_PROJECT_ID must be set}"
BQ_LOCATION="${BQ_LOCATION:-US}"
SA_NAME="nyc-mobility-sa"
SA_EMAIL="${SA_NAME}@${GCP_PROJECT_ID}.iam.gserviceaccount.com"

for ds in raw_tlc raw_weather staging marts quarantine; do
  echo "Ensuring dataset ${GCP_PROJECT_ID}:${ds} in ${BQ_LOCATION}"
  bq --location="${BQ_LOCATION}" mk --dataset --force "${GCP_PROJECT_ID}:${ds}"
done

echo
echo "Service account setup commands:"
cat <<CMDS
  gcloud iam service-accounts create ${SA_NAME} --project=${GCP_PROJECT_ID}
  gcloud projects add-iam-policy-binding ${GCP_PROJECT_ID} --member=serviceAccount:${SA_EMAIL} --role=roles/bigquery.dataEditor
  gcloud projects add-iam-policy-binding ${GCP_PROJECT_ID} --member=serviceAccount:${SA_EMAIL} --role=roles/bigquery.jobUser
  gcloud iam service-accounts keys create gcp-key.json --iam-account=${SA_EMAIL}
CMDS

if [[ "${CREATE_SA:-0}" == "1" ]]; then
  gcloud iam service-accounts create "${SA_NAME}" --project="${GCP_PROJECT_ID}" || echo "Service account already exists"
  for role in roles/bigquery.dataEditor roles/bigquery.jobUser; do
    gcloud projects add-iam-policy-binding "${GCP_PROJECT_ID}" \
      --member="serviceAccount:${SA_EMAIL}" --role="${role}"
  done
  gcloud iam service-accounts keys create gcp-key.json --iam-account="${SA_EMAIL}"
fi
