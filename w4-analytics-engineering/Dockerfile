FROM google/cloud-sdk:latest

# Install dependencies
RUN apt-get update && apt-get install -y python3-pip
RUN pip3 install --break-system-packages dbt-core dbt-bigquery google-cloud-storage pandas pyarrow

WORKDIR /app

# Required for gcloud auth to work during BUILD
COPY gcs.json /app/gcs.json

# Authenticate GCloud
RUN gcloud auth activate-service-account --key-file=/app/gcs.json \
    && gcloud config set project zoomcamp-project-486310

# Tell dbt where to find profiles.yml
ENV DBT_PROFILES_DIR=/app

CMD ["tail", "-f", "/dev/null"]