# Astro Runtime 3.3 (Apache Airflow 3.x) — generated layout of `astro dev init`
FROM astrocrpublic.azurecr.io/runtime:3.3-12

# ---------------------------------------------------------------------------
# Image metadata (OCI standard labels + project-specific labels)
# ---------------------------------------------------------------------------
LABEL maintainer="24L-0672" \
      org.opencontainers.image.title="currency-etl-airflow" \
      org.opencontainers.image.description="Airflow (Astro CLI) ETL pipeline: Frankfurter API daily exchange rates -> transform -> MongoDB" \
      org.opencontainers.image.version="1.0.0" \
      org.opencontainers.image.authors="24L-0672" \
      org.opencontainers.image.licenses="MIT" \
      org.opencontainers.image.base.name="astrocrpublic.azurecr.io/runtime:3.3-12" \
      com.mlops.project="currency-exchange-etl" \
      com.mlops.component="airflow" \
      com.mlops.course="MLOps" \
      com.mlops.task="Task 3 - Building Airflow ETL Pipeline"
