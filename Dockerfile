# Dockerfile
# Starts from the official Airflow image and adds the Snowflake add-ons.
# Docker uses this file to build your custom Airflow image.

FROM apache/airflow:3.2.2

# Install the extra provider packages your pipeline needs.
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
