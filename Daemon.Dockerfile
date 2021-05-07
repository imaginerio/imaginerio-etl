FROM python:3.8-slim

# Create a project folder
RUN mkdir -p /opt/dagster/dagster_home /opt/dagster/app

# Copy requirements to temporary folder
COPY requirements.txt /tmp/

# Install requiremente
RUN pip install -r /tmp/requirements.txt

# Change working dirtory
WORKDIR /opt/dagster/app

ENV DAGSTER_HOME=/opt/dagster/dagster_home

COPY  /src/ /opt/dagster/app

WORKDIR /opt/dagster/app/src 

# For debugging purposes
# RUN pwd
# RUN ls -la

ENTRYPOINT ["dagster-daemon", "run"]