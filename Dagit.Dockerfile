'FROM python:3.8-slim'

RUN mkdir -p /opt/dagster/dagster_home /opt/dagster/app

COPY requirements.txt /tmp/

RUN pip install -r /tmp/requirements.txt

# Copy your pipeline code and workspace to /opt/dagster/app
COPY /src/ /opt/dagster/app/

ENV DAGSTER_HOME=/opt/dagster/dagster_home/

# Copy dagster instance YAML to $DAGSTER_HOME
COPY dagster.yaml /opt/dagster/dagster_home/

WORKDIR /opt/dagster/app

EXPOSE 3000

ENTRYPOINT ["dagit", "-w", "workspace_docker.yaml", "-h", "0.0.0.0", "-p", "3000"]