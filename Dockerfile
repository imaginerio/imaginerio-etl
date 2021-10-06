FROM openjdk:11
COPY --from=python:3.8-slim / /

# Create a project folder
RUN mkdir /dagster

# Change working dirtory
WORKDIR /dagster

# ENV DAGSTER_HOME=/dagster/.dagster

RUN apt-get update && apt-get install -y git

COPY requirements.txt /dagster/

RUN pip install -r requirements.txt

COPY  . /dagster/

WORKDIR /dagster/src

# For debugging purposes
# RUN pwd
# RUN ls -la

# By default, dagit listens on port 3000, so we need to expose it
EXPOSE 10000

# Launch dagit, but we need to use workspace_docker.yaml
# instead of the default workspace.yaml since we are using the python
# executable from the container image, not the local Python executable
#ENTRYPOINT ["dagit", "-w", "workspace.yaml", "-h", "0.0.0.0", "-p", "10000"]