FROM python:3.9

WORKDIR /usr/src/app

RUN apt-get update && apt-get install -y \
    libvips \
    libvips-tools

COPY requirements.txt .

RUN pip install -r requirements.txt

COPY imaginerio-etl ./imaginerio-etl
COPY data ./data

ENTRYPOINT ["python"]

CMD ["-m", "imaginerio-etl.scripts.update"]
