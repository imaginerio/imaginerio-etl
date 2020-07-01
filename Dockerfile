FROM python:3.8-slim-buster

RUN pip install pipenv

COPY Pipfile* /tmp/

RUN cd /tmp/ && pipenv lock --requirements > requirements.txt \
    && pip install -r /tmp/requirements.txt

RUN useradd --create-home user

USER user

WORKDIR /home/user

RUN chown -R user ./

COPY /src/ .

ENTRYPOINT [ "python" ]
