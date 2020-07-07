FROM python:3.8-slim-buster

COPY requirements.txt /tmp/

RUN pip install -r /tmp/requirements.txt

RUN useradd --create-home user

USER user

WORKDIR /home/user

RUN chown -R user ./

COPY /src/ .

ENTRYPOINT [ "python" ]
