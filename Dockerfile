FROM python:3.6-alpine

RUN apk update && apk upgrade && \
    apk add --no-cache bash git openssh build-base

ENV DOCKERIZE_VERSION v0.6.1
RUN wget https://github.com/jwilder/dockerize/releases/download/$DOCKERIZE_VERSION/dockerize-alpine-linux-amd64-$DOCKERIZE_VERSION.tar.gz \
    && tar -C /usr/local/bin -xzvf dockerize-alpine-linux-amd64-$DOCKERIZE_VERSION.tar.gz \
    && rm dockerize-alpine-linux-amd64-$DOCKERIZE_VERSION.tar.gz

COPY requirements.txt /staticord/requirements.txt
RUN pip install -r /staticord/requirements.txt

RUN mkdir /staticord/logs

COPY bot /staticord/bot
WORKDIR /staticord/bot

CMD dockerize -wait tcp://postgres:5432 python /staticord/bot/launcher.py