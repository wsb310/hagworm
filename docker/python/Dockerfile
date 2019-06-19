FROM python:3.7

MAINTAINER wsb <wsb310@gmail.com>

ENV TZ="Asia/Shanghai"

RUN pip install --upgrade pip

COPY requirements.txt /tmp/

RUN pip install -r /tmp/requirements.txt
