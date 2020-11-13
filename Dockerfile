FROM python:3.8-slim

RUN apt-get update && apt-get install -y git libpq-dev gcc

WORKDIR /app

COPY . /app

RUN python setup.py install

CMD ["pgdatadiff"]
