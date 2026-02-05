FROM python:3.10-slim

WORKDIR /app

RUN pip install --no-cache-dir kafka-python

COPY producer.py .

RUN mkdir -p /data
COPY creditcard.csv /data/creditcard.csv
RUN chmod 644 /data/creditcard.csv

CMD ["python", "-u", "producer.py"]