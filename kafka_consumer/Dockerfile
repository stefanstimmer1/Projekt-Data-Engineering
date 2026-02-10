
FROM python:3.12-slim

RUN pip install confluent-kafka

COPY raw_consumer_script.py .

CMD ["python", "-u", "raw_consumer_script.py"]