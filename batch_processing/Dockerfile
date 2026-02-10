FROM python:3.12-slim

RUN pip install confluent-kafka

COPY batch_processing.py .

CMD ["python", "-u", "batch_processing.py"]
