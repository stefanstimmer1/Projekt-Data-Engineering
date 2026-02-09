
FROM python:3.12-slim

RUN pip install confluent-kafka
COPY producer_script.py .

CMD ["python", "producer_script.py"]
