
FROM python:3.12-slim

RUN pip install confluent-kafka

COPY producer_script.py .
COPY ./data/weather_data.csv /data/weather_data.csv

CMD ["python", "-u", "producer_script.py"]
