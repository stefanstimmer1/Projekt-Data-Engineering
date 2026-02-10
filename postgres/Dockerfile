FROM python:3.12-slim

RUN pip install streamlit psycopg2-binary pandas

COPY dashboard.py .

EXPOSE 8501

CMD ["streamlit", "run", "dashboard.py", "--server.address=0.0.0.0"]
