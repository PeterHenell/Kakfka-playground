FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY parquet_consumer.py .
# -u: unbuffered output so prints show up in `docker compose logs` immediately
CMD ["python", "-u", "parquet_consumer.py"]
