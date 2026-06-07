# Build context = project root (see docker-compose.yml).
FROM python:3.11-slim

WORKDIR /app
ENV PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

COPY test_consumer/requirements.txt ./test_consumer/requirements.txt
RUN pip install --no-cache-dir -r test_consumer/requirements.txt

COPY common ./common
COPY test_consumer ./test_consumer

CMD ["python", "-m", "test_consumer.consumer"]
