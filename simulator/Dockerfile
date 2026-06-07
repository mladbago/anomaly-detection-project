# Build context = project root (see docker-compose.yml).
FROM python:3.11-slim

WORKDIR /app
ENV PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

COPY simulator/requirements.txt ./simulator/requirements.txt
RUN pip install --no-cache-dir -r simulator/requirements.txt

COPY common ./common
COPY simulator ./simulator

CMD ["python", "-m", "simulator.producer"]
