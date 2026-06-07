# Build context = project root (see docker-compose.yml).
FROM python:3.11-slim

WORKDIR /app
ENV PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

COPY alert_manager/requirements.txt ./alert_manager/requirements.txt
RUN pip install --no-cache-dir -r alert_manager/requirements.txt

COPY common ./common
COPY alert_manager ./alert_manager

CMD ["python", "-m", "alert_manager.notifier"]
