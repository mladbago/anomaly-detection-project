# Build context = project root (see docker-compose.yml).
FROM python:3.11-slim

WORKDIR /app
ENV PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

COPY dashboard/requirements.txt ./dashboard/requirements.txt
RUN pip install --no-cache-dir -r dashboard/requirements.txt

COPY common ./common
COPY dashboard ./dashboard

EXPOSE 8501

# --server.address 0.0.0.0 so the UI is reachable from the host; headless mode
# stops Streamlit from trying to open a browser inside the container.
CMD ["streamlit", "run", "dashboard/app.py", \
     "--server.port=8501", "--server.address=0.0.0.0", \
     "--server.headless=true", "--browser.gatherUsageStats=false"]
