FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Install tzdata so the container resolves IANA timezone names (e.g. Europe/Madrid)
# and curl for the HEALTHCHECK probe.  Both layers are merged to minimise image size.
RUN apt-get update \
    && apt-get install -y --no-install-recommends tzdata curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

HEALTHCHECK --interval=30s --timeout=10s --start-period=15s --retries=3 \
    CMD curl -f http://localhost:${API_PORT:-8765}/health || exit 1

CMD ["python", "main.py"]