FROM python:3.12-slim

WORKDIR /app

# Install system deps for Playwright (separate layer for caching)
RUN apt-get update && apt-get install -y --no-install-recommends \
    wget gnupg2 curl libnss3 libatk-bridge2.0-0 libdrm2 \
    libxcomposite1 libxdamage1 libxrandr2 libgbm1 \
    libpango-1.0-0 libcairo2 libasound2 libxshmfence1 \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies first (layer caching — only rebuilds when requirements.txt changes)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install Playwright browsers in its own layer (slow step, cached separately)
RUN playwright install chromium --with-deps

# Copy application code last (changes most frequently)
COPY app/ ./app/

ENV PORT=8080
EXPOSE 8080

# Docker-level health check (backup for Railway's HTTP health check)
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1

# Run the combined worker + dashboard
CMD ["python", "-m", "app"]
