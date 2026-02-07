FROM python:3.12-slim

WORKDIR /app

# Install system deps for Playwright
RUN apt-get update && apt-get install -y \
    wget gnupg2 libnss3 libatk-bridge2.0-0 libdrm2 \
    libxcomposite1 libxdamage1 libxrandr2 libgbm1 \
    libpango-1.0-0 libcairo2 libasound2 libxshmfence1 \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install Playwright browsers
RUN playwright install chromium --with-deps

# Copy application code
COPY app/ ./app/

ENV PORT=8080
EXPOSE 8080

# Run the combined worker + dashboard
CMD ["python", "-m", "app"]
