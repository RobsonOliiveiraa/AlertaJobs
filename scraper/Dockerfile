cd ~/AlertaJobs

# cria o Dockerfile dentro da pasta scraper
cat > scraper/Dockerfile <<'EOF'
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Chromium + chromedriver + libs para Selenium
RUN apt-get update && apt-get install -y --no-install-recommends \
    chromium chromium-driver ca-certificates \
    libglib2.0-0 libnss3 libgconf-2-4 libfontconfig1 libxi6 libxrender1 \
    libxrandr2 libdbus-1-3 libxtst6 libxss1 xvfb fonts-liberation curl && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Instale as libs Python que seu script usa
RUN pip install --no-cache-dir selenium psycopg2-binary pandas requests beautifulsoup4 schedule

# Copia apenas a pasta do scraper para a imagem
COPY scraper/ /app/

CMD ["python", "/app/app.py"]
EOF
