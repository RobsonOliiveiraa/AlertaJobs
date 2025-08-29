# Sobrescreve o Dockerfile do scraper com o COPY correto
cat > ~/AlertaJobs/scraper/Dockerfile <<'EOF'
FROM python:3.11-slim
ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
    chromium \
    chromium-driver \
    fonts-liberation \
    libnss3 \
    libasound2 \
    libxss1 \
    libappindicator3-1 \
    libatk-bridge2.0-0 \
    libgbm1 \
    libu2f-udev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

RUN pip install --no-cache-dir \
    selenium \
    webdriver-manager \
    psycopg2-binary \
    requests \
    beautifulsoup4 \
    lxml \
    pandas

# <- AQUI está a correção: arquivo está em scraper/app.py dentro do contexto raiz
COPY scraper/app.py /app/

ENV CHROME_BINARY=/usr/bin/chromium
ENV CHROMEDRIVER=/usr/bin/chromedriver

CMD ["python", "/app/app.py"]
EOF

# Reconstrói só o scraper e sobe tudo
cd ~/AlertaJobs
docker compose down --remove-orphans
docker compose build --no-cache scraper
docker compose up -d

# Verifique
docker compose ps
docker compose logs -f scraper
