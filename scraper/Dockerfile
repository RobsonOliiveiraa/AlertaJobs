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

# copia todo o diretório scraper/ do repo para dentro da imagem
COPY scraper/ /app/

ENV CHROME_BINARY=/usr/bin/chromium
ENV CHROMEDRIVER=/usr/bin/chromedriver

CMD ["python", "/app/app.py"]
