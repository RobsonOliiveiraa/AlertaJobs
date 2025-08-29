# ~/AlertaJobs/scraper/Dockerfile
FROM python:3.11-slim

ENV DEBIAN_FRONTEND=noninteractive

# Dependências do Chromium/ChromeDriver e libs gráficas
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

# Bibliotecas Python que costumam ser usadas no scraper
RUN pip install --no-cache-dir \
    selenium \
    webdriver-manager \
    psycopg2-binary \
    requests \
    beautifulsoup4 \
    lxml \
    pandas

# Copia seu código
COPY app.py /app/

# Dicas para o seu script localizar o binário/driver
ENV CHROME_BINARY=/usr/bin/chromium
ENV CHROMEDRIVER=/usr/bin/chromedriver

# Execute seu scraper
CMD ["python", "/app/app.py"]
