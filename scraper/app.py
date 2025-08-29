import os
import time
import random
import sys
from datetime import datetime

import pandas as pd
from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options

import psycopg2
from psycopg2.extras import execute_values


# =============================================================================
# Configurações via ENV (com defaults sensatos)
# =============================================================================
KEYWORDS = os.getenv("KEYWORDS", "").strip()
LOCATION = os.getenv("LOCATION", "Brasil").strip()
# GeoID do Brasil no LinkedIn (padrão)
GEOID = os.getenv("GEOID", "106057199")
# Últimas 24h
F_TPR = os.getenv("F_TPR", "r86400")
# Fontes a coletar: "linkedin,infojobs"
SCRAPE_SOURCES = os.getenv("SCRAPE_SOURCES", "linkedin,infojobs").lower()
# Intervalo (minutos) entre ciclos
SCRAPE_INTERVAL_MIN = int(os.getenv("SCRAPE_INTERVAL_MIN", "60"))

# Postgres: use as mesmas keys do docker-compose.yml
DB_HOST = os.getenv("DB_HOST", "postgres")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "n8n")
DB_USER = os.getenv("DB_USER", "n8n")
DB_PASSWORD = os.getenv("DB_PASSWORD", "ChangeThis!123")

# Chromium/Driver dentro do container
CHROME_BINARY = os.getenv("CHROME_BINARY", "/usr/bin/chromium")
CHROMEDRIVER = os.getenv("CHROMEDRIVER", "/usr/bin/chromedriver")


# Palavras-chave para considerar vagas do Brasil (filtro por localização)
BR_KEYWORDS = {
    "brasil", "brazil", "br",
    "sp", "são paulo", "rio de janeiro", "rj", "minas gerais", "mg",
    "bahia", "ba", "paraná", "pr", "pernambuco", "pe", "ceará", "ce",
    "goiás", "go", "distrito federal", "df", "porto alegre", "rs",
    "recife", "fortaleza", "curitiba", "salvador", "manaus", "belém",
    "campinas", "florianópolis", "joinville", "natal", "maceió",
    "aracaju", "teresina", "palmas", "campo grande", "cuiabá",
    "vitória", "santos", "sorocaba", "ribeirão preto", "londrina",
    "maringá", "uberlândia", "juiz de fora", "blumenau", "cascavel",
}

# Map de tipo de vaga para filtro do LinkedIn
TIPOS_VAGA = {"Remoto": "2", "Presencial": "1", "Híbrido": "3"}


# =============================================================================
# Selenium / Chromium
# =============================================================================
def make_driver():
    """Inicializa o Chromium headless com flags adequadas para Docker."""
    opts = Options()
    opts.binary_location = CHROME_BINARY
    # Flags essenciais no Docker
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1280,720")
    opts.add_argument("--disable-software-rasterizer")

    # User-Agent estático (pode rotacionar se quiser)
    ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
    opts.add_argument(f"--user-agent={ua}")

    service = Service(executable_path=CHROMEDRIVER)
    driver = webdriver.Chrome(service=service, options=opts)
    return driver


def gentle_scroll(driver, times=12, min_sleep=1.5, max_sleep=3.0):
    for _ in range(times):
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(random.uniform(min_sleep, max_sleep))
        # tenta fechar modal de login, se aparecer
        try:
            close_btn = driver.find_element(By.XPATH, "//button[@aria-label='Fechar' or @aria-label='Close']")
            if close_btn.is_displayed() and close_btn.is_enabled():
                close_btn.click()
                time.sleep(0.5)
        except Exception:
            pass


# =============================================================================
# Scrapers
# =============================================================================
def scrape_linkedin(driver) -> list:
    """Coleta vagas do LinkedIn (sem login) para Brasil e últimos 1 dia."""
    posts = []
    for tipo_nome, tipo_valor in TIPOS_VAGA.items():
        url = (
            "https://www.linkedin.com/jobs/search/?"
            f"keywords={KEYWORDS}&location={LOCATION}&geoId={GEOID}"
            f"&f_TPR={F_TPR}&f_WT={tipo_valor}&position=1&pageNum=0"
        )
        print(f"[LinkedIn] Buscando vagas tipo {tipo_nome}: {url}", flush=True)
        try:
            driver.get(url)
            gentle_scroll(driver, times=15, min_sleep=1.2, max_sleep=2.2)

            soup = BeautifulSoup(driver.page_source, "html.parser")
            job_divs = soup.find_all("div", class_="job-card-container")
            if not job_divs:
                job_divs = soup.find_all("div", class_="base-card")

            print(f"[LinkedIn] Cards encontrados: {len(job_divs)}", flush=True)

            for div in job_divs:
                title = div.find("h3", class_="base-search-card__title") or div.find(
                    "h3", class_="base-card__title"
                )
                company = div.find("h4", class_="base-search-card__company-name") or div.find(
                    "h4", class_="base-card__company-name"
                )
                location = div.find("span", class_="job-card-container__location") or div.find(
                    "span", class_="job-search-card__location"
                )
                link_tag = div.find("a", class_="base-card__full-link") or div.find(
                    "a", class_="result-card__full-card-link"
                )
                link = link_tag["href"].strip() if link_tag and link_tag.has_attr("href") else ""
                date_tag = div.find("time")
                date_posted = date_tag.text.strip().lower() if date_tag else ""

                loc_text = (location.text if location else "").strip().lower()
                # filtra por Brasil
                if not any(k in loc_text for k in BR_KEYWORDS):
                    continue

                # tenta inferir tipo
                tipo_vaga = tipo_nome
                title_text = (title.text if title else "").lower()
                if any(x in title_text for x in ["remoto", "remote"]) or any(
                    x in loc_text for x in ["remoto", "remote"]
                ):
                    tipo_vaga = "Remoto"
                elif any(x in title_text for x in ["híbrido", "hybrid"]) or any(
                    x in loc_text for x in ["híbrido", "hybrid"]
                ):
                    tipo_vaga = "Híbrido"

                posts.append(
                    {
                        "title": title.text.strip() if title else "",
                        "company": company.text.strip() if company else "",
                        "location": location.text.strip() if location else "",
                        "link": link,
                        "tipo_vaga": tipo_vaga,
                        "data_publicacao": date_posted,
                        "source": "linkedin",
                    }
                )
        except Exception as e:
            print(f"[LinkedIn] Erro: {e}", flush=True)

    return posts


def scrape_infojobs(driver) -> list:
    """Coleta vagas do InfoJobs (sem login) – rotas/estados fixos como exemplo."""
    posts = []
    infojobs_links = [
        ("https://www.infojobs.com.br/empregos-em-sao-paulo.aspx?Antiguedad=1", "São Paulo"),
        ("https://www.infojobs.com.br/empregos-em-rio-janeiro.aspx?Antiguedad=1", "Rio de Janeiro"),
    ]

    for url, estado in infojobs_links:
        print(f"[InfoJobs] Coletando: {url}", flush=True)
        try:
            driver.get(url)
            gentle_scroll(driver, times=15, min_sleep=1.0, max_sleep=2.0)

            soup = BeautifulSoup(driver.page_source, "html.parser")

            # o layout muda com frequência; pegamos tudo que pareça card com link de vaga
            for a in soup.select("a[href^='/vaga-de-']"):
                title = a.get_text(strip=True)
                link = "https://www.infojobs.com.br" + a.get("href", "")

                # procura blocos ao redor para extrair empresa/tipo/local
                card = a.find_parent(["div", "li"])
                company = ""
                location = estado
                tipo_vaga = ""
                if card:
                    txt = card.get_text(" ", strip=True).lower()
                    # heurística simples:
                    if "presencial" in txt:
                        tipo_vaga = "Presencial"
                    elif "home office" in txt or "remoto" in txt:
                        tipo_vaga = "Remoto"
                    elif "híbrido" in txt or "hibrido" in txt:
                        tipo_vaga = "Híbrido"
                    # empresa e localização (muito dependente do HTML atual)
                    # deixamos com estado se não encontrar
                posts.append(
                    {
                        "title": title,
                        "company": company,
                        "location": location,
                        "link": link,
                        "tipo_vaga": tipo_vaga,
                        "data_publicacao": "hoje",
                        "source": "infojobs",
                    }
                )
        except Exception as e:
            print(f"[InfoJobs] Erro: {e}", flush=True)

    return posts


# =============================================================================
# Persistência no Postgres
# =============================================================================
DDL_VAGAS = """
CREATE TABLE IF NOT EXISTS vagas (
    id BIGSERIAL PRIMARY KEY,
    title TEXT,
    company TEXT,
    location TEXT,
    link TEXT UNIQUE,
    tipo_vaga TEXT,
    data_publicacao TEXT,
    source TEXT,
    created_at TIMESTAMPTZ DEFAULT now()
);
"""

INSERT_VAGAS = """
INSERT INTO vagas (title, company, location, link, tipo_vaga, data_publicacao, source)
VALUES %s
ON CONFLICT (link) DO UPDATE SET
  title = EXCLUDED.title,
  company = EXCLUDED.company,
  location = EXCLUDED.location,
  tipo_vaga = EXCLUDED.tipo_vaga,
  data_publicacao = EXCLUDED.data_publicacao,
  source = EXCLUDED.source;
"""


def save_to_postgres(rows: list):
    if not rows:
        print("[DB] Nada para inserir.", flush=True)
        return
    try:
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
        )
        cur = conn.cursor()
        cur.execute(DDL_VAGAS)

        values = [
            (
                r.get("title", ""),
                r.get("company", ""),
                r.get("location", ""),
                r.get("link", ""),
                r.get("tipo_vaga", ""),
                r.get("data_publicacao", ""),
                r.get("source", ""),
            )
            for r in rows
            if r.get("link")
        ]

        if values:
            execute_values(cur, INSERT_VAGAS, values, page_size=500)
            conn.commit()
            print(f"[DB] Upsert OK: {len(values)} registros.", flush=True)

        cur.close()
        conn.close()
    except Exception as e:
        print(f"[DB] Erro ao salvar: {e}", flush=True)


# =============================================================================
# Main loop
# =============================================================================
def run_once():
    driver = None
    all_posts = []
    try:
        driver = make_driver()
        print("[BOOT] Chromium iniciado.", flush=True)

        if "linkedin" in SCRAPE_SOURCES:
            all_posts.extend(scrape_linkedin(driver))
        if "infojobs" in SCRAPE_SOURCES:
            all_posts.extend(scrape_infojobs(driver))

        # Dedup por link
        dedup = {}
        for p in all_posts:
            if p.get("link"):
                dedup[p["link"]] = p
        final = list(dedup.values())

        print(f"[TOTAL] Coletadas {len(final)} vagas após deduplicação.", flush=True)
        save_to_postgres(final)
    except Exception as e:
        print(f"[RUN] Erro inesperado: {e}", flush=True)
    finally:
        try:
            if driver:
                driver.quit()
        except Exception:
            pass


if __name__ == "__main__":
    print(
        f"== AlertaJobs Scraper ==\n"
        f"Sources: {SCRAPE_SOURCES} | Intervalo: {SCRAPE_INTERVAL_MIN} min\n"
        f"Keywords: '{KEYWORDS}' | Location: '{LOCATION}'\n"
        f"DB: {DB_USER}@{DB_HOST}:{DB_PORT}/{DB_NAME}",
        flush=True,
    )
    while True:
        start = datetime.now()
        run_once()
        elapsed = (datetime.now() - start).seconds
        sleep_s = max(10, SCRAPE_INTERVAL_MIN * 60 - elapsed)
        print(f"[SLEEP] Aguardando {sleep_s}s até próxima coleta...", flush=True)
        time.sleep(sleep_s)
