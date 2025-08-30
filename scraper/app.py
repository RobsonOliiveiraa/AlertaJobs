import os
import re
import time
import hashlib
from datetime import datetime, timedelta
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

import requests
from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options

# ============================ Config ============================
KEYWORDS = os.getenv("KEYWORDS", "").strip()
LOCATION = os.getenv("LOCATION", "Brasil").strip()
GEOID = os.getenv("GEOID", "106057199")            # Brasil
F_TPR = os.getenv("F_TPR", "r86400")               # últimas 24h
SCRAPE_SOURCES = os.getenv("SCRAPE_SOURCES", "linkedin,infojobs").lower()

# Agendamento (horas do dia). Para rodar 1x e sair, use RUN_ONCE=1
_sched_env = os.getenv("SCHEDULE_HOURS", "").strip()
SCHEDULE_HOURS = sorted(int(h) for h in re.findall(r"\d+", _sched_env)) if _sched_env else [8, 12, 16, 19]

RUN_ONCE = os.getenv("RUN_ONCE", "0") == "1"
DRY_RUN  = os.getenv("DRY_RUN", "0") == "1"

# InfoJobs
MAX_INFOJOBS_PAGES = int(os.getenv("MAX_INFOJOBS_PAGES", "5"))
INFOJOBS_ENDPOINTS = [
    ("https://www.infojobs.com.br/empregos-em-sao-paulo.aspx?Antiguedad=1", "São Paulo"),
    ("https://www.infojobs.com.br/empregos-em-rio-janeiro.aspx?Antiguedad=1", "Rio de Janeiro"),
]

# Postgres (usado somente se DRY_RUN=0)
DB_HOST = os.getenv("DB_HOST", "postgres")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "n8n")
DB_USER = os.getenv("DB_USER", "n8n")
DB_PASSWORD = os.getenv("DB_PASSWORD", "ChangeThis!123")

# Chromium/Driver (para LinkedIn via Selenium)
CHROME_BINARY = os.getenv("CHROME_BINARY", "/usr/bin/chromium")
CHROMEDRIVER  = os.getenv("CHROMEDRIVER", "/usr/bin/chromedriver")

# HTTP headers
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123 Safari/537.36",
    "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
}

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

TIPOS_VAGA = {"Remoto": "2", "Presencial": "1", "Híbrido": "3"}

# ===================== Helpers de ID/links ======================
LINKEDIN_VIEW_ID = re.compile(r"/jobs/view/(\d+)")
INFOJOBS_ID      = re.compile(r"-([0-9]{6,})\.aspx", re.IGNORECASE)

def md5(s: str) -> str:
    return hashlib.md5(s.encode("utf-8")).hexdigest()

def canonicalize_link(url: str) -> str:
    if not url:
        return ""
    p = urlparse(url)
    return urlunparse((p.scheme, p.netloc, p.path, "", "", ""))

def extract_linkedin_id_from_div(div) -> str | None:
    urn = div.get("data-entity-urn") or div.get("data-id") or ""
    m = re.search(r"jobPosting:(\d+)", urn)
    if m:
        return m.group(1)
    for _, v in div.attrs.items():
        if isinstance(v, str):
            m2 = re.search(r"\b(\d{6,})\b", v)
            if m2:
                return m2.group(1)
    return None

# ============================ Selenium ==========================
def make_driver():
    opts = Options()
    opts.binary_location = CHROME_BINARY
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1280,800")
    opts.add_argument("--disable-software-rasterizer")
    opts.add_argument("--user-agent=" + HEADERS["User-Agent"])
    # evita prompts de geolocalização
    opts.add_experimental_option("prefs", {"profile.default_content_setting_values.geolocation": 2})
    service = Service(executable_path=CHROMEDRIVER)
    return webdriver.Chrome(service=service, options=opts)

def gentle_scroll(driver, times=14, min_sleep=1.0, max_sleep=2.0):
    import random as _r
    for _ in range(times):
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(_r.uniform(min_sleep, max_sleep))
        try:
            close_btn = driver.find_element(By.XPATH, "//button[@aria-label='Fechar' or @aria-label='Close']")
            if close_btn.is_displayed() and close_btn.is_enabled():
                close_btn.click()
                time.sleep(0.3)
        except Exception:
            pass

# ============================ Scrapers ==========================
def scrape_linkedin(driver) -> list[dict]:
    posts = []
    for tipo_nome, tipo_valor in TIPOS_VAGA.items():
        url = (
            "https://www.linkedin.com/jobs/search/?"
            f"keywords={KEYWORDS}&location={LOCATION}&geoId={GEOID}"
            f"&f_TPR={F_TPR}&f_WT={tipo_valor}&position=1&pageNum=0"
        )
        print(f"[LinkedIn] {tipo_nome} → {url}")
        try:
            driver.get(url)
            gentle_scroll(driver, times=15)
            soup = BeautifulSoup(driver.page_source, "html.parser")
            job_divs = soup.find_all("div", class_="job-card-container") or soup.find_all("div", class_="base-card")
            print(f"[LinkedIn] Cards: {len(job_divs)}")

            for div in job_divs:
                title = div.find("h3", class_="base-search-card__title") or div.find("h3", class_="base-card__title")
                company = div.find("h4", class_="base-search-card__company-name") or div.find("h4", class_="base-card__company-name")
                location = div.find("span", class_="job-card-container__location") or div.find("span", class_="job-search-card__location")
                link_tag = div.find("a", class_="base-card__full-link") or div.find("a", class_="result-card__full-card-link")

                link = link_tag["href"].strip() if link_tag and link_tag.has_attr("href") else ""
                title_text = (title.text if title else "").strip()
                company_text = (company.text if company else "").strip()
                location_text = (location.text if location else "").strip()
                loc_lower = location_text.lower()

                if not any(k in loc_lower for k in BR_KEYWORDS):
                    continue

                tipo = tipo_nome
                t_low = title_text.lower()
                if any(x in (t_low + " " + loc_lower) for x in ("remoto", "remote")):
                    tipo = "Remoto"
                elif any(x in (t_low + " " + loc_lower) for x in ("híbrido", "hibrido", "hybrid")):
                    tipo = "Híbrido"

                job_id = None
                if link:
                    m = LINKEDIN_VIEW_ID.search(link)
                    if m:
                        job_id = m.group(1)
                if not job_id:
                    job_id = extract_linkedin_id_from_div(div)
                if not job_id and link:
                    job_id = md5(canonicalize_link(link) + "linkedin")

                posts.append({
                    "job_uid": f"linkedin:{job_id}" if job_id else None,
                    "title": title_text,
                    "company": company_text,
                    "location": location_text,
                    "link": canonicalize_link(link),
                    "tipo_vaga": tipo,
                    "data_publicacao": "",
                    "source": "linkedin",
                })
        except Exception as e:
            print(f"[LinkedIn] Erro: {e}")

    return posts

def add_or_replace_query(url: str, key: str, value: str) -> str:
    p = urlparse(url)
    q = parse_qs(p.query)
    q[key] = [value]
    return urlunparse((p.scheme, p.netloc, p.path, p.params, urlencode(q, doseq=True), p.fragment))

def scrape_infojobs_http() -> list[dict]:
    """Paginação por ?Page=1..N via requests (sem Selenium)."""
    posts = []
    for base_url, estado in INFOJOBS_ENDPOINTS:
        total_estado = 0
        seen_ids = set()
        for page in range(1, MAX_INFOJOBS_PAGES + 1):
            url = add_or_replace_query(base_url, "Page", str(page))
            try:
                r = requests.get(url, headers=HEADERS, timeout=25)
                if r.status_code != 200:
                    print(f"[InfoJobs] {estado} p{page} HTTP {r.status_code} – parando.")
                    break
                soup = BeautifulSoup(r.text, "lxml")
                anchors = soup.select("a[href^='/vaga-de-']")

                page_count = 0
                for a in anchors:
                    href = a.get("href", "")
                    m = INFOJOBS_ID.search(href)
                    if not m:
                        continue
                    ij_id = m.group(1)
                    if ij_id in seen_ids:
                        continue
                    seen_ids.add(ij_id)

                    title = a.get_text(strip=True)
                    link_full = "https://www.infojobs.com.br" + href
                    posts.append({
                        "job_uid": f"infojobs:{ij_id}",
                        "title": title,
                        "company": "",
                        "location": estado,
                        "link": canonicalize_link(link_full),
                        "tipo_vaga": "",
                        "data_publicacao": "",
                        "source": "infojobs",
                    })
                    page_count += 1

                print(f"[InfoJobs] {estado} p{page}: +{page_count} vagas")
                total_estado += page_count
                if page_count == 0:
                    break
                time.sleep(0.7)
            except Exception as e:
                print(f"[InfoJobs] {estado} p{page} erro: {e}")
                break

        print(f"[InfoJobs] {estado}: total +{total_estado} vagas (até Page={page})")
    return posts

# ============================ Persistência ======================
DDL_VAGAS = """
CREATE TABLE IF NOT EXISTS vagas (
  id BIGSERIAL PRIMARY KEY,
  job_uid TEXT UNIQUE,
  title TEXT,
  company TEXT,
  location TEXT,
  link TEXT,
  tipo_vaga TEXT,
  data_publicacao TEXT,
  source TEXT,
  created_at TIMESTAMPTZ DEFAULT now(),
  updated_at TIMESTAMPTZ DEFAULT now()
);
"""

ALTERS = [
    "ALTER TABLE vagas ADD COLUMN IF NOT EXISTS job_uid TEXT;",
    "ALTER TABLE vagas ADD COLUMN IF NOT EXISTS link TEXT;",
    "ALTER TABLE vagas ADD COLUMN IF NOT EXISTS source TEXT;",
    "ALTER TABLE vagas ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT now();",
    "CREATE UNIQUE INDEX IF NOT EXISTS uniq_vagas_job_uid ON vagas(job_uid);",
]

INSERT_VAGAS = """
INSERT INTO vagas (job_uid, title, company, location, link, tipo_vaga, data_publicacao, source, updated_at)
VALUES %s
ON CONFLICT (job_uid) DO UPDATE SET
  title = EXCLUDED.title,
  company = EXCLUDED.company,
  location = EXCLUDED.location,
  link = EXCLUDED.link,
  tipo_vaga = EXCLUDED.tipo_vaga,
  data_publicacao = EXCLUDED.data_publicacao,
  source = EXCLUDED.source,
  updated_at = now();
"""

def ensure_schema(cur):
    cur.execute(DDL_VAGAS)
    for stmt in ALTERS:
        cur.execute(stmt)

def save_to_postgres(rows: list[dict]):
    import psycopg2
    from psycopg2.extras import execute_values
    if not rows:
        print("[DB] Nada para inserir.")
        return
    try:
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
        )
        cur = conn.cursor()
        ensure_schema(cur)

        values = [
            (
                r.get("job_uid"),
                r.get("title", ""),
                r.get("company", ""),
                r.get("location", ""),
                r.get("link", ""),
                r.get("tipo_vaga", ""),
                r.get("data_publicacao", ""),
                r.get("source", ""),
                datetime.utcnow(),
            )
            for r in rows if r.get("job_uid")
        ]
        if values:
            execute_values(cur, INSERT_VAGAS, values, page_size=500)
            conn.commit()
            print(f"[DB] Upsert OK: {len(values)} registros.")

        cur.close(); conn.close()
    except Exception as e:
        print(f"[DB] Erro ao salvar: {e}")

# ============================== Run =============================
def run_once():
    all_posts = []
    li_count = ij_count = 0

    # LinkedIn (Selenium)
    driver = None
    try:
        if "linkedin" in SCRAPE_SOURCES:
            driver = make_driver()
            print("[BOOT] Chromium (LinkedIn) iniciado.")
            li_posts = scrape_linkedin(driver)
            li_count = len(li_posts)
            all_posts.extend(li_posts)
    finally:
        if driver:
            try: driver.quit()
            except: pass

    # InfoJobs (HTTP paginado)
    if "infojobs" in SCRAPE_SOURCES:
        ij_posts = scrape_infojobs_http()
        ij_count = len(ij_posts)
        all_posts.extend(ij_posts)

    # Dedup por job_uid (e fallback por link+source)
    dedup = {}
    for p in all_posts:
        uid = p.get("job_uid") or md5(p.get("link", "") + p.get("source", ""))
        p["job_uid"] = uid
        dedup[uid] = p
    final = list(dedup.values())

    print(f"[COUNT] LinkedIn: {li_count} | InfoJobs: {ij_count}")
    print(f"[TOTAL] Coletadas {len(final)} vagas após deduplicação.")

    if DRY_RUN:
        print(f"[DRY_RUN] Não gravado no DB. Registros a salvar: {len(final)}")
    else:
        save_to_postgres(final)

def next_run_msg():
    now = datetime.now()
    today_hours = [now.replace(hour=h, minute=0, second=0, microsecond=0) for h in SCHEDULE_HOURS]
    fut = [t for t in today_hours if t > now]
    if not fut:
        target = (now + timedelta(days=1)).replace(hour=SCHEDULE_HOURS[0], minute=0, second=0, microsecond=0)
    else:
        target = fut[0]
    delta = int((target - now).total_seconds())
    return target, delta

if __name__ == "__main__":
    print("== AlertaJobs Scraper ==")
    print(f"Sources: {SCRAPE_SOURCES}")
    print(f"SCHEDULE | DRY_RUN={1 if DRY_RUN else 0}")
    print(f"Keywords: '{KEYWORDS}' | Location: '{LOCATION}'")

    if RUN_ONCE:
        run_once()
    else:
        while True:
            run_once()
            target, delta = next_run_msg()
            print(f"[SLEEP] Próxima execução {target.strftime('%d/%m %H:%M')} (em {delta}s)…")
            time.sleep(max(delta, 10))
