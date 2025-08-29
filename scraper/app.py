import os
import re
import time
import random
import hashlib
from datetime import datetime, timedelta
from urllib.parse import urlparse, parse_qs, urlunparse

from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException

import psycopg2
from psycopg2.extras import execute_values

# =============================================================================
# Config
# =============================================================================
KEYWORDS = os.getenv("KEYWORDS", "").strip()
LOCATION = os.getenv("LOCATION", "Brasil").strip()
GEOID = os.getenv("GEOID", "106057199")             # Brasil
F_TPR = os.getenv("F_TPR", "r86400")                # últimas 24h
SCRAPE_SOURCES = os.getenv("SCRAPE_SOURCES", "linkedin,infojobs").lower()

# Agenda: por padrão 08, 12, 16, 19. Pode sobrescrever com SCHEDULE_HOURS="8,12,16,19"
_sched_env = os.getenv("SCHEDULE_HOURS", "").strip()
if _sched_env:
    SCHEDULE_HOURS = sorted(int(h) for h in re.findall(r"\d+", _sched_env))
else:
    SCHEDULE_HOURS = [8, 12, 16, 19]

DB_HOST = os.getenv("DB_HOST", "postgres")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "n8n")
DB_USER = os.getenv("DB_USER", "n8n")
DB_PASSWORD = os.getenv("DB_PASSWORD", "ChangeThis!123")

CHROME_BINARY = os.getenv("CHROME_BINARY", "/usr/bin/chromium")
CHROMEDRIVER = os.getenv("CHROMEDRIVER", "/usr/bin/chromedriver")

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

# =============================================================================
# Helpers de link/ID
# =============================================================================
LINKEDIN_VIEW_ID = re.compile(r"/jobs/view/(\d+)")
INFOJOBS_ID = re.compile(r"-([0-9]{6,})\.aspx", re.IGNORECASE)

def md5(s: str) -> str:
    return hashlib.md5(s.encode("utf-8")).hexdigest()

def canonicalize_link(url: str, source: str) -> str:
    if not url:
        return ""
    p = urlparse(url)
    if "linkedin.com" in p.netloc:
        m = LINKEDIN_VIEW_ID.search(p.path)
        if m:
            return f"https://www.linkedin.com/jobs/view/{m.group(1)}"
        return urlunparse((p.scheme, p.netloc, p.path, "", "", ""))
    if "infojobs.com.br" in p.netloc:
        return urlunparse((p.scheme, p.netloc, p.path, "", "", ""))
    return urlunparse((p.scheme, p.netloc, p.path, "", "", ""))

def extract_job_uid(url: str, source: str) -> str:
    if not url:
        return ""
    canon = canonicalize_link(url, source)
    if source == "linkedin":
        m = LINKEDIN_VIEW_ID.search(canon)
        if m:
            return f"li:{m.group(1)}"
        q = parse_qs(urlparse(url).query)
        if "currentJobId" in q and q["currentJobId"]:
            return f"li:{q['currentJobId'][0]}"
        if "trackingId" in q and q["trackingId"]:
            return f"li:trk:{q['trackingId'][0]}"
        return f"li:{md5(canon)}"
    if source == "infojobs":
        m = INFOJOBS_ID.search(canon)
        if m:
            return f"ij:{m.group(1)}"
        return f"ij:{md5(canon)}"
    return md5(canon)

# =============================================================================
# Selenium / Chromium
# =============================================================================
def make_driver(geo: dict | None = None) -> webdriver.Chrome:
    """Cria Chrome headless e, se 'geo' existir, injeta geolocalização + bloqueio de alerts ANTES de qualquer página."""
    opts = Options()
    opts.binary_location = CHROME_BINARY
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1366,2400")
    opts.add_argument("--lang=pt-BR")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)
    # geolocation: allow (1)
    prefs = {"profile.default_content_setting_values.geolocation": 1}
    opts.add_experimental_option("prefs", prefs)
    ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
    opts.add_argument(f"--user-agent={ua}")

    driver = webdriver.Chrome(service=Service(CHROMEDRIVER), options=opts)

    # Script injetado em TODO novo documento (antes de a página rodar JS)
    if geo:
        origin = geo.get("origin", "https://www.infojobs.com.br")
        lat = float(geo.get("lat", -23.55052))
        lng = float(geo.get("lng", -46.633308))

        inject = f"""
        (() => {{
          const allowed = {{ state: 'granted', onchange: null }};
          const fakePos = {{ coords: {{ latitude: {lat}, longitude: {lng}, accuracy: 100 }} }};
          // Finge que a permissão está concedida
          const origPerms = navigator.permissions && navigator.permissions.query
              ? navigator.permissions.query.bind(navigator.permissions) : null;
          if (navigator.permissions) {{
            navigator.permissions.query = (p) => {{
              if (p && p.name === 'geolocation') return Promise.resolve(allowed);
              return origPerms ? origPerms(p) : Promise.resolve(allowed);
            }};
          }} else {{
            Object.defineProperty(navigator, 'permissions', {{
              value: {{ query: async () => allowed }}
            }});
          }}
          // Geolocalização fake
          const ok = (cb) => setTimeout(() => cb(fakePos), 0);
          if (navigator.geolocation) {{
            navigator.geolocation.getCurrentPosition = (s) => ok(s);
            navigator.geolocation.watchPosition = (s) => (ok(s), Math.floor(Math.random()*10000));
          }} else {{
            Object.defineProperty(navigator, 'geolocation', {{
              value: {{
                getCurrentPosition: (s) => ok(s),
                watchPosition: (s) => (ok(s), Math.floor(Math.random()*10000))
              }}
            }});
          }}
          // Mata alert/confirm/prompt
          window.alert = () => true;
          window.confirm = () => true;
          window.prompt = () => '';
        }})();
        """
        try:
            driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {"source": inject})
            driver.execute_cdp_cmd("Browser.grantPermissions", {
                "origin": origin, "permissions": ["geolocation"]
            })
            driver.execute_cdp_cmd("Emulation.setGeolocationOverride", {
                "latitude": lat, "longitude": lng, "accuracy": 50
            })
        except WebDriverException as e:
            print(f"[WARN] Falha ao injetar perms/geo: {e}", flush=True)

    return driver

def gentle_scroll(driver, times=12, min_sleep=1.0, max_sleep=2.0):
    for _ in range(times):
        driver.execute_script("window.scrollBy(0, 1400);")
        time.sleep(random.uniform(min_sleep, max_sleep))
        try:
            close_btn = driver.find_element(By.XPATH, "//button[@aria-label='Fechar' or @aria-label='Close']")
            if close_btn.is_displayed():
                close_btn.click()
        except Exception:
            pass

# =============================================================================
# Scrapers
# =============================================================================
def scrape_linkedin(driver) -> list[dict]:
    posts = []
    for tipo_nome, tipo_valor in TIPOS_VAGA.items():
        url = (
            "https://www.linkedin.com/jobs/search/?"
            f"keywords={KEYWORDS}&location={LOCATION}&geoId={GEOID}"
            f"&f_TPR={F_TPR}&f_WT={tipo_valor}&position=1&pageNum=0"
        )
        print(f"[LinkedIn] {tipo_nome} → {url}", flush=True)
        try:
            driver.get(url)
            gentle_scroll(driver, times=18, min_sleep=1.1, max_sleep=2.0)

            soup = BeautifulSoup(driver.page_source, "html.parser")
            job_divs = soup.find_all("div", class_="job-card-container") or soup.find_all("div", class_="base-card")
            print(f"[LinkedIn] Cards: {len(job_divs)}", flush=True)

            for div in job_divs:
                title = div.find("h3", class_="base-search-card__title") or div.find("h3", class_="base-card__title")
                company = div.find("h4", class_="base-search-card__company-name") or div.find("h4", class_="base-card__company-name")
                location = div.find("span", class_="job-card-container__location") or div.find("span", class_="job-search-card__location")
                link_tag = div.find("a", class_="base-card__full-link") or div.find("a", class_="result-card__full-card-link")
                link = link_tag["href"].strip() if link_tag and link_tag.has_attr("href") else ""
                date_tag = div.find("time")
                date_posted = date_tag.text.strip().lower() if date_tag else ""

                loc_text = (location.text if location else "").strip().lower()
                if not any(k in loc_text for k in BR_KEYWORDS):
                    continue

                tipo_vaga = tipo_nome
                title_text = (title.text if title else "").lower()
                if any(x in title_text for x in ["remoto", "remote"]) or any(x in loc_text for x in ["remoto", "remote"]):
                    tipo_vaga = "Remoto"
                elif any(x in title_text for x in ["híbrido", "hybrid"]) or any(x in loc_text for x in ["híbrido", "hybrid"]):
                    tipo_vaga = "Híbrido"

                canon = canonicalize_link(link, "linkedin")
                uid = extract_job_uid(link, "linkedin")

                posts.append({
                    "job_uid": uid,
                    "title": title.text.strip() if title else "",
                    "company": company.text.strip() if company else "",
                    "location": location.text.strip() if location else "",
                    "link": link,
                    "canonical_link": canon,
                    "tipo_vaga": tipo_vaga,
                    "data_publicacao": date_posted,
                    "source": "linkedin",
                })
        except Exception as e:
            print(f"[LinkedIn] Erro: {e}", flush=True)
    return posts

def scrape_infojobs() -> list[dict]:
    posts = []
    infojobs_links = [
        ("https://www.infojobs.com.br/empregos-em-sao-paulo.aspx?Antiguedad=1", "São Paulo"),
        ("https://www.infojobs.com.br/empregos-em-rio-janeiro.aspx?Antiguedad=1", "Rio de Janeiro"),
    ]
    drv = make_driver(geo={"origin": "https://www.infojobs.com.br", "lat": -23.55052, "lng": -46.633308})

    for url, estado in infojobs_links:
        print(f"[InfoJobs] {estado} → {url}", flush=True)
        try:
            drv.get(url)

            # se mesmo assim aparecer alerta: aceita e segue
            try:
                WebDriverWait(drv, 2).until(EC.alert_is_present())
                drv.switch_to.alert.accept()
                time.sleep(0.3)
            except TimeoutException:
                pass
            except Exception:
                # se não conseguir aceitar, tenta dismiss
                try:
                    drv.switch_to.alert.dismiss()
                except Exception:
                    pass

            # aceitar cookies (se aparecer)
            try:
                btn = drv.find_element(By.CSS_SELECTOR, "#didomi-notice-agree-button, button[aria-label*='Aceitar']")
                if btn.is_displayed():
                    btn.click()
                    time.sleep(0.4)
            except Exception:
                pass

            gentle_scroll(drv, times=15, min_sleep=0.6, max_sleep=1.2)
            soup = BeautifulSoup(drv.page_source, "html.parser")

            for a in soup.select("a[href^='/vaga-de-']"):
                title = a.get_text(strip=True)
                href = a.get("href", "")
                if not href:
                    continue
                full = "https://www.infojobs.com.br" + href
                canon = canonicalize_link(full, "infojobs")
                uid = extract_job_uid(full, "infojobs")

                card = a.find_parent(["article", "div", "li"])
                company = ""
                location = estado
                tipo_vaga = ""
                if card:
                    txt = card.get_text(" ", strip=True).lower()
                    if "home office" in txt or "remoto" in txt:
                        tipo_vaga = "Remoto"
                    elif "hibrido" in txt or "híbrido" in txt:
                        tipo_vaga = "Híbrido"
                    elif "presencial" in txt:
                        tipo_vaga = "Presencial"

                posts.append({
                    "job_uid": uid,
                    "title": title,
                    "company": company,
                    "location": location,
                    "link": full,
                    "canonical_link": canon,
                    "tipo_vaga": tipo_vaga,
                    "data_publicacao": "hoje",
                    "source": "infojobs",
                })
        except Exception as e:
            print(f"[InfoJobs] Erro: {e}", flush=True)

    try:
        drv.quit()
    except Exception:
        pass

    return posts

# =============================================================================
# DB
# =============================================================================
DDL_VAGAS = """
CREATE TABLE IF NOT EXISTS vagas (
    id BIGSERIAL PRIMARY KEY,
    job_uid TEXT,
    title TEXT,
    company TEXT,
    location TEXT,
    link TEXT,
    canonical_link TEXT,
    tipo_vaga TEXT,
    data_publicacao TEXT,
    source TEXT,
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now()
);
"""
DDL_ALTERS = [
    "ALTER TABLE vagas ADD COLUMN IF NOT EXISTS job_uid TEXT;",
    "ALTER TABLE vagas ADD COLUMN IF NOT EXISTS canonical_link TEXT;",
    "CREATE UNIQUE INDEX IF NOT EXISTS uniq_vagas_job_uid ON vagas(job_uid);",
    "CREATE INDEX IF NOT EXISTS idx_vagas_source ON vagas(source);",
]

INSERT_VAGAS = """
INSERT INTO vagas (job_uid, title, company, location, link, canonical_link, tipo_vaga, data_publicacao, source, updated_at)
VALUES %s
ON CONFLICT (job_uid) DO UPDATE SET
  title = EXCLUDED.title,
  company = EXCLUDED.company,
  location = EXCLUDED.location,
  link = EXCLUDED.link,
  canonical_link = EXCLUDED.canonical_link,
  tipo_vaga = EXCLUDED.tipo_vaga,
  data_publicacao = EXCLUDED.data_publicacao,
  source = EXCLUDED.source,
  updated_at = now();
"""

def save_to_postgres(rows: list[dict]):
    if not rows:
        print("[DB] Nada para inserir.", flush=True)
        return
    try:
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
        )
        cur = conn.cursor()
        cur.execute(DDL_VAGAS)
        for q in DDL_ALTERS:
            cur.execute(q)

        values = [
            (
                r.get("job_uid", ""),
                r.get("title", ""),
                r.get("company", ""),
                r.get("location", ""),
                r.get("link", ""),
                r.get("canonical_link", ""),
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
            print(f"[DB] Upsert OK: {len(values)} registros.", flush=True)

        cur.close()
        conn.close()
    except Exception as e:
        print(f"[DB] Erro ao salvar: {e}", flush=True)

# =============================================================================
# Scheduler
# =============================================================================
def seconds_until_next_run(now: datetime | None = None) -> tuple[int, datetime]:
    now = now or datetime.now()
    today_slots = [now.replace(hour=h, minute=0, second=0, microsecond=0) for h in SCHEDULE_HOURS]
    future = [t for t in today_slots if t > now]
    if future:
        nxt = min(future)
    else:
        # amanhã, no primeiro horário
        nxt = (now + timedelta(days=1)).replace(hour=SCHEDULE_HOURS[0], minute=0, second=0, microsecond=0)
    return int((nxt - now).total_seconds()), nxt

# =============================================================================
# Main
# =============================================================================
def run_once():
    all_posts: list[dict] = []

    li_driver = None
    try:
        if "linkedin" in SCRAPE_SOURCES:
            li_driver = make_driver()
            print("[BOOT] Chromium (LinkedIn) iniciado.", flush=True)
            all_posts.extend(scrape_linkedin(li_driver))
    except Exception as e:
        print(f"[RUN] Erro LinkedIn: {e}", flush=True)
    finally:
        try:
            if li_driver:
                li_driver.quit()
        except Exception:
            pass

    if "infojobs" in SCRAPE_SOURCES:
        try:
            all_posts.extend(scrape_infojobs())
        except Exception as e:
            print(f"[RUN] Erro InfoJobs: {e}", flush=True)

    # Dedup por job_uid
    dedup = {}
    for p in all_posts:
        uid = p.get("job_uid")
        if uid:
            dedup[uid] = p
    final = list(dedup.values())

    print(f"[TOTAL] Coletadas {len(final)} vagas após deduplicação.", flush=True)
    save_to_postgres(final)

if __name__ == "__main__":
    print(
        f"== AlertaJobs Scraper ==\n"
        f"Sources: {SCRAPE_SOURCES}\n"
        f"Horários: {SCHEDULE_HOURS}h (hora local do container)\n"
        f"Keywords: '{KEYWORDS}' | Location: '{LOCATION}'\n"
        f"DB: {DB_USER}@{DB_HOST}:{DB_PORT}/{DB_NAME}",
        flush=True,
    )
    while True:
        run_once()
        secs, nxt = seconds_until_next_run()
        print(f"[SLEEP] Próxima execução {nxt.strftime('%d/%m %H:%M')} (em {secs}s)…", flush=True)
        time.sleep(secs)
