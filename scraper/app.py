
import pandas as pd
import time
import random
from selenium import webdriver
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
import requests

# Configurações do navegador
options = webdriver.ChromeOptions()
options.add_argument('--disable-blink-features=AutomationControlled')
options.add_argument('--start-maximized')
options.add_argument('--headless=new')
options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
driver = webdriver.Chrome(options=options)

# Parâmetros de busca
KEYWORDS = ''
LOCATION = 'Brasil'
GEOID = '106057199'
F_TPR = 'r86400'
POSITION = '1'
PAGE_NUM = '0'

tipos_vaga = {'Remoto': '2', 'Presencial': '1', 'Híbrido': '3'}
todos_posts = []
br_keywords = [
	'brasil', 'brazil', 'sp', 'são paulo', 'rio de janeiro', 'rj', 'minas gerais', 'mg', 'bahia', 'ba', 'paraná', 'pr', 'pernambuco', 'pe',
	'ceará', 'ce', 'goiás', 'go', 'distrito federal', 'df', 'porto alegre', 'rs', 'recife', 'fortaleza', 'curitiba', 'salvador', 'manaus', 'belém', 'campinas', 'br', 'florianópolis', 'joinville', 'natal', 'maceió', 'aracaju', 'teresina', 'palmas', 'campo grande', 'cuiabá', 'vitória', 'santos', 'sorocaba', 'ribeirão preto', 'marília', 'londrina', 'maringá', 'uberlândia', 'juiz de fora', 'petrolina', 'caruaru', 'itajaí', 'blumenau', 'criciúma', 'chapecó', 'ponta grossa', 'cascavel', 'foz do iguaçu', 'aparecida de goiânia', 'anápolis', 'goianésia', 'catalão', 'luziânia', 'formosa', 'valparaíso de goiás', 'senador canedo', 'trindade', 'caldas novas', 'rio verde', 'jataí', 'mineiros', 'quirinópolis', 'porangatu', 'itumbiara', 'ipanema', 'patos de minas', 'araguari', 'divinópolis', 'montes claros', 'teófilo otoni', 'varginha', 'poços de caldas', 'passos', 'alfenas', 'barbacena', 'são joão del rei', 'são lourenço', 'ouro preto', 'diamantina', 'ipatinga', 'timóteo', 'coronel fabriciano', 'governador valadares', 'caratinga', 'muriaé', 'cataguases', 'ubá', 'viçosa', 'são francisco', 'pirapora', 'janúba', 'montes claros', 'bocaiúva', 'salinas', 'pedro leopoldo', 'sete lagoas', 'contagem', 'betim', 'nova lima', 'sabará', 'santa luzia', 'vila velha', 'cariacica', 'serra', 'guarapari', 'colatina', 'linhares', 'são mateus', 'aracruz', 'castelo', 'cachoeiro de itapemirim', 'marataízes', 'anchieta', 'piúma', 'iconha', 'itapemirim', 'presidente kennedy', 'mimoso do sul', 'muqui', 'atílio vivácqua', 'jerônimo monteiro', 'dores do rio preto', 'divino de são lourenço', 'guaçuí', 'alegre', 'ibiá', 'manhuaçu', 'aimorés', 'resplendor', 'baixo guandu', 'aimorés', 'resplendor', 'baixo guandu', 'aimorés', 'resplendor', 'baixo guandu'
]


import sys

for tipo_nome, tipo_valor in tipos_vaga.items():
	try:
		url = f'https://www.linkedin.com/jobs/search?keywords={KEYWORDS}&location={LOCATION}&geoId={GEOID}&f_TPR={F_TPR}&f_WT={tipo_valor}&position={POSITION}&pageNum={PAGE_NUM}'
		print(f'Buscando vagas tipo {tipo_nome}: {url}')
		driver.get(url)

		# Scroll até o final para carregar vagas iniciais
		for _ in range(20):
			driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
			time.sleep(random.uniform(2, 4))
			# Tenta fechar modal se aparecer
			try:
				# Modal de login (LinkedIn)
				close_btn = driver.find_element(By.XPATH, "//button[@aria-label='Fechar']")
				if close_btn.is_displayed() and close_btn.is_enabled():
					close_btn.click()
					print("Modal fechado automaticamente.")
					time.sleep(1)
			except Exception:
				pass

	# Apenas scroll para carregar vagas, sem clicar em 'Ver mais vagas'

		print("Extraindo vagas...")
		soup = BeautifulSoup(driver.page_source, 'html.parser')
		job_divs = soup.find_all('div', class_='job-card-container')
		if not job_divs:
			job_divs = soup.find_all('div', class_='base-card')
		print(f"Quantidade de cards encontrados: {len(job_divs)}")

		for div in job_divs:
			title = div.find('h3', class_='base-search-card__title')
			if not title:
				title = div.find('h3', class_='base-card__title')
			company = div.find('h4', class_='base-search-card__company-name')
			if not company:
				company = div.find('h4', class_='base-card__company-name')
			location = div.find('span', class_='job-card-container__location')
			if not location:
				location = div.find('span', class_='job-search-card__location')
			link_tag = div.find('a', class_='base-card__full-link')
			if not link_tag:
				link_tag = div.find('a', class_='result-card__full-card-link')
			link = link_tag['href'] if link_tag and link_tag.has_attr('href') else ''
			date_tag = div.find('time')
			date_posted = date_tag.text.strip().lower() if date_tag else ''
			loc_text = location.text.lower() if location else ''
			if any(cidade in loc_text for cidade in br_keywords):
				desc_tag = div.find('span', class_='job-search-card__employment-type')
				tipo_vaga = tipo_nome
				if desc_tag:
					tipo_vaga = desc_tag.text.strip()
				if any(x in (title.text if title else '').lower() for x in ['remoto', 'remote']):
					tipo_vaga = 'Remoto'
				elif any(x in loc_text for x in ['remoto', 'remote']):
					tipo_vaga = 'Remoto'
				elif any(x in (title.text if title else '').lower() for x in ['híbrido', 'hybrid']):
					tipo_vaga = 'Híbrido'
				elif any(x in loc_text for x in ['híbrido', 'hybrid']):
					tipo_vaga = 'Híbrido'
				elif tipo_vaga == '':
					tipo_vaga = tipo_nome
				todos_posts.append({
					'title': title.text.strip() if title else '',
					'company': company.text.strip() if company else '',
					'location': location.text.strip() if location else '',
					'link': link,
					'tipo_vaga': tipo_vaga,
					'data_publicacao': date_posted
				})
	except Exception as e:
		print(f"Erro crítico durante busca do tipo '{tipo_nome}': {e}")
		driver.quit()


driver.quit()

# Coleta de vagas do InfoJobs
def coletar_infojobs(url, estado):
	print(f'Buscando vagas InfoJobs: {url}')
	vagas = []
	try:
		# Usa Selenium para abrir a página e fazer scroll
		options_infojobs = webdriver.ChromeOptions()
		options_infojobs.add_argument('--disable-blink-features=AutomationControlled')
		options_infojobs.add_argument('--start-maximized')
		options_infojobs.add_argument('--headless=new')
		options_infojobs.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
		driver_infojobs = webdriver.Chrome(options=options_infojobs)
		driver_infojobs.get(url)
		# Scroll para carregar mais vagas
		for _ in range(20):
			driver_infojobs.execute_script("window.scrollTo(0, document.body.scrollHeight);")
			time.sleep(random.uniform(2, 4))
		soup = BeautifulSoup(driver_infojobs.page_source, 'html.parser')
		driver_infojobs.quit()
		# Busca todos os cards de vaga
		cards = soup.find_all('div', class_=lambda x: x and 'card' in x)
		for card in cards:
			# Título
			title_tag = card.find('a', href=True)
			if not title_tag or not title_tag['href'].startswith('/vaga-de-'):
				continue
			title = title_tag.text.strip()
			link = 'https://www.infojobs.com.br' + title_tag['href']
			# Empresa
			company_tag = card.find('span', string=True)
			company = ''
			if company_tag:
				company = company_tag.text.strip()
			# Localização
			location_tag = card.find('span', string=True)
			location = estado
			if location_tag and ('SP' in location_tag.text or 'RJ' in location_tag.text):
				location = location_tag.text.strip()
			# Tipo de vaga
			tipo_vaga = ''
			tipo_tag = card.find('span', string=True)
			if tipo_tag and any(x in tipo_tag.text.lower() for x in ['presencial', 'home office', 'híbrido']):
				tipo_vaga = tipo_tag.text.strip()
			# Data
			date_posted = 'hoje'
			date_tag = card.find('span', string='Hoje')
			if date_tag:
				date_posted = date_tag.text.strip()
			vagas.append({
				'title': title,
				'company': company,
				'location': location,
				'link': link,
				'tipo_vaga': tipo_vaga,
				'data_publicacao': date_posted
			})
	except Exception as e:
		print(f'Erro ao coletar InfoJobs {estado}: {e}')
	return vagas

# URLs InfoJobs
infojobs_links = [
	('https://www.infojobs.com.br/empregos-em-sao-paulo.aspx?Antiguedad=1', 'São Paulo'),
	('https://www.infojobs.com.br/empregos-em-rio-janeiro.aspx?Antiguedad=1', 'Rio de Janeiro')
]

for url, estado in infojobs_links:
	todos_posts.extend(coletar_infojobs(url, estado))

df = pd.DataFrame(todos_posts)

# Grava no PostgreSQL

# Para uso em VPS/n8n, instale: pip install psycopg2-binary
import psycopg2
from psycopg2 import sql
import os


# Configuração via variáveis de ambiente para facilitar uso no n8n/VPS
PG_HOST = os.getenv('PG_HOST', 'localhost')
PG_PORT = int(os.getenv('PG_PORT', '5432'))
PG_DB = os.getenv('PG_DB', 'n8n')
PG_USER = os.getenv('PG_USER', 'n8n')
PG_PASSWORD = os.getenv('PG_PASSWORD', 'ChangeThis!123')

if not df.empty:
	print(f'Total de vagas capturadas: {len(df)}')
	try:
		conn = psycopg2.connect(
			host=PG_HOST,
			port=PG_PORT,
			dbname=PG_DB,
			user=PG_USER,
			password=PG_PASSWORD
		)
		cur = conn.cursor()
		# Testa conexão
		cur.execute('SELECT 1;')
		# Insere dados
		for _, row in df.iterrows():
			cur.execute(
				sql.SQL("""
					INSERT INTO vagas (title, company, location, link, tipo_vaga, data_publicacao)
					VALUES (%s, %s, %s, %s, %s, %s)
				"""),
				(row['title'], row['company'], row['location'], row['link'], row['tipo_vaga'], row['data_publicacao'])
			)
		conn.commit()
		cur.close()
		conn.close()
		print(f'Dados inseridos no PostgreSQL com sucesso: {len(df)} vagas.')
	except Exception as e:
		print(f'Erro ao inserir no PostgreSQL: {e}')
else:
	print('Nenhuma vaga encontrada com os filtros aplicados.')




# Scroll automático para carregar mais vagas
SCROLL_PAUSES = 20  # Aumente este valor para mais scrolls
for i in range(SCROLL_PAUSES):
	driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
	time.sleep(random.uniform(2, 4))

print("Extraindo vagas após scroll...")
soup = BeautifulSoup(driver.page_source, 'html.parser')
job_divs = soup.find_all('div', class_='job-card-container')
if not job_divs:
	job_divs = soup.find_all('div', class_='base-card')
print(f"Quantidade de cards encontrados: {len(job_divs)}")

br_keywords = [
	'brasil', 'brazil', 'sp', 'são paulo', 'rio de janeiro', 'rj', 'minas gerais', 'mg', 'bahia', 'ba', 'paraná', 'pr', 'pernambuco', 'pe',
	'ceará', 'ce', 'goiás', 'go', 'distrito federal', 'df', 'porto alegre', 'rs', 'recife', 'fortaleza', 'curitiba', 'salvador', 'manaus', 'belém', 'campinas', 'br', 'florianópolis', 'joinville', 'natal', 'maceió', 'aracaju', 'teresina', 'palmas', 'campo grande', 'cuiabá', 'vitória', 'santos', 'sorocaba', 'ribeirão preto', 'marília', 'londrina', 'maringá', 'uberlândia', 'juiz de fora', 'petrolina', 'caruaru', 'itajaí', 'blumenau', 'criciúma', 'chapecó', 'ponta grossa', 'cascavel', 'foz do iguaçu', 'aparecida de goiânia', 'anápolis', 'goianésia', 'catalão', 'luziânia', 'formosa', 'valparaíso de goiás', 'senador canedo', 'trindade', 'caldas novas', 'rio verde', 'jataí', 'mineiros', 'quirinópolis', 'porangatu', 'itumbiara', 'ipanema', 'patos de minas', 'araguari', 'divinópolis', 'montes claros', 'teófilo otoni', 'varginha', 'poços de caldas', 'passos', 'alfenas', 'barbacena', 'são joão del rei', 'são lourenço', 'ouro preto', 'diamantina', 'ipatinga', 'timóteo', 'coronel fabriciano', 'governador valadares', 'caratinga', 'muriaé', 'cataguases', 'ubá', 'viçosa', 'são francisco', 'pirapora', 'janúba', 'montes claros', 'bocaiúva', 'salinas', 'pedro leopoldo', 'sete lagoas', 'contagem', 'betim', 'nova lima', 'sabará', 'santa luzia', 'vila velha', 'cariacica', 'serra', 'guarapari', 'colatina', 'linhares', 'são mateus', 'aracruz', 'castelo', 'cachoeiro de itapemirim', 'marataízes', 'anchieta', 'piúma', 'iconha', 'itapemirim', 'presidente kennedy', 'mimoso do sul', 'muqui', 'atílio vivácqua', 'jerônimo monteiro', 'dores do rio preto', 'divino de são lourenço', 'guaçuí', 'alegre', 'ibiá', 'manhuaçu', 'aimorés', 'resplendor', 'baixo guandu', 'aimorés', 'resplendor', 'baixo guandu', 'aimorés', 'resplendor', 'baixo guandu'
]


