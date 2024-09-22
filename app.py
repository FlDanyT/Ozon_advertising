import os
import requests
import zipfile
import pandas as pd
from dotenv import load_dotenv
import time

load_dotenv() # Загружаем переменные окружения


host = "https://performance.ozon.ru/api/client/token"

headers_token = {
  "Content-Type": "application/json",
  "Accept": "application/json"
  }

payload = {
    "client_id": os.getenv('client_id'), 
    "client_secret": os.getenv('client_secret'), 
    "grant_type":"client_credentials"
  }


res = requests.post(host, headers=headers_token, json=payload) # Получаем токен
access_token = res.json()['access_token']


url = "https://performance.ozon.ru:443/api/client/campaign"

headers={
  "Authorization": f'Bearer {access_token}'
  }

time.sleep(5)

res = requests.get(url, headers=headers)
compaings = res.json()

active_campaigns = [c for c in compaings['list'] if c['state'] == 'CAMPAIGN_STATE_RUNNING'] # Получаем список активных компаний
active_campaign_ids = [c['id'] for c in active_campaigns] # Получаем id активных кампаний


date_from = '2024-09-01'
date_to = '2024-09-15'

url = "https://performance.ozon.ru:443/api/client/statistics" # Статистика по кампании

payload = {
  "campaigns": active_campaign_ids,
  "dateFrom": date_from,
  "dateTo": date_to,
  "groupBy": "DATE"
}

time.sleep(5)

res = requests.post(url, headers=headers, json=payload)
res.json()

report_uuid = res.json()['UUID']


url = f'https://performance.ozon.ru:443/api/client/statistics/{report_uuid}'

time.sleep(5)
res = requests.get(url, headers=headers)


ulr = 'https://performance.ozon.ru:443/api/client/statistics/report'

payload = {
  'UUID': report_uuid,
}

time.sleep(5)
res = requests.get(ulr, headers=headers, params=payload)


tmp_floder = 'tmp'
report_zip = 'report.zip'
with open(os.path.join(tmp_floder, report_zip), 'wb') as f:
  f.write(res.content)
time.sleep(10)

with zipfile.ZipFile(os.path.join(tmp_floder, report_zip), 'r') as f:
 f.extractall(tmp_floder)


csv_file = [x for x in os.listdir(tmp_floder) if x.endswith('.csv')] # Делаем csv файлы
reports = []

for f in csv_file:
 reports.append(pd.read_csv(
   os.path.join(tmp_floder, f), 
   sep=';',
   skiprows=1,
   skipfooter=1)) # Удаляем ; и первую строку & последнюю строку

df = pd.concat(reports)
df.to_csv('final_report.csv', index=None) # Делаем единый csv файл