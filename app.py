import os
import requests
from bs4 import BeautifulSoup
import pandas as pd
from sqlalchemy import create_engine
import time
from dotenv import load_dotenv

load_dotenv()

# Dosya yolu
codes_file = 'codes_2023.txt'

# Veritabanı bağlantı parametreleri
hostname = os.getenv('DB_HOST')
database = os.getenv('DB_NAME')
username = os.getenv('DB_USER')
pwd = os.getenv('DB_PWD')
port_id = os.getenv('DB_PORT')

# Veritabanı bağlantı dizgesi
db_url = f'postgresql://{username}:{pwd}@{hostname}:{port_id}/{database}'

# Base URL
base_url = 'https://yokatlas.yok.gov.tr/content/lisans-dynamic/1000_1.php?y='

# Pro code'ları dosyadan okuma
with open(codes_file, 'r') as file:
    pro_codes = file.readlines()

# Tüm verileri toplamak için bir liste
all_data = []

# Her pro_code için veri çekme ve toplama
for pro_code in pro_codes:
    pro_code = pro_code.strip()
    try:
        response = requests.get(f"{base_url}{pro_code}", timeout=20, verify=False)
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')
            # Tablo başlığını çekme
            header = soup.find('th', colspan="2").get_text(strip=True) if soup.find('th', colspan="2") else None
            tables = soup.find_all('table', class_='table table-bordered')
            for table in tables:
                rows = table.find_all('tr')
                for row in rows:
                    cols = row.find_all('td')
                    if len(cols) == 2:
                        key = cols[0].get_text(strip=True)
                        value = cols[1].get_text(strip=True)
                        all_data.append({
                            'pro_code': pro_code,
                            'Attribute': key,
                            'Value': value,
                            'bolum_adi': header
                        })
            time.sleep(0.2)  # İstekler arasında bekleme süresi
        else:
            print(f"Bozuk program kodu: {pro_code}")
    except requests.exceptions.RequestException as e:
        print(f"Bağlanılamadı: {pro_code}, hata: {e}")

# Veriyi pandas DataFrame'e dönüştürme
if all_data:
    df = pd.DataFrame(all_data)

    # Veriyi pivot edip geniş formata çevirme
    df_pivot = df.pivot_table(index=['pro_code', 'bolum_adi'], columns='Attribute', values='Value', aggfunc='first').reset_index()

    # id sütunu ekleme
    df_pivot['id'] = range(1, len(df_pivot) + 1)

    # Sütun sıralamasını ayarlama
    cols = df_pivot.columns.tolist()
    cols = ['id'] + [col for col in cols if col != 'id']
    df_pivot = df_pivot[cols]

    # DataFrame'i veritabanına kaydetme
    engine = create_engine(db_url)
    df_pivot.to_sql('genel_bilgiler_last_2024', con=engine, if_exists='replace', index=False)

    print("Veri başarıyla veritabanına kaydedildi.")
else:
    print("Çekilen veri yok.")
