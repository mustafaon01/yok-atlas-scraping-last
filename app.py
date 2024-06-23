import os
import requests
from bs4 import BeautifulSoup
import pandas as pd
from sqlalchemy import create_engine, text
import time
from dotenv import load_dotenv
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

load_dotenv()


years = [2019, 2020, 2021, 2022]

table_name = "genel_bilgiler_last_2024"
# table_name = "demo_genel_bilgiler_1"

# Veritabanı bağlantı parametreleri
hostname = os.getenv('DB_HOST')
database = os.getenv('DB_NAME')
username = os.getenv('DB_USER')
pwd = os.getenv('DB_PWD')
port_id = os.getenv('DB_PORT')

# Veritabanı bağlantı dizgesi
db_url = f'postgresql://{username}:{pwd}@{hostname}:{port_id}/{database}'

# Base URL

for year in years:
    codes_file = f'{year}.txt'
    base_url = f'https://yokatlas.yok.gov.tr/{year}/content/lisans-dynamic/1000_1.php?y='
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
                                'bolum_adi': header,
                                'yil': year
                            })
                time.sleep(0.2)
            else:
                print(f"Bozuk program kodu: {pro_code}")
        except requests.exceptions.RequestException as e:
            print(f"Bağlanılamadı: {pro_code}, hata: {e}")

    # Veriyi pandas DataFrame'e dönüştürme
    if all_data:
        df = pd.DataFrame(all_data)

        df_pivot = df.pivot_table(index=['pro_code', 'bolum_adi', 'yil'], columns='Attribute', values='Value', aggfunc='first').reset_index()

        # df_pivot['id'] = range(1, len(df_pivot) + 1)

        # Kolon isimlerini değiştirme
        df_pivot = df_pivot.rename(columns={
            f"{year} Tavan Puan(0,12)*": "Tavan_Puan_(0,12)",
            f"{year} Tavan Başarı Sırası(0,12)*": "Tavan_Basari_Sirasi_(0,12)",
            f"{year-1}'de Yerleşip {year}'de OBP'si Kırılarak Yerleşen Sayısı": "OBP'si Kırılarak Yerleşen Sayısı",
            f"{year-1}'de Yerleşip {year}'de OBP'si Kırılarak Yerleşen Sayısı*": "OBP'si Kırılarak Yerleşen Sayısı"
        })

        # Bazı yıllar columlar böyle geliyor bazı yıllar gelmiyorlar.
        df_pivot.columns = [col.replace('*', '') if col in ["Toplam Kontenjan**", "Yerleşenlerin Ortalama OBP'si*", "Yerleşenlerin Ortalama Diploma Notu*"] else col for col in df_pivot.columns]

        engine = create_engine(db_url)

        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT MAX(id) FROM {table_name}"))
            # print("result", result.scalar())
            max_id_temp = result.scalar()
            max_id = max_id_temp if max_id_temp else 0

        df_pivot['id'] = range(max_id + 1, max_id + 1 + len(df_pivot))

        cols = df_pivot.columns.tolist()
        cols = ['id'] + [col for col in cols if col != 'id']
        df_pivot = df_pivot[cols]

        df_pivot.to_sql(table_name, con=engine, if_exists='append', index=False)

        print(f"{year} Yılına Ait Veri başarıyla veritabanına kaydedildi.")
    else:
        print("Çekilen veri yok.")
