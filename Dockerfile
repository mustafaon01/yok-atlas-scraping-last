# Base image olarak Python 3.9 kullanıyoruz
FROM python:3.11-slim

# Çalışma dizinini oluştur
WORKDIR /app

# Gerekli paketlerin listesini kopyala
COPY requirements.txt requirements.txt

# Gerekli paketleri yükle
RUN pip install --no-cache-dir -r requirements.txt

# Python kodunu kopyala
COPY . .

# .env dosyasını kopyala
COPY .env .env

# Python kodunu çalıştır
CMD ["python", "app.py"]
