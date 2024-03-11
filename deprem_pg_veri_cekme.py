# Gerekli paket kurulumları için terminal kodları

#pip3 install requests --user
#pip3 install BeautifulSoup --user
#pip3 install pandas --user
#pip3 install create_engine --user
#pip3 install psycopg2 --user

import requests
from bs4 import BeautifulSoup
import pandas as pd
from sqlalchemy import create_engine
import psycopg2

# Veriyi çekeceğimiz URL
url = 'http://www.koeri.boun.edu.tr/scripts/lasteq.asp'

# Sayfayı çekiyoruz
response = requests.get(url)

# BeautifulSoup ile parse ediyoruz
soup = BeautifulSoup(response.text, 'html.parser')

# <pre> etiketini buluyoruz
pre_tag = soup.find('pre')

# İçeriği alıyoruz ve satırlara bölüyoruz
lines = pre_tag.text.split('\n')

# İlk 8 satırı atlıyoruz
lines = lines[8:]

# Her satırı ayrı ayrı parse ediyoruz
data = []
for line in lines:
    data.append(line.split())

# Veriyi bir pandas DataFrame'ine dönüştürüyoruz
df = pd.DataFrame(data)

# PostgreSQL veritabanına bağlanıyoruz
engine = create_engine('postgresql://postgres:password@localhost:5432/postgres')

# DataFrame'i PostgreSQL veritabanına yazıyoruz
df.to_sql('deprem', engine, if_exists='append')

# veri tabanına bağlanıp okunacak tabloyu oluştur
try:
    connection = psycopg2.connect(
        host="localhost",
        database="postgres",
        user="postgres",
        password="password",
        port=5432
    )
    print("Veritabanına başarıyla bağlandı!")
except Exception as e:
    print("Bağlantı hatası:", e)
    
try:
    cursor = connection.cursor()

    # SQL komutlarını çalıştır
    cursor.execute("""
        DELETE FROM deprem a USING deprem b WHERE a.seq < b.seq AND a."1" = b."1" AND a."0" = b."0";
        DELETE FROM deprem WHERE "1" IS NULL;
        update deprem set "7" = null WHERE "7" = '-.-';
        DROP MATERIALIZED VIEW IF EXISTS public.deprem_verileri;
        CREATE MATERIALIZED VIEW deprem_verileri AS
        SELECT
            "index",
            "0"::date AS tarih,
            "1"::time AS saat,
            "2"::decimal AS lat,
            "3"::decimal AS lon,
            "4"::decimal AS derinlik,
            "6"::decimal AS buyukluk,
            "7"::decimal AS hissedilen,
            concat ("8",' ',"9", ' ',"10" )AS bolge,
            "seq" as sira,
            ST_SetSRID(ST_MakePoint("3"::decimal, "2"::decimal), 4326) AS geom
        FROM
            public.deprem;        
        CREATE INDEX deprem_geom_idx ON deprem_verileri USING GIST (geom);
    """)

    # Değişiklikleri kaydet
    connection.commit()
    print("SQL komutları başarıyla çalıştırıldı!")
except Exception as e:
    print("Hata:", e)
finally:
    cursor.close()
    connection.close()
