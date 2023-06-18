import psycopg2
from pyspark.sql import SparkSession

# Inicie a sessão Spark
spark = SparkSession.builder.getOrCreate()

# Carg dos dados da camada Specialized
df = spark.read.csv('s3://my-bucket/specialized/us-counties-2020.csv', header=True)

# Conecte-se ao banco de dados
conn = psycopg2.connect(database="mydb", user="myuser", password="mypassword", host="myhost", port="myport")
cur = conn.cursor()

# Extracao dos dados únicos de data e localização e insira-os nas tabelas de dimensão
dates = df.select("date").distinct().collect()
locations = df.select("county", "state").distinct().collect()

for row in dates:
    date = row["date"]
    year, month, day = map(int, date.split("-"))
    quarter = (month - 1) // 3 + 1
    week = int(date.strftime("%V"))
    cur.execute("INSERT INTO dim_date (date, year, quarter, month, week, day) VALUES (%s, %s, %s, %s, %s, %s)", (date, year, quarter, month, week, day))

for row in locations:
    county, state = row["county"], row["state"]
    cur.execute("INSERT INTO dim_location (county, state) VALUES (%s, %s)", (county, state))

# Insircao dos dados na tabela de fatos
for row in df.collect():
    date, county, state, cases, deaths = row["date"], row["county"], row["state"], row["cases"], row["deaths"]
    cur.execute("SELECT date_id FROM dim_date WHERE date = %s", (date,))
    date_id = cur.fetchone()[0]
    cur.execute("SELECT location_id FROM dim_location WHERE county = %s AND state = %s", (county, state))
    location_id = cur.fetchone()[0]
    cur.execute("INSERT INTO facts_covid (date_id, location_id, cases, deaths) VALUES (%s, %s, %s, %s)", (date_id, location_id, cases, deaths))

# Fechar a conexão
conn.commit()
cur.close()
conn.close()
