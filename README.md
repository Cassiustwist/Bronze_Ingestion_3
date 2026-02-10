# Pipeline Bronze — Ingestão CSV (Customers) → Postgres

Este projeto treina um fluxo real de Engenharia de Dados, repetível e simples:

RAW (CSV sujo) → BRONZE (CSV padronizado) → Postgres (tabela bronze.customers)

---

## 0) Regras do Projeto (pra não dar erro)

- Trabalhe sempre na pasta raiz do repositório (onde existe .git/)
- NÃO crie subpastas extras tipo "Bronze-4" dentro do repo
- .env e data/ NÃO sobem pro Git

---

## 1) Estrutura final esperada

Na raiz do repo:

.
├── data/
│   ├── raw/
│   │   └── customers.csv
│   └── bronze/
│       └── bronze_customers.csv
├── ingest_bronze_customers.py
├── load_bronze_customers_to_db.py
├── requirements.txt
├── .env
├── .env.example
└── .gitignore

---

## 2) Criar pastas de dados

data/raw  
data/bronze

---

## 3) Criar o CSV RAW

Arquivo: data/raw/customers.csv

Customer Name , AGE , City  
  Ana Silva , 29 , sao paulo  
Bruno Costa, ,RIO DE JANEIRO  
CARLA SOUZA ,35, belo horizonte  
Diego Lima,42 ,curitiba  
 Fernanda Alves, 27,São Paulo  

---

## 4) Criar VENV (Python 3.11)

py -0  
py -3.11 -m venv .venv  
.venv\Scripts\activate  
python --version  

---

## 5) Instalar dependências

pip install pandas psycopg2-binary python-dotenv  
pip freeze > requirements.txt  

---

## 6) Criar .env

DATA_DIR=data

DB_HOST=localhost  
DB_PORT=5432  
DB_NAME=empregadados_local  
DB_USER=postgres  
DB_PASS=SUA_SENHA_AQUI  

DB_SCHEMA=bronze  
DB_TABLE=customers  
RAW_FILE=customers.csv  
BRONZE_FILE=bronze_customers.csv  

---

## 7) Criar .env.example

DATA_DIR=data

DB_HOST=localhost  
DB_PORT=5432  
DB_NAME=empregadados_local  
DB_USER=postgres  
DB_PASS=CHANGE_ME  

DB_SCHEMA=bronze  
DB_TABLE=customers  
RAW_FILE=customers.csv  
BRONZE_FILE=bronze_customers.csv  

---

## 8) Criar .gitignore

.venv/  
__pycache__/  
.env  
data/  
*.log  
.ipynb_checkpoints/  

---

## 9) Rodar INGEST (RAW → BRONZE)

python ingest_bronze_customers.py  

Gera: data/bronze/bronze_customers.csv

---

## 10) Rodar LOAD (BRONZE → Postgres)

python load_bronze_customers_to_db.py  

Cria tabela: bronze.customers

---

## 11) Validar no banco

SELECT * FROM bronze.customers;

---

## 12) Subir no Git

git add .  
git commit -m "Pipeline Bronze customers"  
git push  

---

## Fluxo em 1 linha

RAW → ingest → BRONZE → load → Postgres




ingest_bronze_customers.py
import os
from pathlib import Path
import pandas as pd
from dotenv import load_dotenv
import unicodedata
import re


def normalize_whitespace(x):
    if pd.isna(x):
        return pd.NA
    s = str(x).strip()
    s = re.sub(r"\s+", " ", s)
    return s if s else pd.NA


def remove_accents(x):
    if pd.isna(x):
        return pd.NA
    s = str(x)
    return "".join(
        c for c in unicodedata.normalize("NFD", s)
        if unicodedata.category(c) != "Mn"
    )


def clean_text(x):
    x = normalize_whitespace(x)
    if pd.isna(x):
        return pd.NA
    x = remove_accents(x)
    return x.title()


def clean_age(x):
    x = normalize_whitespace(x)
    if pd.isna(x):
        return pd.NA
    try:
        val = int(float(str(x)))
        if val < 0 or val > 120:
            return pd.NA
        return val
    except Exception:
        return pd.NA


def standardize_column_name(col):
    col = col.strip().lower()
    col = re.sub(r"\s+", " ", col)
    col = col.replace(" ", "_")
    col = re.sub(r"[^a-z0-9_]", "", col)
    return col


def main():
    load_dotenv()

    data_dir = Path(os.getenv("DATA_DIR", "data"))
    raw_file = os.getenv("RAW_FILE", "customers.csv")
    bronze_file = os.getenv("BRONZE_FILE", "bronze_customers.csv")

    raw_path = data_dir / "raw" / raw_file
    bronze_path = data_dir / "bronze" / bronze_file
    bronze_path.parent.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(raw_path, dtype=str, skipinitialspace=True)
    df.columns = [standardize_column_name(c) for c in df.columns]

    df = df.rename(columns={
        "customer_name": "customer_name",
        "customername": "customer_name",
        "age": "age",
        "idade": "age",
        "city": "city",
        "cidade": "city",
    })

    df["customer_name"] = df["customer_name"].apply(clean_text)
    df["city"] = df["city"].apply(clean_text)
    df["age"] = df["age"].apply(clean_age).astype("Int64")

    df.to_csv(bronze_path, index=False)
    print("Bronze gerado com sucesso!")
    print(df)


if __name__ == "__main__":
    main()






load_bronze_customers_to_db.py

import os
import pandas as pd
import psycopg2
from dotenv import load_dotenv


def main():
    load_dotenv()

    conn = psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASS"),
    )
    cur = conn.cursor()

    schema = os.getenv("DB_SCHEMA")
    table = os.getenv("DB_TABLE")
    data_dir = os.getenv("DATA_DIR")
    bronze_file = os.getenv("BRONZE_FILE")

    bronze_path = f"{data_dir}/bronze/{bronze_file}"
    df = pd.read_csv(bronze_path)

    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")

    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {schema}.{table} (
            customer_name TEXT,
            age INT,
            city TEXT
        );
    """)

    for _, row in df.iterrows():
        cur.execute(
            f"INSERT INTO {schema}.{table} (customer_name, age, city) VALUES (%s, %s, %s);",
            (row["customer_name"], row["age"], row["city"])
        )

    conn.commit()
    cur.close()
    conn.close()

    print("Dados carregados no banco com sucesso!")


if __name__ == "__main__":
    main()
















