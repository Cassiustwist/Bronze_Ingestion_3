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
