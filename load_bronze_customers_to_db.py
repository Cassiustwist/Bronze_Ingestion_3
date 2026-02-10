import os
import pandas as pd
import psycopg2
from dotenv import load_dotenv


def main():
    load_dotenv()

    schema = os.getenv("DB_SCHEMA", "bronze")
    table = os.getenv("DB_TABLE", "customers")

    data_dir = os.getenv("DATA_DIR", "data")
    bronze_file = os.getenv("BRONZE_FILE", "bronze_customers.csv")
    bronze_path = os.path.join(data_dir, "bronze", bronze_file)

    # Lê o bronze e normaliza tipos (principalmente age)
    df = pd.read_csv(bronze_path, dtype=str, keep_default_na=True)

    # age: converte para número, inválidos viram NaN → depois vira None
    df["age"] = pd.to_numeric(df["age"], errors="coerce")

    conn = psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASS"),
    )
    cur = conn.cursor()

    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {schema}.{table} (
            customer_name TEXT,
            age INT,
            city TEXT
        );
    """)

    insert_sql = f"INSERT INTO {schema}.{table} (customer_name, age, city) VALUES (%s, %s, %s);"

    for _, row in df.iterrows():
        name = row.get("customer_name")
        city = row.get("city")

        # Converte NaN -> None (NULL no Postgres)
        age = row.get("age")
        if pd.isna(age):
            age = None
        else:
            age = int(age)

        # Também converte strings vazias pra None (opcional, mas limpo)
        if isinstance(name, str) and name.strip() == "":
            name = None
        if isinstance(city, str) and city.strip() == "":
            city = None

        cur.execute(insert_sql, (name, age, city))

    conn.commit()
    cur.close()
    conn.close()
    print("✅ Dados carregados no banco com sucesso!")


if __name__ == "__main__":
    main()
