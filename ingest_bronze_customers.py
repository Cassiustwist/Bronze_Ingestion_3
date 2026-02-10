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