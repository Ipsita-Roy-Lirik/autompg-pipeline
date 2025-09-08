import pandas as pd
import logging
import json
from dotenv import load_dotenv
import os
from sqlalchemy import create_engine

# ---------------- Logging Setup ----------------

# Reset logging handlers to avoid duplicate logs
for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# ---------------- Load Environment Variables ----------------
load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASS")
CSV_FILE = os.getenv("CLEAN_CSV_PATH")
TABLE_NAME = os.getenv("TABLE_NAME")

# ---------------- Load Column Names ----------------
with open("columns.json", "r") as f:
    COLUMNS = json.load(f)

# ---------------- Database Connection ----------------
def get_engine():
    """Create SQLAlchemy engine"""
    try:
        engine = create_engine(
            f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        )
        logging.info("✅ Database connection established")
        return engine
    except Exception as e:
        logging.error(f"❌ Failed to connect to DB: {e}")
        return None

# ---------------- Load CSV ----------------
def load_csv(file_path):
    """Load cleaned CSV"""
    try:
        df = pd.read_csv(file_path)
        logging.info(f"✅ Cleaned CSV loaded: {df.shape[0]} rows")
        return df
    except Exception as e:
        logging.error(f"❌ Failed to load CSV: {e}")
        return None


def upload_to_postgresql(df, table_name: str):
    
    try:
        engine = get_engine()
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        logging.info(f"✅ Data uploaded to table {table_name} in PostgreSQL .")
    except Exception as e:
        logging.error(f"❌ Error uploading data to PostgreSQL: {e}")
        raise

# ---------------- Main ----------------
def main():
    engine = get_engine()
    if not engine:
        return

    df = load_csv(CSV_FILE)
    if df is None:
        return

    upload_to_postgresql(df, TABLE_NAME)

if __name__ == "__main__":
    main()
