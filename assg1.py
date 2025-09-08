import pandas as pd
import json
import logging
import os
from pathlib import Path
from dotenv import load_dotenv

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Load environment variables
load_dotenv()

RAW_DATA_PATH = os.getenv("RAW_DATA_PATH")
CLEAN_CSV_PATH = os.getenv("CLEAN_CSV_PATH")
COLUMNS_FILE_PATH = os.getenv("COLUMNS_FILE_PATH")

def load_columns(columns_file):
    """Load column names from JSON file."""
    try:
        with open(columns_file, "r") as f:
            data = json.load(f)
        return data["columns"]
    except Exception as e:
        logging.error(f" Error loading columns file: {e}")
        return None

def load_dataset(file_path: str, columns: list) -> pd.DataFrame:
    """Load dataset from .data, .csv, or .json formats."""
    try:
        file = Path(file_path)
        if not file.exists():
            logging.error(f" File not found: {file_path}")
            return None

        if file.suffix == ".data":
            df = pd.read_csv(file, sep=r"\s+", na_values="?", names=columns)
        elif file.suffix == ".csv":
            df = pd.read_csv(file, na_values="?")
        elif file.suffix == ".json":
            df = pd.read_json(file)
        else:
            logging.error(" Unsupported file format")
            return None

        logging.info(" Raw data loaded successfully")
        return df
    except Exception as e:
        logging.error(f" Error loading dataset: {e}")
        return None

def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """Clean dataset: remove missing values, identify numeric/categorical, convert types."""
    try:
        # Drop missing values
        df.dropna(inplace=True)
        logging.info(f" Removed missing values, new shape: {df.shape}")

        # Identify numeric and categorical columns
        numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()
        categorical_cols = df.select_dtypes(exclude=["number"]).columns.tolist()
        logging.info(f"Numeric columns: {numeric_cols}")
        logging.info(f"Categorical columns: {categorical_cols}")

        # Convert categorical columns to category dtype
        for col in categorical_cols:
            df[col] = df[col].astype("category")
        logging.info(" Converted categorical columns")

        return df
    except Exception as e:
        logging.error(f" Error cleaning dataset: {e}")
        return df

def save_cleaned_data(df: pd.DataFrame, csv_path: str):
    """Save cleaned dataset to CSV."""
    try:
        df.to_csv(csv_path, index=False)
        logging.info(f" Cleaned data saved to {csv_path}")
    except Exception as e:
        logging.error(f" Error saving cleaned dataset: {e}")

if __name__ == "__main__":
    columns = load_columns(COLUMNS_FILE_PATH)
    if columns:
        df = load_dataset(RAW_DATA_PATH, columns)
        if df is not None:
            df = clean_data(df)
            save_cleaned_data(df, CLEAN_CSV_PATH)
