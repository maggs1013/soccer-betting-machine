import pandas as pd

def read_csv_safe(path: str) -> pd.DataFrame:
    try:
        df = pd.read_csv(path)
    except Exception:
        df = pd.DataFrame()
    return df

def write_csv(df: pd.DataFrame, path: str):
    df.to_csv(path, index=False)