import pandas as pd

def find_date_column(df, candidates):
    cols_lower = {c.lower(): c for c in df.columns}
    for cand in candidates:
        if cand.lower() in cols_lower:
            return cols_lower[cand.lower()]
    for c in df.columns:

try:
    df[date_col] = pd.to_datetime(
        df[date_col],
        errors="coerce",
        dayfirst=True,
        infer_datetime_format=True
    )
except Exception:
    return None


def standardize_date_index(df, date_col):
    df = df.copy()
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce", dayfirst=True)
    df = df.dropna(subset=[date_col])
    df = df.set_index(date_col).sort_index()
    df.index.name = "date"
    return df

def detect_numeric_columns(df, patterns):
    numeric_cols = []
    for col in df.columns:
        col_lower = col.lower()
        if any(p in col_lower for p in patterns):
            numeric_cols.append(col)
    if not numeric_cols:
        numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()
    return numeric_cols

def clean_numeric_column(series):
    s = series.astype(str)
    s = s.str.replace(",", "", regex=False)
    s = s.str.replace(r"[<>]", "", regex=True)
    s = s.str.strip()
    return pd.to_numeric(s, errors="coerce")

def clean_numeric_columns(df, numeric_cols):
    df = df.copy()
    for col in numeric_cols:
        df[col] = clean_numeric_column(df[col])
    return df

def add_provenance(df, source_file):
    df = df.copy()
    df["source_file"] = source_file
    return df

