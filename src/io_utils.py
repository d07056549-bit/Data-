import os
from pathlib import Path
from typing import List
import pandas as pd

def list_data_files(root: str, exts=(".csv", ".xlsx", ".xls")) -> List[Path]:
    root_path = Path(root)
    return [
        p for p in root_path.rglob("*")
        if p.is_file() and p.suffix.lower() in exts
    ]

def load_table(path: Path) -> pd.DataFrame:
    if path.suffix.lower() == ".csv":
        return pd.read_csv(path)
    else:
        return pd.read_excel(path)

def ensure_dir(path: Path):
    path.mkdir(parents=True, exist_ok=True)
