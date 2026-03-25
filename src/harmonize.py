from pathlib import Path
from typing import List, Optional
import pandas as pd

from .cleaning import (
    find_date_column,
    standardize_date_index,
    detect_numeric_columns,
    clean_numeric_columns,
    add_provenance,
)
from .io_utils import ensure_dir

def resample_to_frequencies(
    df: pd.DataFrame,
    numeric_cols: List[str],
    weekly_freq: str,
    monthly_freq: str
) -> (pd.DataFrame, pd.DataFrame):
    agg = {col: "sum" for col in numeric_cols}
    weekly = df.resample(weekly_freq).agg(agg)
    monthly = df.resample(monthly_freq).agg(agg)
    return weekly, monthly

def harmonize_single_file(
    path: Path,
    settings: dict,
    logger,
) -> Optional[dict]:
    import pandas as pd
    from .io_utils import load_table

    try:
        df = load_table(path)
    except Exception as e:
        logger.error(f"Failed to load {path}: {e}")
        return None

    date_col = find_date_column(df, settings["date_column_candidates"])
    if not date_col:
        logger.warning(f"No date column found in {path}, skipping.")
        return None

    df = standardize_date_index(df, date_col)
    if df.empty:
        logger.warning(f"No valid dates in {path}, skipping.")
        return None

    numeric_cols = detect_numeric_columns(df, settings["numeric_column_patterns"])
    if not numeric_cols:
        logger.warning(f"No numeric columns detected in {path}, skipping.")
        return None

    df = clean_numeric_columns(df, numeric_cols)
    df = add_provenance(df, source_file=str(path))

    weekly, monthly = resample_to_frequencies(
        df,
        numeric_cols,
        settings["weekly_frequency"],
        settings["monthly_frequency"],
    )

    return {
        "weekly": weekly,
        "monthly": monthly,
        "numeric_cols": numeric_cols,
    }

def save_harmonized(
    weekly: pd.DataFrame,
    monthly: pd.DataFrame,
    processed_root: Path,
    weekly_folder: str,
    monthly_folder: str,
    rel_path: Path,
):
    weekly_dir = processed_root / weekly_folder / rel_path.parent
    monthly_dir = processed_root / monthly_folder / rel_path.parent
    ensure_dir(weekly_dir)
    ensure_dir(monthly_dir)

    weekly_path = weekly_dir / (rel_path.stem + ".parquet")
    monthly_path = monthly_dir / (rel_path.stem + ".parquet")

    weekly.to_parquet(weekly_path)
    monthly.to_parquet(monthly_path)
