import pandas as pd
from pathlib import Path

def build_spine():
    spine = pd.DataFrame(
        index=pd.date_range("1980-01-01", "2030-12-31", freq="W-MON")
    )
    spine.index.name = "date"
    return spine


def load_and_prefix(path, prefix):
    df = pd.read_parquet(path)
    df = df.add_prefix(prefix + "_")
    return df


def merge_into_spine(spine, df):
    # Align on date index
    df = df.copy()
    df.index = pd.to_datetime(df.index)
    df = df.sort_index()

    # Join onto the spine
    return spine.join(df, how="left")


def main():
    spine = build_spine()

    # Path to your processed weekly mobility dataset
    mobility_path = Path(
        r"C:\Users\Empok\Documents\GitHub\Sofie\Data\processed\weekly\Black Swan\Global_Mobility_Report.parquet"
    )

    # Load and prefix the dataset
    mobility = load_and_prefix(mobility_path, "mobility")

    # Merge into the spine
    spine = merge_into_spine(spine, mobility)

    # Show results
    print(spine.head(10))
    print(spine.tail(10))
    print(f"Columns now in spine: {list(spine.columns)}")

