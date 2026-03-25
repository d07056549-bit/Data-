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
    print(spine.head())
    print(spine.tail())
    print(f"Spine length: {len(spine)}")


if __name__ == "__main__":
    main()
