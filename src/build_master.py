import pandas as pd
from pathlib import Path

def merge_acled_weekly(spine):
    acled_dir = Path(
        r"C:\Users\Empok\Documents\GitHub\Sofie\Data\processed\weekly\Conflict\ACLED"
    )

    for file in acled_dir.glob("*.parquet"):
        region_name = file.stem.split("_")[0].lower().replace("-", "_")
        prefix = f"acled_{region_name}"

        print(f"Merging ACLED weekly dataset: {file.name} as prefix '{prefix}'")

        df = load_and_prefix(file, prefix)
        spine = merge_into_spine(spine, df)

    return spine

def merge_ucdp_weekly(spine):
    ucdp_dir = Path(
        r"C:\Users\Empok\Documents\GitHub\Sofie\Data\processed\weekly\Conflict\UCDP"
    )

    for file in ucdp_dir.glob("*.parquet"):
        # Use filename stem as prefix, cleaned
        prefix = "ucdp_" + file.stem.lower().replace("-", "_")

        print(f"Merging UCDP weekly dataset: {file.name} as prefix '{prefix}'")

        df = load_and_prefix(file, prefix)
        spine = merge_into_spine(spine, df)

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

    # 1. Merge mobility dataset
    mobility_path = Path(
        r"C:\Users\Empok\Documents\GitHub\Sofie\Data\processed\weekly\Black Swan\Global_Mobility_Report.parquet"
    )
    mobility = load_and_prefix(mobility_path, "mobility")
    spine = merge_into_spine(spine, mobility)

    # 2. Merge all ACLED weekly datasets
    spine = merge_acled_weekly(spine)

     # 3. Merge all UCDP weekly datasets
    spine = merge_ucdp_weekly(spine)

    # Show results
    print(spine.head(10))
    print(spine.tail(10))
    print(f"Total columns now in spine: {len(spine.columns)}")

