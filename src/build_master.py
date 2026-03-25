import pandas as pd
from pathlib import Path

def build_spine():
    spine = pd.DataFrame(
        index=pd.date_range("1980-01-01", "2030-12-31", freq="W-MON")
    )
    spine.index.name = "date"
    return spine

def merge_acled_weekly(spine):
    acled_dir = Path(
        r"C:\Users\Empok\Documents\GitHub\Sofie\Data\processed\weekly\Conflict\ACLED"
    )

    for file in acled_dir.glob("*.parquet"):
        region_name = file.stem.split("_")[0].lower().replace("-", "_")
        prefix = f"acled_{region_name}"

        print(f"Merging ACLED weekly dataset: {file.name} as  '{prefix}'")

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

def merge_gpr_weekly(spine):
    gpr_path = Path(
        r"C:\Users\Empok\Documents\GitHub\Sofie\Data\processed\weekly\Events\Geopolitical Risk\data_gpr_export.parquet"
    )

    print(f"Merging GPR weekly dataset: {gpr_path.name} as prefix 'gpr'")

    df = load_and_prefix(gpr_path, "gpr")
    spine = merge_into_spine(spine, df)

    return spine

def merge_monthly(spine):
    monthly_dir = Path(
        r"C:\Users\Empok\Documents\GitHub\Sofie\Data\processed\monthly"
    )

    for file in monthly_dir.rglob("*.parquet"):
        parts = file.parts
        folder = parts[-2].lower().replace(" ", "_")
        name = file.stem.lower().replace("-", "_")

        # FIX: ensure monthly columns never collide with weekly ones
        prefix = f"{folder}_{name}_monthly"

        print(f"Merging MONTHLY dataset: {file.name} as prefix '{prefix}'")

        df = load_and_prefix(file, prefix)

        df.index = pd.to_datetime(df.index)
        df = df.reindex(spine.index, method="ffill")

        spine = spine.join(df, how="left")

    return spine

def merge_yearly(spine):
    yearly_dir = Path(
        r"C:\Users\Empok\Documents\GitHub\Sofie\Data\processed\years"
    )

    for file in yearly_dir.rglob("*.parquet"):
        parts = file.parts
        folder = parts[-2].lower().replace(" ", "_")
        name = file.stem.lower().replace("-", "_")

        # Avoid collisions with weekly/monthly
        prefix = f"{folder}_{name}_yearly"

        print(f"Merging YEARLY dataset: {file.name} as prefix '{prefix}'")

        df = load_and_prefix(file, prefix)

        # Ensure datetime index
        df.index = pd.to_datetime(df.index)

        # Forward-fill yearly values into weekly spine
        df = df.reindex(spine.index, method="ffill")

        spine = spine.join(df, how="left")

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

    # 4. Merge GPR weekly dataset
    spine = merge_gpr_weekly(spine)

    # 5. Merge monthly dataset
    spine = merge_monthly(spine)

    # 6. Merge yearly dataset
    spine = merge_yearly(spine)
    
    # SAVE MASTER SHEET
    output_path = Path(
    r"C:\Users\Empok\Documents\GitHub\Sofie\Data\processed\master\master_weekly.parquet"
)

spine.to_parquet(output_path)
print(f"Master sheet saved to: {output_path}")



    # Show results
    print(spine.head(10))
    print(spine.tail(10))
    print(f"Total columns now in spine: {len(spine.columns)}")

if __name__ == "__main__":
    main()

