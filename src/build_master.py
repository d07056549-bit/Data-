import pandas as pd

def build_spine():
    spine = pd.DataFrame(
        index=pd.date_range("1980-01-01", "2030-12-31", freq="W-MON")
    )
    spine.index.name = "date"
    return spine

def main():
    spine = build_spine()
    print(spine.head())
    print(spine.tail())
    print(f"Spine length: {len(spine)}")

if __name__ == "__main__":
    main()
